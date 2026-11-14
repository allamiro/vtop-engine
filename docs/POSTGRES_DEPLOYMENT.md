# PostgreSQL deployment and least privilege

VTOP deliberately separates PostgreSQL schema ownership from engine runtime
access. Normal startup never executes DDL. It checks that the expected columns
and the commit-rule invariant trigger exist (the trigger enforces
[VTOP_PROTOCOL_DRAFT.md §13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule) at the
database layer; durable state persistence implements §14), then operates with
table DML only.

## Identities

Use two database identities:

| Identity | Used by | Required access |
|---|---|---|
| migration owner | deployment job or init step | owns the VTOP schema objects and applies DDL |
| engine runtime | every running engine | `CONNECT`, schema `USAGE`, and `SELECT, INSERT, UPDATE` on `batches` |

Do not make the runtime role a table owner. Do not grant it `CREATE`, `ALTER`,
`DROP`, `DELETE`, `TRUNCATE`, `REFERENCES`, or `TRIGGER`.

## Deployment sequence

The config contains only a secret reference:

```yaml
engine:
  state_store: { env: VTOP_STATE_STORE }
```

1. In the migration job, make `VTOP_STATE_STORE` resolve to the migration
   owner's URL and run:

   ```bash
   vtopctl migrate --config /etc/vtop/config.yaml
   ```

2. Apply the runtime grants below as a database administrator. Replace the
   database, schema, and role names for the deployment.

   ```sql
   REVOKE CREATE ON SCHEMA public FROM PUBLIC;

   GRANT CONNECT ON DATABASE vtop TO vtop_runtime;
   GRANT USAGE ON SCHEMA public TO vtop_runtime;
   GRANT SELECT, INSERT, UPDATE ON TABLE public.batches TO vtop_runtime;

   REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER
       ON TABLE public.batches FROM vtop_runtime;
   ```

   Ledger retention (#128) is a MAINTENANCE operation, not a runtime one:
   the runtime role keeps its DELETE denial, and the engine refuses
   `engine.ledger_retention_days` on PostgreSQL at startup. Schedule
   `vtopctl prune-ledger --older-than-days N` (cron/systemd timer) with a
   dedicated maintenance identity instead. Each pass writes a digest of its
   candidate rows before deleting them (each is re-checked at deletion, so a
   row that gained a reason to stay is digested but kept) (`--digest-dir`, or
   `engine.ledger_digest_dir`; without either it is
   `<engine.work_dir>/ledger-digests`, which is scratch - name a durable,
   backed-up directory), which `vtopctl audit-coverage` reads (#560); run both
   against the same directory:

   ```sql
   GRANT CONNECT ON DATABASE vtop TO vtop_maintenance;
   GRANT USAGE ON SCHEMA public TO vtop_maintenance;
   GRANT SELECT, DELETE ON TABLE public.batches TO vtop_maintenance;
   ```

   The prune predicate itself protects recovery: only SOURCE_COMMITTED rows
   below the age cutoff whose per-path max-end_byte successor survives are
   deleted, so cursor seeding is unchanged by any prune.

3. In the engine workload, make the same `VTOP_STATE_STORE` reference resolve
   to the runtime role's URL. Do not mount the migration secret into the engine
   container.

For remote PostgreSQL, both URLs must use `sslmode=verify-full`; supply
`sslrootcert` when the database CA is private. Passing URLs through the secret
reference keeps credentials out of config serialization and command arguments.

## Failure behavior

If the table, required columns (`verification_class` since #664), or
invariant trigger are absent, the
trigger is disabled or its function lacks a current schema version marker,
or the `state_enum` CHECK does not admit every state the engine writes
(`superseded` since #567), engine startup fails with instructions to run
`vtopctl migrate`. It does not try to repair the
schema. If the runtime role lacks `USAGE` or `SELECT`, startup reports the
missing runtime access. Migration failures remain fatal to the deployment step.

The migration SQL is packaged at
`crates/vtop-state/migrations/postgres/0001_state_store.sql`. It is idempotent
and may also be reviewed or applied by an external migration system. Keep one
migration job per database; engine replicas should start only after it succeeds.

## Upgrades and rollback

- Run the new binary's migration step before rolling out its engine replicas.
- Keep schema ownership with the migration identity so later releases can
  upgrade without widening runtime privileges.
- A runtime rollback may use the upgraded backward-compatible schema. Do not
  drop columns or the invariant trigger while any VTOP process can access the
  database.
- **`superseded` (#567) needs the migration.** The release that adds the
  terminal `SUPERSEDED` state widens the `state_enum` CHECK. `vtopctl
  migrate` replaces the constraint in place, only if it lacks the state. The
  replacement validates every row while it holds the table's lock, and a
  later re-run does nothing. Engines of that release refuse to start on a
  schema without it.
  Older engines keep running on the widened schema; they never write the
  state. An older engine that READS a superseded row fails closed: its startup
  recovery claim, and `vtopctl status`, stop with `unknown batch state:
  superseded` rather than misreading it. So once an upgraded engine has
  written a superseded row - which its recovery does to the first stale
  handed-back row it finds - an older engine sharing the ledger cannot
  restart. Upgrade every
  engine on a ledger together, or drain the older ones first. Rolling back
  past this release means restoring a ledger backup taken before the upgrade.
- **`verification_class` (#664) needs the migration.** The release that
  records which class of verification a row passed adds a nullable
  `verification_class` column (`ADD COLUMN IF NOT EXISTS`, no default, no
  rewrite of existing rows). Engines of that release name the column at
  startup and refuse a schema without it, asking for `vtopctl migrate`; run
  it before rolling them out, as above. **Restart every older engine right
  after the migration** (or drain them before it): engines before #664 read
  rows with `SELECT *` and `RETURNING *` through statements each connection
  caches, and once the column exists PostgreSQL refuses those with `cached
  plan must not change result type` until the connection is replaced -
  failing that engine's ledger reads and writes meanwhile, which fails
  batches into replay (the commit rule is not affected). Restarted, an older
  engine runs on the migrated schema, before and after an upgraded engine
  has written a class: it never names the column, a row it verifies keeps
  NULL, and NULL is read as unknown and counted as before. The class decides
  only which counter a recovered commit is counted in, never whether a row
  commits, so an older engine ignoring it misreads nothing. Engines of this
  release name every column they read, so a column a later migration adds
  does not affect them while they run. The schema marker is not bumped (see
  below). A rollback may keep the column; do not drop it while any engine of
  that release can reach the database.
- Changes to the JSON inside ledger rows need no migration step. The content
  fingerprint added to file and spool markers (#578) is such a change: no
  DDL, older rows keep being read and judged as before. See the ledger format
  note in [`ARCHITECTURE.md`](ARCHITECTURE.md) §7.2.
- The invariant trigger also refuses an `INSERT` in any state but `batching`
  (#625). Re-running `vtopctl migrate` replaces the trigger function in
  place. Every release inserts only `batching`, so an older engine on the
  upgraded schema is unaffected.
- Schema version marker. The first non-empty line of the trigger function's
  body (`prosrc`) is the SQL comment `-- vtop-state-schema 3`, written on the
  line right after `$fn$`. Engine startup reads only that line: it requires
  the trigger to be enabled and its function body to open with a marker of
  at least the version the engine was built for (2 since #625, 3 since #567,
  whose migration also widens the state CHECK), not just a trigger with that
  name. Startup also checks the `state_enum` CHECK itself, so a current
  marker over an old CHECK is refused as well. A missing or unparseable marker, or one on any
  later line, counts as an old schema.
  An engine from this release therefore refuses to start, with instructions
  to run `vtopctl migrate`, until the migration has run, so run it before
  rolling out the engines, as above. The marker is part of the body, so
  whatever installs a body also sets its marker. If an older release's
  `vtopctl migrate` is run against an upgraded database, its
  `CREATE OR REPLACE` installs the older, unmarked body, and this release's
  engines refuse to start until this release's migration runs again.
  **Any future change to the trigger or its function, or to the schema the
  migration leaves that an engine could misread or an older release's
  migration would undo, must bump this marker** in the migration together
  with `MIN_SCHEMA_VERSION` in `pg_store.rs`. A new nullable column that
  startup names and older engines never name does not: startup already
  refuses a schema without it, an older release's migration never drops it,
  and a higher marker cannot make an older engine refuse anything, since an
  engine accepts every marker at or above its own minimum. The
  `verification_class` column (#664) is such a change, and leaves the marker
  at 3.
- Restoring the ledger from a backup. Stop every engine first, and **always
  finish with `vtopctl migrate`**. A schema restored from an older archive
  (`pg_restore`, or `--schema-only`) carries that release's trigger
  function, and the migration's `CREATE OR REPLACE` installs the current one
  and re-creates the trigger enabled. A data-only restore into a migrated
  schema (`pg_restore --data-only`) is refused by the trigger, as it already
  was for `source_committed` rows. Restore the data in one of two ways:
  - `pg_restore --data-only --disable-triggers`. This issues
    `ALTER TABLE batches DISABLE TRIGGER ALL`, which only the table's owner
    (the migration identity) or a superuser may run; the runtime role is
    refused with `must be owner of table batches`. The pg_restore
    documentation says to run it as a superuser, and a superuser is required
    once the table has foreign-key triggers, which `batches` does not.
  - Without superuser: once the schema exists (`vtopctl migrate`, or
    `pg_restore --schema-only` followed by `vtopctl migrate`), have the
    owner run `ALTER TABLE batches DISABLE TRIGGER trg_commit_after_verify`,
    then restore with `--data-only`.

  Then run `vtopctl migrate` before any engine starts. Startup refuses a
  disabled trigger, and a function restored from an older archive, which
  has no current marker.
- Test grants after role changes: runtime must be able to read/write ledger
  rows but must fail `CREATE TABLE`, `ALTER TABLE`, `DELETE`, and `TRUNCATE`.

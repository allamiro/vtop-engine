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

If the table, required columns, or invariant trigger are absent, or the
trigger is disabled or its function lacks a current schema version marker,
engine startup fails with instructions to run `vtopctl migrate`. It does not try to repair the
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
- Changes to the JSON inside ledger rows need no migration step. The content
  fingerprint added to file and spool markers (#578) is such a change: no
  DDL, older rows keep being read and judged as before. See the ledger format
  note in [`ARCHITECTURE.md`](ARCHITECTURE.md) §7.2.
- The invariant trigger also refuses an `INSERT` in any state but `batching`
  (#625). Re-running `vtopctl migrate` replaces the trigger function in
  place. Every release inserts only `batching`, so an older engine on the
  upgraded schema is unaffected.
- Schema version marker. The first line of the trigger function's body is
  the SQL comment `-- vtop-state-schema 2`. Engine startup requires the
  trigger to be enabled and its function body to carry a marker of at least
  the version the engine was built for (2 since #625), not just a trigger
  with that name. A missing or unparseable marker counts as an old schema.
  An engine from this release therefore refuses to start, with instructions
  to run `vtopctl migrate`, until the migration has run, so run it before
  rolling out the engines, as above. The marker is part of the body, so
  whatever installs a body also sets its marker. If an older release's
  `vtopctl migrate` is run against an upgraded database, its
  `CREATE OR REPLACE` installs the older, unmarked body, and this release's
  engines refuse to start until this release's migration runs again.
  **Any future change to the trigger or its function must bump this
  marker** in the migration together with `MIN_SCHEMA_VERSION` in
  `pg_store.rs`.
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

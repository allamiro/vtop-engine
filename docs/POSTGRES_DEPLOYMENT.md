# PostgreSQL deployment and least privilege

VTOP deliberately separates PostgreSQL schema ownership from engine runtime
access. Normal startup never executes DDL. It checks that the expected columns
and verify-before-commit trigger exist, then operates with table DML only.

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

3. In the engine workload, make the same `VTOP_STATE_STORE` reference resolve
   to the runtime role's URL. Do not mount the migration secret into the engine
   container.

For remote PostgreSQL, both URLs must use `sslmode=verify-full`; supply
`sslrootcert` when the database CA is private. Passing URLs through the secret
reference keeps credentials out of config serialization and command arguments.

## Failure behavior

If the table, required columns, or invariant trigger are absent, engine startup
fails with instructions to run `vtopctl migrate`. It does not try to repair the
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
- Test grants after role changes: runtime must be able to read/write ledger
  rows but must fail `CREATE TABLE`, `ALTER TABLE`, `DELETE`, and `TRUNCATE`.

# Single engine with PostgreSQL

One engine whose ledger lives in PostgreSQL, so it outlives the engine's host.
This is Tier 1 in
[Production HA §9](../PRODUCTION_HA.md#9-deployment-topologies). Everything on
[Single engine with SQLite](single-node.md) applies; this page covers what
changes. The full rollout, grants and schema rules are in
[PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md).

## Prerequisites

| Item | Notes |
|---|---|
| `vtopctl` with PostgreSQL support | The release binaries and the image do not include it. Build with `cargo build --release --locked -p vtop-cli --features postgres` ([Build from source](../install/from-source.md)). |
| PostgreSQL over TLS | Every connection that is not loopback or a Unix socket must use `sslmode=verify-full`, with `sslrootcert` for a private CA. |
| Three database identities | A **migration** identity that owns the schema, a **runtime** identity for the engine (`SELECT, INSERT, UPDATE` on `batches`), and a **maintenance** identity for pruning (`SELECT, DELETE`). |
| A durable digest directory | Where `vtopctl prune-ledger` writes the rows it deletes. Without one it is `<work_dir>/ledger-digests`, which is scratch. |

## Steps

1. **Reference the URL, never inline it.** In `config.yaml`:

    ```yaml
    engine:
      state_store: { env: VTOP_STATE_STORE }
      ledger_digest_dir: /var/lib/vtop/ledger-digests
    ```

    An inline `postgres://` URL is refused.
    `{ file: /run/secrets/vtop-state-store }` works too.

2. **Migrate, as the migration identity.** With `VTOP_STATE_STORE` holding the
   migration owner's URL:

    ```bash
    vtopctl migrate --config /etc/vtop/config.yaml
    ```

    The engine never changes the schema itself.

3. **Grant the runtime and maintenance roles**, as a database administrator.
   The exact `GRANT` and `REVOKE` statements are in
   [PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md#deployment-sequence).

4. **Start the engine as the runtime identity.** Point `VTOP_STATE_STORE` at
   the runtime role's URL. Never mount the migration secret into the engine.

5. **Schedule pruning as the maintenance identity.**
   `engine.ledger_retention_days` is refused on PostgreSQL; run this from cron
   or a systemd timer instead:

    ```bash
    vtopctl prune-ledger --config /etc/vtop/config.yaml --older-than-days 30
    ```

    It deletes at most `--limit` rows (default 500) per run.

## Verify it works

- The engine logs at every start that it is single-instance on this database.
  That is expected.
- `vtopctl status` and `vtopctl audit-coverage --check-storage` as on
  [Single engine with SQLite](single-node.md#verify-it-works).
- Startup refuses a schema that is missing, old, or has its trigger disabled,
  and tells you to run `vtopctl migrate`.
- The runtime role must fail `CREATE TABLE`, `ALTER TABLE`, `DELETE` and
  `TRUNCATE`; test it after any role change.

## What to monitor

As for SQLite, plus the database itself. `engine.state_store_timeout_seconds`
(default 30) bounds each ledger request, so a database that stops answering
fails requests instead of hanging the engine.

## Upgrade notes

Run the new release's `vtopctl migrate` before the new engine starts. A
release whose schema adds a state (such as `superseded` in v0.8.0) refuses to
start on the old schema, and once it has written such a row, an older engine
cannot restart on that ledger. Back up the database first; see
[Upgrades](../operate/upgrades.md) and
[Backup and recovery](../operate/backup-and-recovery.md).

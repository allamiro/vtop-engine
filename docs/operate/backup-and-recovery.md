# Backup and recovery

The engine recovers from its own crashes by itself. What it cannot recover
without help is a lost ledger or lost objects, so those are what you back up.

## What the engine recovers by itself

On every start, recovery reads the ledger and finishes or undoes what was in
flight. A batch that was not verified is read again from the source; a
verified batch whose commit did not happen has its commit retried. Nothing
needs doing by hand, and the source still holds every range that was not
committed. The full table is on
[How it works](../concepts/how-it-works.md#what-happens-when-something-fails).

To run recovery as a one-shot command, for every incomplete batch or for one:

```bash
vtopctl replay --config /etc/vtop/config.yaml
vtopctl replay --batch-id <batch_id> --config /etc/vtop/config.yaml
```

## What to back up

| What | Why | How |
|---|---|---|
| **The ledger** | Each batch's state, and the cursor recovery resumes from. The coverage audit and the scrub read it. | SQLite: copy the database file with the engine stopped. It runs in WAL mode, so a copy taken while it runs can miss recent writes. PostgreSQL: your usual backups. |
| **Prune digests** | The rows pruning deleted, which the audit and scrub still account for. | `engine.ledger_digest_dir`, or `<database file>.ledger-digests/` beside a SQLite file. Back it up with the ledger. |
| **The bucket** | The archive itself: objects and manifests. | Bucket versioning, replication or the store's backup. With `upload.require_object_versioning`, manifests are pinned by version, so an overwritten manifest cannot roll a batch back. |
| **Config and secrets** | To rebuild the engine. | Your configuration management. Never commit the secrets. |

`engine.work_dir` is scratch and needs no backup.

A backup procedure, restore drills and recovery targets are not yet written
up by the project;
[Production HA §16](../PRODUCTION_HA.md#16-backup-restore--disaster-recovery-proposed)
records the proposed approach.

## Restoring the ledger

Stop every engine first.

- **SQLite:** put the database file and its digest directory back, then start
  the engine. Recovery reconciles what was in flight.
- **PostgreSQL:** restore, then **always run `vtopctl migrate`** before any
  engine starts: a schema restored from an older backup carries an older
  trigger function, and the engine refuses it. A data-only restore into a
  migrated schema is refused by the commit-rule trigger unless triggers are
  disabled for the restore; the two ways to do that are in
  [PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md#upgrades-and-rollback).

Then check the result:

```bash
vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
vtopctl scrub --config /etc/vtop/config.yaml --sample 100
```

## Downgrades

Downgrades are not supported. Restoring a backup taken before an upgrade is
the only way back; see [Upgrades](upgrades.md).

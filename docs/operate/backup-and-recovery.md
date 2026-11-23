# Backup and recovery

The engine recovers from its own crashes by itself. What it cannot recover
without help is a lost ledger or lost objects, so those are what you back up.
This page gives the commands for backing up and restoring the ledger on SQLite
and on PostgreSQL, the recovery targets, and the drill that CI runs against
each procedure and what it measured. When there is no backup, or the bucket or
a whole region is lost, see [Disaster recovery](disaster-recovery.md).

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
| **The ledger** | Each batch's state, and the cursor recovery resumes from. The coverage audit and the scrub read it. | SQLite: the online backup, while the engine runs ([below](#sqlite)). PostgreSQL: `pg_dump`, or a base backup with WAL archiving ([below](#postgresql)). |
| **Prune digests** | The rows pruning deleted, which the audit and scrub still account for. | `engine.ledger_digest_dir`, or `<database file>.ledger-digests/` beside a SQLite file. **On PostgreSQL, set `engine.ledger_digest_dir` to a durable directory** (the commands below use `/var/lib/vtop/ledger-digests`): unset, the digests go to `<engine.work_dir>/ledger-digests`, which is scratch and is not what this page backs up. Copy it **after** the ledger backup, so no digest a backed-up row needs is missing. |
| **The bucket** | The archive itself: objects and manifests. | Bucket versioning, replication or the store's backup. With `upload.require_object_versioning`, manifests are pinned by version, so an overwritten manifest cannot roll a batch back. |
| **Config and secrets** | To rebuild the engine. | Your configuration management. Never commit the secrets. |

`engine.work_dir` is scratch and needs no backup.

**Where and as whom.** Run the commands on this page on the engine's host (or
a host that can reach its database), as the user the engine runs as
(`vtop` in [the example unit](https://github.com/allamiro/vtop-engine/blob/main/examples/systemd/vtop-engine.service)),
with `/etc/vtop/config.yaml` the engine's config. To stop and start the engine,
use whatever runs it: `systemctl stop vtop-engine` / `systemctl start
vtop-engine` for that unit, or scale the Deployment to 0 and back to 1. The
PostgreSQL commands need `VTOP_STATE_STORE` exported in your shell (the same
value the engine's `engine.state_store: { env: VTOP_STATE_STORE }` resolves
to), a `vtopctl` built with the `postgres` feature (the release binaries are not,
and container images up to v0.10.2 are not; see
[Install](../install/index.md)), and, when a manifest MAC
key is configured, that key in the environment for every `vtopctl` command
below (`migrate`, `scrub` and `audit verify` need it).

A copy of a SQLite file taken while the engine runs can miss recent writes:
the ledger runs in WAL mode, and the newest rows may be in the `-wal` file.
Use the online backup below, or stop the engine first.

## Recovery targets

What a restore costs, in two numbers. Both are measured by the drill.

- **RPO, the ledger history the restore loses.** The gap between the newest
  write the lost ledger held and the newest the restored one holds, read from
  the rows' own `updated_at`. It is set by how often you back up (SQLite,
  `pg_dump`) or by `archive_timeout` and how fast WAL reaches the archive
  (PostgreSQL with WAL archiving).
- **RTO, the time to be archiving again.** From the first step of the restore
  until the engine commits a batch again. It includes the restore, `vtopctl
  migrate` on PostgreSQL, the engine's start and its recovery. It does not
  include noticing the failure, or fetching a backup from somewhere else.

**The archive loses nothing to either.** The ledger rows in the lost window
describe batches whose objects are already in the bucket, and whose source
ranges the restored ledger does not show as committed. The engine reads those
ranges from the source again. A range read again is a duplicate, or reuses the
object it finds at the same key after a strong verification; it is never a
hole. A source that has already deleted what it handed over is the exception,
under [what the drill does not cover](#what-the-drill-does-not-cover).

The drill fails when a measured number is past one of these bounds. The
bounds are the script's, and the script refuses to run when this table lists
different ones.

| Bound | Seconds | What it bounds |
|---|---|---|
| `VTOP_DRILL_BACKUP_INTERVAL_S` | 8 | SQLite and `pg_dump`: how long the engine runs after the backup before the ledger is destroyed (the interval between backups, in the drill). |
| `VTOP_DRILL_ARCHIVE_TIMEOUT_S` | 5 | PostgreSQL with WAL archiving: `archive_timeout` on the primary. |
| `VTOP_DRILL_SQLITE_RPO_S` | 13 | SQLite RPO: the backup interval, plus 5 for the age of the newest row the backup holds and the time the engine takes to stop. |
| `VTOP_DRILL_DUMP_RPO_S` | 13 | `pg_dump` RPO: the same. |
| `VTOP_DRILL_PITR_RPO_S` | 10 | WAL archiving RPO: `archive_timeout`, plus 5 for the archive command's own time. |
| `VTOP_DRILL_SQLITE_RTO_S` | 60 | SQLite RTO. |
| `VTOP_DRILL_DUMP_RTO_S` | 90 | `pg_dump` RTO. |
| `VTOP_DRILL_PITR_RTO_S` | 120 | WAL archiving RTO: new server from the base backup, replay of the archive, `vtopctl migrate`, first commit. |

A bound that holds in the drill is a bound on the procedure, not on your
deployment: your backup interval and `archive_timeout` set your RPO, and your
ledger's size and your storage set your RTO. See
[Choosing your own targets](#choosing-your-own-targets).

### What the drill measured

Each row is one run of the drill, with the conditions below it. They show
that the procedures work and what their parts cost on a small ledger; they are
not a promise about yours.

| Mode | RPO | RTO | Backup took | Ledger rows lost, archived again | Where |
|---|---|---|---|---|---|
| SQLite, `.backup` | 6.2 s | 0.44 s | 9 ms | 3 | CI |
| PostgreSQL 14, `pg_dump` | 8.2 s | 0.95 s | 0.14 s | 4 | CI |
| PostgreSQL 15, `pg_dump` | 8.2 s | 1.06 s | 0.14 s | 4 | CI |
| PostgreSQL 16, `pg_dump` | 8.2 s | 1.03 s | 0.14 s | 4 | CI |
| PostgreSQL 17, `pg_dump` | 8.2 s | 1.07 s | 0.14 s | 4 | CI |
| PostgreSQL 18, `pg_dump` | 8.2 s | 0.74 s | 0.09 s | 4 | CI |
| PostgreSQL 14, base backup and WAL archive | 2.3 s | 1.2 s | 0.17 s | 1 | laptop |
| PostgreSQL 18, base backup and WAL archive | 4.1 s | 1.1 s | 0.20 s | 2 | laptop |

Conditions, for every row: a `vtopctl` debug build; a file source grown by
about 20 records a second of 85 bytes, in batches of up to 40 records or 2
seconds; the bucket a local directory; a ledger of a few dozen rows; a backup
interval of 8 seconds and `archive_timeout` of 5 seconds. Every run archived
every source record (430 to 980 of them) and passed the checks under
[The drill](#the-drill).

- **CI** is a GitHub-hosted `ubuntu-latest` runner, the images pinned in
  `postgres-matrix.yml`, in the run of this page's pull request for the
  SQLite and dump modes.
- **laptop** is Docker Desktop on an arm64 laptop with two CPUs, PostgreSQL
  from `postgres:18-alpine` and the pinned `postgres:14` image. The WAL
  archiving rows are from here because they were measured with the disaster
  placed late in an archive interval (below); CI measures them the same way
  from the nightly on, and keeps the figures as an artifact.

Reading them:

- The RPO of a backup taken at intervals is up to the interval, and less when
  the engine was between writes at the moment of the loss. The 6 to 8 seconds
  above are an 8-second interval.
- The RPO of WAL archiving falls anywhere from 0 to `archive_timeout`,
  depending on where in that interval the server is lost: a lost host loses the
  WAL it had not yet archived. The drill ends the server 3 seconds after a
  segment was archived, so that it measures a loss and not the best case; the
  2 to 4 seconds above are that stretch.
- The RTO is small because the ledger is small and the engine reads the
  backlog (the records written since the backup) in one pass. The restore step
  of the PostgreSQL modes was about a second.

CI measures the same numbers on every run and keeps them in the workflow run's
summary and as an artifact: `restore drill (sqlite)` in the CI workflow, and
`restore drill (pg N)` in the `postgres matrix` workflow, nightly.

## SQLite

**Back up** while the engine runs. `.backup` uses SQLite's online backup API,
which copies a consistent snapshot even while the engine writes:

```bash
umask 077
sqlite3 /var/lib/vtop/state.db ".timeout 10000" ".backup '/backup/state.db'"
mkdir -p /backup/digests
# after the database; the directory exists once `vtopctl prune-ledger` has run
[ -d /var/lib/vtop/state.db.ledger-digests ] \
  && cp -a /var/lib/vtop/state.db.ledger-digests/. /backup/digests/
sqlite3 /backup/state.db 'PRAGMA integrity_check'               # prints: ok
```

Run it as the engine's user, so the copy belongs to it.

The `umask 077` matters on the way back: the engine refuses a ledger other
local users can reach ([Secret files](secrets.md)), and `.backup` creates the
copy with your umask's mode.

**Restore.** Stop every engine first (a crashed one is already stopped;
`systemctl status vtop-engine` says which).

```bash
rm -f /var/lib/vtop/state.db /var/lib/vtop/state.db-wal /var/lib/vtop/state.db-shm
cp /backup/state.db /var/lib/vtop/state.db
chmod 0600 /var/lib/vtop/state.db
rm -rf /var/lib/vtop/state.db.ledger-digests
mkdir -p /var/lib/vtop/state.db.ledger-digests
cp -a /backup/digests/. /var/lib/vtop/state.db.ledger-digests/
chown -R vtop:vtop /var/lib/vtop/state.db /var/lib/vtop/state.db.ledger-digests  # if you ran this as root
systemctl start vtop-engine        # or: vtopctl run --config /etc/vtop/config.yaml
```

Remove the old `-wal` and `-shm` files: a WAL left from the lost ledger does
not belong to the restored file. Recovery reconciles what was in flight, and
the engine reads again whatever the source holds past the restored ledger's
newest commit. Then [check the result](#check-the-result).

## PostgreSQL

Two procedures, which the drill runs separately. A dump is simple and loses
what was written since the last one. WAL archiving with a base backup loses
at most about `archive_timeout`, and can also recover to a moment you choose,
which is the way back from a mistake rather than from a lost host.

### Dump and restore

```bash
# Back up while the engine runs. pg_dump reads one snapshot.
pg_dump --format=custom --file=/backup/ledger.dump "$VTOP_STATE_STORE"
mkdir -p /backup/digests
cp -a /var/lib/vtop/ledger-digests/. /backup/digests/           # after the dump

# Restore, with every engine stopped, into an empty database.
createdb vtop
pg_restore --exit-on-error --dbname=vtop /backup/ledger.dump
mkdir -p /var/lib/vtop/ledger-digests
cp -a /backup/digests/. /var/lib/vtop/ledger-digests/
vtopctl migrate --config /etc/vtop/config.yaml                  # always, before any engine starts
vtopctl run --config /etc/vtop/config.yaml
```

**Roles.** A dump holds the database, not the cluster's roles. If your
deployment runs the two identities of
[PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md#identities) (a migration
owner that owns the schema and a runtime role the engine connects as), the
server you restore into, if it is not the one you dumped from, needs both
roles **before** `pg_restore` (create them from your deployment's own script,
or keep `pg_dumpall --roles-only` beside the dump): the dump names the owner
and the grants, and `--exit-on-error` stops at the first role that does not
exist. After the restore and `vtopctl migrate`, check the runtime role's grants
against
[the deployment sequence](../POSTGRES_DEPLOYMENT.md#deployment-sequence), and
run `vtopctl migrate` with `VTOP_STATE_STORE` set to the migration owner's URL,
not the runtime role's.

A dump that carries the schema as well as the rows restores into an empty
database without disabling the commit rule's trigger: `pg_restore` loads the
rows before it creates the triggers. `vtopctl migrate` after it re-creates the
trigger and its function from this release's, which a schema restored from an
older dump needs, and the engine refuses to start without. A data-only restore
into a schema `vtopctl migrate` has already created is a different procedure,
with steps for the trigger and the audit records, in
[PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md#upgrades-and-rollback).

### Base backup and WAL archiving

Archive WAL to storage that survives the loss of the server and its disk, and
take a base backup of the data directory. On the primary:

```ini
# postgresql.conf
archive_mode = on
archive_command = 'test ! -f /walarch/%f && cp %p /walarch/%f'   # any command that durably copies the file
archive_timeout = 60        # force a segment switch this often while the ledger is written
```

`archive_mode` takes a server restart; the other two reload. Then take the
base backup, from a role that has the `REPLICATION` attribute and a
`pg_hba.conf` entry for replication connections (without one the command stops
with "no pg_hba.conf entry for replication connection"):

```bash
pg_basebackup --host=<primary> --username=<replication role> \
  --pgdata=/basebackup/base --wal-method=stream --checkpoint=fast
```

Copy the prune digests (`engine.ledger_digest_dir`) to the same storage **after
every `vtopctl prune-ledger`**, not only when you take a base backup: a recovery
to the end of the archive replays the pruning's deletions, and a digest written
since your last copy is lost with the host while the rows it recorded are gone
from the recovered ledger.

```bash
mkdir -p /backup/digests
cp -a /var/lib/vtop/ledger-digests/. /backup/digests/
```

`archive_timeout` bounds the RPO: a server lost between two switches loses
the WAL of that interval. The drill sets it to 5 seconds so that it can
measure; a production value is a trade between that loss and the many small
segment files it writes.

**Restore to the end of the archive** after losing the server and its disk:

1. Copy the base backup into an empty data directory, owned by the server's
   user, mode `0700`.
2. `touch <datadir>/recovery.signal`.
3. Start the server with `restore_command` pointing at the archive and
   `recovery_target_action = promote` (on the command line, as below, for a
   restore you watch; for a service, put the two settings in the data
   directory's `postgresql.auto.conf` and start it as you start any server):

   ```bash
   postgres -D <datadir> \
     -c 'restore_command=cp /walarch/%f %p' \
     -c recovery_target_action=promote
   ```

4. Wait until `SELECT pg_is_in_recovery()` is false (`psql -c 'SELECT
   pg_is_in_recovery()'` prints `f`; the server log ends the replay with
   "archive recovery complete").
5. Put the digest directory back (`mkdir -p` it and `cp -a /backup/digests/. <dir>/`),
   run `vtopctl migrate`, and start the engine.

**Restore to a time**, for a ledger that was damaged at a known moment, adds
`-c recovery_target_time='2026-11-20 18:52:18.465513+00'` to the same start.
The recovered ledger stops at that time. The engine then reads again from the
source everything that was archived after it. The target must be earlier than
the newest WAL in the archive.

After a recovery the new server is on a new timeline and archives nothing
until you configure it again: turn archiving back on and take a new base
backup before relying on it.

## Check the result

The same three commands the drill runs after every restore:

```bash
vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
vtopctl scrub --config /etc/vtop/config.yaml --sample 100
vtopctl audit verify --config /etc/vtop/config.yaml
```

The first two read the prune digests from `engine.ledger_digest_dir`; if you
restored them somewhere else, add `--digest-dir <dir>` to both. Each command
exits non-zero on a finding; read the exit code, not only the text.

The first reports every committed range as tiled, with no hole and every
committed object and manifest present; a range the restored ledger does not
yet show as committed is `pending`, not a hole, until the engine has archived
it again. The second reads a random sample of the committed batches back from
the bucket and checks each against its manifest. The third recomputes the
audit trail's hash chain, which a restore must leave unbroken. Keep the row
count and last link it prints: a chain cannot show rows removed from its end
([Auditing](auditing.md)).

## The drill

[`scripts/restore-drill.sh`](https://github.com/allamiro/vtop-engine/blob/main/scripts/restore-drill.sh)
runs the procedures above against a live engine, in three modes.

```bash
scripts/restore-drill.sh sqlite          target/debug/vtopctl
RESTORE_DRILL_PG_IMAGE=postgres:18 \
  scripts/restore-drill.sh postgres-dump target/debug/vtopctl
RESTORE_DRILL_PG_IMAGE=postgres:18 \
  scripts/restore-drill.sh postgres-pitr target/debug/vtopctl
```

Each mode:

1. starts a writer that appends to a file about 20 records a second, and an
   engine (`vtopctl run`) that archives it to a local directory, with the
   state store under test;
2. waits for six committed batches and runs `vtopctl prune-ledger`, so the
   backup holds a digest;
3. takes the backup while the engine runs, lets the engine run on for the
   backup interval, then kills it with SIGKILL and destroys the ledger (for
   WAL archiving, the server and its data volume, keeping the archive and the
   base backup, 3 seconds after a WAL segment was archived so that some of the
   ledger's history is lost);
4. restores as this page says, and starts the engine again, while the writer
   keeps writing;
5. stops the writer and waits for the engine to archive the source to its
   last byte, then stops it with SIGTERM, which must exit 0;
6. checks the restored ledger: `audit-coverage --check-storage` exits 0, says
   every committed object and manifest is present, and reports no `FAIL`, hole,
   gap, unproven or pending range; `scrub --sample 100` exits 0 and checked at
   least one batch; `audit verify` passes; and every record the source ever
   held (each carries a sequence number) is found in an object a committed
   ledger or digest row names, counted from the objects themselves;
7. compares the measured RPO and RTO with the bounds above, and fails when
   either is past its bound.

The WAL-archiving mode restores twice from the same base backup and archive:
to the end of the archive, which gives the RPO and RTO and is checked as above,
and to a time between the backup and the disaster, which must leave a ledger
whose newest write is not after that time and earlier than the full recovery's,
and from which the engine also resumes with the same checks.

The SQLite mode runs on every pull request that touches the state store,
the engine, the file source or the CLI, and on every push to `main`
(`restore drill (sqlite)` in `ci.yml`). The PostgreSQL modes run nightly and on
dispatch, on every major in the supported list, from the images pinned in
`postgres-matrix.yml` (`restore drill` in that workflow), and on a pull request
that changes that workflow. Each run writes its measurements as a JSON file,
kept as a workflow artifact, and into the run's summary.

### What the drill does not cover

- **A prune after the last digest copy.** The drill copies the digest directory
  once, after the backup, as the page says to for a dump or SQLite. For WAL
  archiving the page says to copy it after every prune; the drill does not
  prune again after its copy, so that rule is the page's, not the drill's.
- **A ledger of any size.** The drill's ledger holds a few dozen rows. The
  restore step of RTO grows with the ledger and, for WAL archiving, with the
  WAL between the base backup and the end of the archive; measure it on a copy
  of your own.
- **The object store.** The drill's bucket is a local directory that the
  disaster does not touch. Backing up and restoring the archive is the store's
  job (versioning, replication); a lost bucket and a lost region are drilled
  in [Disaster recovery](disaster-recovery.md).
- **A source that keeps its position outside the ledger.** The drill reads a
  file, whose position the engine seeds from the ledger. A Kafka source keeps
  its consumer group's offsets in the broker, which a restore of the ledger
  does not move, so the restored ledger and the group can disagree. The
  [region drill](disaster-recovery.md#a-region-is-lost) restores an older
  ledger under a newer group: the engine sees the group ahead of the ledger,
  resumes from the ledger and says so, with no hole. Run `audit-coverage`
  after a restore, and do not treat the restore as complete until it passes.
- **A source that deletes after commit.** With `delete_after_commit` the engine
  removes bytes it has committed. A restore to a ledger older than a deletion
  cannot bring those bytes back from the source; the archive still holds the
  objects, and the `scrub` reads only batches a ledger or digest row names.
- **The metadata tier.** `vtop-meta`'s own backup after a feature-level raise
  is not part of this drill.
- **Managed services.** A managed PostgreSQL has its own backup and
  point-in-time restore; use that, and run the checks above on the result.

### Choosing your own targets

Decide the RPO you can accept, then set the interval to it: a dump or SQLite
backup every `N` minutes loses up to `N` minutes of ledger history, and WAL
archiving with `archive_timeout = T` loses up to about `T` seconds. The loss
costs rework, not data: the engine archives the range again. Measure the RTO
by running the drill's procedure, not the script, on a copy of your ledger.

## Downgrades

Downgrades are not supported. Restoring a backup taken before an upgrade is
the only way back; see [Upgrades](upgrades.md). That restore is this page's
procedure, and `vtopctl migrate` after it brings the schema to the running
release.

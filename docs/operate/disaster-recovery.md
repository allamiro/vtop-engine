# Disaster recovery

[Backup and recovery](backup-and-recovery.md) covers a ledger that was backed
up. This page covers the rest: a ledger with **no** backup, a lost bucket, a
lost region, a full disk, an object that fails verification, and a key
rotation. For each it says what the engine rebuilds by itself, what it cannot
rebuild, the steps, and what a drill that follows those steps measured.

Every procedure here is run by a script against a lab built from real parts
(the `vtopctl` binary, MinIO, PostgreSQL and Kafka in containers). Each script
follows this page's steps literally, so a step that is wrong or missing here
fails the drill, and each refuses to run when the bounds in
[the table below](#the-bounds) differ from its own.
[How to run them](#running-the-drills) is at the end.

## What the engine can and cannot rebuild

| What is lost | The engine rebuilds | It cannot rebuild | Section |
|---|---|---|---|
| The ledger, no backup, the bucket intact | Archiving resumes with no hole and no duplicate. With a source that still holds the data, a new ledger can prove every archived record again. | The old ledger's history: each batch's row, id and timestamps, and the audit trail's hash chain. Without the extra step, records archived before the loss are in the bucket but no longer proven by the ledger. | [The ledger is lost](#the-ledger-is-lost-with-no-backup) |
| The bucket, no replica | Every record the source still holds, archived again into a new bucket. | Every record the source no longer holds. The old ledger names exactly which. | [The bucket is lost](#the-bucket-is-lost) |
| A region: the engine, the ledger and the bucket | The ledger from its last backup and the bucket from its replica; then what neither held, from the source. | Records that neither the replica nor the source holds. The check after the restore names the objects the replica lacks. The source's own recovery is out of scope. | [A region is lost](#a-region-is-lost) |
| A disk is full: the work directory, the ledger or the store | Everything, by itself, once there is space: no restart, nothing lost. | Nothing is lost. Records of at most one batch can be archived twice. | [A disk is full](#a-disk-is-full) |
| Verification fails: the store answers wrongly, or an archived object is changed | While the engine verifies: nothing is committed until the store is right. At rest: the previous version of the object, if the bucket keeps versions. | An object with no earlier version: the scrub reports it, and it is lost. | [Verification fails](#verification-fails) |
| A manifest MAC key is rotated, then withdrawn | Nothing is lost: manifests are never re-signed, and withdrawing a key is reversible. | | [Key rotation](#key-rotation) |

## The bounds

The drills fail when a measured time is past one of these bounds, and each
drill refuses to run when its rows here differ from the numbers in its script.
They are bounds on the **procedure in the drill's lab**: a ledger of a few
dozen rows and 180 to 240 records. Your RTO is set by your ledger's size, how
much of the source the engine must read again, your store and your network.
Measure it on a copy of your own.

| Bound | Seconds | What it bounds |
|---|---|---|
| `VTOP_DR_LEDGER_LOST_RTO_S` | 60 | A lost ledger: from creating the new database until the engine commits again, and until it has archived the rest of the source. |
| `VTOP_DR_BUCKET_LOST_RTO_S` | 120 | A lost bucket: from keeping the old ledger until every record the source still holds is archived again. |
| `VTOP_DR_REGION_LOST_RTO_S` | 180 | A lost region: from the first step in the DR region until the engine has archived the source again, after the repair when the check fails. |
| `VTOP_DR_DISK_FULL_RTO_S` | 90 | A full disk: from freeing the space until every record is committed. It includes the engine's own retry back-off. |
| `VTOP_DR_VERIFICATION_RTO_S` | 60 | A verification fault: from the store being right again until the engine has committed everything. |
| `VTOP_DR_VERIFICATION_REPAIR_S` | 60 | An object changed at rest: from the scrub's finding until a clean scrub, after the bad version is removed. |
| `VTOP_DR_KEY_ROTATION_S` | 120 | The commands of a rotation, from the first archive under the old key to the last scrub. |

The RPO is not a time here. The archive's RPO is **zero records while the
source still holds them**; when it does not, the loss is a count that the
procedure reports exactly. What each procedure loses is in its section.

## Conventions used in the steps

Every command below is run on the engine's host (the replacement host, after a
host loss), as the user the engine runs as, against `/etc/vtop/config.yaml`.
The names in the commands mean this:

| Name | What it is |
|---|---|
| `vtopctl` | A build with the `postgres` feature for every PostgreSQL step: release binaries and container images up to v0.10.2 are not ([Install](../install/index.md)). |
| `VTOP_STATE_STORE` | The PostgreSQL URL the engine's `engine.state_store: { env: VTOP_STATE_STORE }` resolves to, exported in your shell. Its database name is the one you create below: `vtop` in these steps. |
| `ADMIN_URL` | A PostgreSQL URL for the server's maintenance database (`.../postgres`) as a role that may create, rename and drop databases. Not the engine's role. |
| `BROKERS`, `GROUP`, `TOPIC` | The Kafka bootstrap servers, `sources.kafka.consumer_group`, and the topic or topics `sources.kafka.topic_include_regex` matches. `kafka-consumer-groups.sh` and `kafka-get-offsets.sh` come from a Kafka distribution; add `--command-config client.properties` when the brokers need authentication. |
| Object store commands | Shown for S3 with the AWS CLI (`aws s3`, `aws s3api`); the drills use MinIO's `mc`, where `mc ls --recursive <alias>/<bucket>/<prefix>` is the same listing. Credentials are the AWS SDK's chain (`configure/basics.md`), the same the engine uses. |
| Start and stop the engine | `systemctl start vtop-engine` and `systemctl stop vtop-engine` for the [example unit](https://github.com/allamiro/vtop-engine/blob/main/examples/systemd/vtop-engine.service); scale the Deployment to 0 and back to 1 on Kubernetes; `vtopctl run --config /etc/vtop/config.yaml` in a terminal. |
| Time to the first commit, and to be caught up | The bounds below are measured from a step to the first `source_committed` batch, and to nothing left in flight. Read them with `vtopctl status --config /etc/vtop/config.yaml`, which prints the batch count for each state: the first commit is when `source_committed` rises above its old count; caught up is when no batch is in any state but `source_committed`, `superseded`, `failed` or `replay_required` and the `source_committed` count stops rising while the source is quiet. With `VTOP_METRICS_ADDR` set, `vtop_commits_total` is the same signal. |

When a manifest MAC key is configured, `migrate`, `scrub` and `audit verify`
need it in the environment as the engine does
([Auditing](auditing.md#tamper-evidence)); set it before the steps.

## Before any recovery

- **One engine per ledger.** Stop the engine, or know that the host it ran on
  is gone and cannot come back. Nothing on the new side fences the old one for
  you ([Losing the engine's host](../deploy/engine-host-loss.md)).
- **Record the audit trail's end, routinely, before you need it.**
  `vtopctl audit verify --config ...` prints a row count and the last link. A
  chain cannot show rows removed from its end, so a copy of that line, kept
  where the ledger's writers cannot reach it (taken daily, say), is the only
  evidence of what a lost ledger held ([Auditing](auditing.md)). After a ledger
  is lost the command cannot be run: if you kept no copy, say so in the
  incident record. When the old ledger is still readable (a bucket loss, a
  restore), run it first and keep the output.
- **Keep the old ledger when there is one**, as a dump and as a renamed
  database. It is the record of what was lost.
- **A new PostgreSQL database needs its roles and grants.** The steps below
  say "create the database, `vtopctl migrate`, start". If your deployment runs
  the two identities of [PostgreSQL in depth](../POSTGRES_DEPLOYMENT.md#identities)
  (a migration owner and a runtime role), do it in that order: create the
  database owned by the migration owner (`CREATE DATABASE vtop OWNER
  <migration owner>`); run `vtopctl migrate` with `VTOP_STATE_STORE` set to the
  **migration owner's** URL; apply the runtime grants of
  [the deployment sequence](../POSTGRES_DEPLOYMENT.md#deployment-sequence);
  then start the engine with `VTOP_STATE_STORE` set to the **runtime role's**
  URL. On a new server (a region loss), create both roles first: a dump does
  not carry roles. A deployment with one role that owns everything, as the
  drills use, skips the grants. The drills run one role, so this is the part of
  these steps they do not exercise.
- **After every recovery** run the three commands that follow a restore, and
  do not call the recovery complete until they pass:

  ```bash
  vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
  vtopctl scrub --config /etc/vtop/config.yaml --sample 100
  vtopctl audit verify --config /etc/vtop/config.yaml
  ```

## The ledger is lost, with no backup

The ledger's database is gone and nothing backed it up; the bucket is intact.
The ledger holds each batch's state and the position the sources resume from;
the objects and manifests are in the bucket and are untouched.

**What the engine does by itself.** Nothing: it has no ledger. Start it on a new
one and it archives from where the source says it was:

- **A Kafka source** resumes at the consumer group's committed offset, which is
  on the broker. No record is archived twice and none is missed.
- **A file or spool source** takes its position from the ledger, so it starts at
  the beginning of the file and reads it all again. The same bytes resolve to
  the same object key, so each object is found, verified and reused, and not
  written again.

**What the engine cannot rebuild.** The old ledger's history: the batch ids and
timestamps of every batch, and the audit trail's chain, which starts again at
the first row of the new ledger. With a Kafka source and no extra step, the
records archived before the loss stay in the bucket but the new ledger does
not know them, so `scrub` and `audit-coverage` do not look at them. Prune
digests that were not backed up are gone with the ledger.

### Steps

1. Stop the engine. Confirm the bucket still holds the archive
   (`aws s3 ls --recursive s3://<bucket>/<prefix> | head`; with MinIO,
   `mc ls --recursive <alias>/<bucket>/<prefix> | head`).
2. Create the new database and its schema (with two database identities, the
   [roles and grants](#before-any-recovery) too):

   ```bash
   psql "$ADMIN_URL" -c 'CREATE DATABASE vtop'
   vtopctl migrate --config /etc/vtop/config.yaml
   ```

   `migrate` ends with `state-store migration complete (postgresql); audit
   record <id>` and exit 0; an error names what it lacked. For SQLite, remove
   nothing: the engine creates the file at its path (this variant is not
   drilled: the drills run the PostgreSQL ledger).
3. **Kafka source only: choose.**
    - *Resume.* Do nothing. The new ledger proves the records archived from now
      on.
    - *Rebuild.* To have the new ledger prove the history too, while the broker
      still holds it, reset the consumer group to the start of the topic
      **before the engine first starts on the new ledger**:

      ```bash
      kafka-consumer-groups.sh --bootstrap-server "$BROKERS" --group "$GROUP" \
        --topic "$TOPIC" --reset-offsets --to-earliest --execute
      ```

      Kafka refuses the reset unless the group has no active member
      (`kafka-consumer-groups.sh --bootstrap-server "$BROKERS" --describe
      --group "$GROUP" --state` says `Empty`); after a host loss the old
      consumer leaves only when its session times out, so wait for `Empty`.
      The tool prints each partition's new offset: the topic's first retained
      offset, `0` unless retention has removed records. Give `--topic` once for
      each topic the engine reads, or `--all-topics`.

      The engine reads everything the topic still holds, finds each object at
      its key and reuses it. It will not move a group behind a ledger that
      already proves the records, so doing this after the engine has run on the
      new ledger has no effect. A record the broker no longer holds stays in the
      bucket and stays unproven.
4. Start the engine with the same configuration (`systemctl start
   vtop-engine`, or `vtopctl run --config /etc/vtop/config.yaml`). Wait for the
   first commit and then for it to be caught up, as
   [read from `vtopctl status`](#conventions-used-in-the-steps).
5. Run the [three checks](#before-any-recovery); each exits non-zero on a
   finding.

### Pass criteria

- Every record the source held is in the bucket, counted from the objects; none
  is missing.
- *Kafka, resume:* none is archived twice, and the new ledger proves exactly
  the records archived since the loss. The drill reports how many records are
  in the bucket that the ledger no longer proves.
- *Kafka, rebuild* and *file:* none is archived twice when the batches are
  rebuilt within the hour of their original time partition (below), and the new
  ledger proves every record.
- The checks pass, and the time to the first commit and to be caught up is
  inside `VTOP_DR_LEDGER_LOST_RTO_S`.

### What the drill measured

| Leg | First commit | Caught up | Records | Missing | Archived twice | Records the new ledger proves | Ledger lost |
|---|---|---|---|---|---|---|---|
| file | 0.50 s | 0.59 s | 180 | 0 | 0 | 180 of 180 | 6 batches, 120 records |
| Kafka, resume | 0.57 s | 3.3 s | 180 | 0 | 0 | 60 of 180 | 6 batches, 120 records |
| Kafka, rebuild | 3.2 s | 11.9 s | 180 | 0 | 0 | 180 of 180 | 6 batches, 120 records |

Conditions are under [Conditions of the measurements](#conditions-of-the-measurements).

!!! warning "A rebuild an hour later writes second copies"
    The object key carries the time partition (`partitioning.time_source`,
    `seal` by default: the hour the batch was sealed). A rebuild that runs in a
    later hour than the original seals its batches in that later hour, names
    other keys, and archives the same records a second time. That is a
    duplicate and never a hole, but it is storage, and it is not what the
    measurement above shows: the drill runs within one hour. With a Kafka source
    and `partitioning.time_source: event` the partition comes from the records'
    own timestamps and the keys match at any distance. This is read from the
    [reference](../configure/reference.md#partitioning), not drilled across an
    hour boundary.

## The bucket is lost

The store's data is gone, with no replica, backup or earlier version to bring
back. The ledger is intact and says every batch is committed. The source went
on receiving records, and some of what it held has expired since.

**What the engine does by itself.** While the store is down every batch fails
and is read again; stopping the engine then exits non-zero with `shutdown flush
incomplete`, which is expected (what it had not archived is read again on the
next start). Once a store answers, the engine writes new batches, but it has no
way to know that committed batches' objects are gone, and it never re-reads a
committed range.

**What the engine cannot rebuild.** What the source no longer holds.

### Steps

1. **A store that answers, and a copy if there is one.** The next step asks
   the store what it holds, so something must answer at the address the config
   names (or change `upload.endpoint_url`). If the store has versioning,
   replication or a backup, restore the bucket from it, then go to
   [A region is lost](#a-region-is-lost), step 4; the rest of this section is
   for when there is no copy. Otherwise create a new, empty bucket with the
   same name, and with what the config requires of it. Versioning, when
   `upload.require_object_versioning` is set; Object Lock, when
   `upload.object_lock` is set, which can only be turned on when the bucket is
   created. The engine does not create it (`upload.create_bucket` is `false`
   by default, and is refused with Object Lock):

   ```bash
   aws s3api create-bucket --bucket <bucket> --region <region> \
     --create-bucket-configuration LocationConstraint=<region>   # omit the last flag in us-east-1
   aws s3api put-bucket-versioning --bucket <bucket> \
     --versioning-configuration Status=Enabled
   ```

2. **Find out what is gone.**

   ```bash
   vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
   vtopctl scrub --config /etc/vtop/config.yaml
   ```

   Both exit non-zero. The audit prints one `FAIL storage` line for each
   missing object and each missing manifest, with the batch id; the scrub
   counts `missing=N` and names each batch. These are exact: N is the number of
   committed batches the store lost. (Asked of a store that is not there at
   all, they fail with a connection error and name nothing: that is step 1
   not done.)
3. **Keep the old ledger as the record of the loss**, then start a new one.
   Stop the engine and anything else connected first: PostgreSQL refuses to
   rename a database that has connections, and the old ledger cannot be kept in
   use.

   ```bash
   pg_dump --format=custom --file=/backup/ledger-lost-bucket.dump "$VTOP_STATE_STORE"
   psql "$ADMIN_URL" -c 'ALTER DATABASE vtop RENAME TO vtop_lost_bucket'
   psql "$ADMIN_URL" -c 'CREATE DATABASE vtop'
   vtopctl migrate --config /etc/vtop/config.yaml
   ```

   With two database identities, the [roles and grants](#before-any-recovery)
   apply to the new database. The old ledger cannot be kept in use: its rows
   say the ranges are committed, and the engine never reads a committed range
   again.
4. **Reset the consumer group to the start of what the broker still holds**,
   with the engine stopped and the group `Empty` (see
   [the lost-ledger steps](#the-ledger-is-lost-with-no-backup), step 3, for
   the check and the options):

   ```bash
   kafka-consumer-groups.sh --bootstrap-server "$BROKERS" --group "$GROUP" \
     --topic "$TOPIC" --reset-offsets --to-earliest --execute
   ```

   The tool prints the new offset of each partition: the first offset the
   broker still holds. For a file or spool source the engine reads what is left
   of the file from its start; nothing needs resetting.
5. Start the engine, wait until it has archived the source (the first commit,
   then caught up, [read from `vtopctl status`](#conventions-used-in-the-steps)),
   and run the [three checks](#before-any-recovery).
6. **Report the loss**, from the old ledger. The records lost are the committed
   batches whose range is below the first offset the broker still holds, for
   each partition. Get that offset (the last field of each line is it, the
   fields are `topic:partition:offset`):

   ```bash
   kafka-get-offsets.sh --bootstrap-server "$BROKERS" --topic "$TOPIC" --time -2
   ```

   Then ask the **renamed** database, once for each partition, with
   `OLD_LEDGER_URL` the URL of `vtop_lost_bucket` (not the new `vtop`):

   ```bash
   psql "$OLD_LEDGER_URL" -v topic="$TOPIC" -v partition=0 -v first=<first retained offset> <<'SQL'
   SELECT batch_id, record_count, progress_start_json, progress_end_json
   FROM batches
   WHERE state = 'source_committed' AND source_type = 'kafka'
     AND (progress_end_json::json)->>'topic' = :'topic'
     AND ((progress_end_json::json)->>'partition')::int = :partition
     AND ((progress_end_json::json)->>'end_offset')::bigint < :first;
   SQL
   ```

   The sum of `record_count` is the number of records lost. A batch that
   straddles the first retained offset is partly lost and is not in this
   list; its `start_offset` is below `:first` and its `end_offset` is not. With
   a topic of several partitions, repeat for each with its own offset.

### Pass criteria

- The loss is **reported exactly**: the audit's `FAIL storage` lines are an
  object and a manifest for each committed batch, and the scrub's `missing` is
  the number of batches, no more.
- Everything the source still holds is archived again, none of it twice, and
  the new ledger passes the checks.
- The records missing from the new archive are exactly the ones the old ledger
  committed below the first retained offset.
- The time from step 3 until the retained records are archived again is inside
  `VTOP_DR_BUCKET_LOST_RTO_S`.

### What the drill measured

A topic of 180 records and six committed batches lost with the store; the
topic's first 40 records expired from the broker before the recovery.

| Records | Archived again | Lost | Archived twice | The old ledger's loss report | Objects reported missing | RTO |
|---|---|---|---|---|---|---|
| 180 | 140 (offsets 40 to 179) | 40 (offsets 0 to 39) | 0 | 40 records | 12 (6 objects, 6 manifests) | 9.6 s |

The loss is the source's retention: nothing the broker held was lost, and what
it had dropped could not be got back from anywhere else.

## A region is lost

The primary region held the engine, the PostgreSQL ledger and the bucket, and
is gone. The DR region holds a replica of the bucket (the store's own
replication: S3 cross-region replication, MinIO bucket replication) and the
ledger's latest backup ([Backup and recovery](backup-and-recovery.md)). **The
source must survive the region**: a stretched or mirrored Kafka cluster, or
files on storage the DR region mounts. If it does not, the recovery below still
restores what the replica and the backup hold and nothing more. The drill
keeps one Kafka broker and the group's offsets across the disaster; whether a
mirror carries the consumer group's committed offsets, and what the engine does
when they are missing or behind the ledger, is not drilled: check the group's
offsets with `kafka-consumer-groups.sh --describe --group "$GROUP"` before
step 6.

**What the engine does by itself.** On a restored ledger that is older than the
consumer group, it logs that the group is ahead of the ledger and resumes from
the ledger's position, so the records archived after the backup are read
again. Objects the replica already holds are found and reused.

**What the engine cannot rebuild.** Objects the replica did not receive before
the region died, for ranges the restored ledger already calls committed: it
never re-reads a committed range. The check in step 4 names them.

### Steps

1. **Fence.** Make sure nothing in the primary region can start again.
2. **A ledger server in the DR region, with the backup restored**, as in
   [Dump and restore](backup-and-recovery.md#dump-and-restore). A new server
   has none of the roles the dump refers to: create them first (the migration
   owner and the runtime role of [the deployment](../POSTGRES_DEPLOYMENT.md#identities),
   from your deployment's script or a `pg_dumpall --roles-only` you kept), or
   `pg_restore --exit-on-error` stops at the first `OWNER TO` or `GRANT` that
   names a missing role. Then:

   ```bash
   createdb vtop
   pg_restore --exit-on-error --dbname=vtop /backup/ledger.dump
   mkdir -p /var/lib/vtop/ledger-digests
   cp -a /backup/digests/. /var/lib/vtop/ledger-digests/
   vtopctl migrate --config /etc/vtop/config.yaml
   ```

3. **Point the engine at the DR bucket.** The same configuration and secrets,
   with `upload.endpoint_url` (or `VTOP_S3_ENDPOINT_URL`) and `upload.region`
   of the DR region's store, the same bucket name, prefix, consumer group and
   manifest key. The credentials are not in the config: the DR host's own AWS
   credentials (an instance profile, or `AWS_ACCESS_KEY_ID` and
   `AWS_SECRET_ACCESS_KEY` in the engine's environment) must reach the DR
   bucket. The Kafka brokers in `sources.kafka.bootstrap_servers` must be ones
   the DR region can reach. Set `engine.ledger_digest_dir` to the directory you
   restored the digests into.
4. **Check before the engine starts:**

   ```bash
   vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
   ```

    - It **passes**: the replica holds every object the restored ledger calls
      committed. Go to step 6.
    - It **fails**: the replica was behind the ledger. The `FAIL storage` lines
      name each object and manifest the DR region lacks. Go to step 5.
5. **Repair: a new ledger, and read the source again.** The same repair as for
   [a lost ledger](#the-ledger-is-lost-with-no-backup), *rebuild*: keep the
   restored ledger (`ALTER DATABASE ... RENAME`), create a new one, `vtopctl
   migrate`, reset the consumer group to the start of the topic, start. Each
   object the replica has is found and reused, and each it lacks is written.
6. Start the engine (`vtopctl run`), wait until it has archived the source, and
   run the [three checks](#before-any-recovery).

### Pass criteria

- *The replica is complete:* the check in step 4 passes; the engine resumes
  from the ledger and says so; every record is in the DR bucket, none archived
  twice; the ledger proves them all again; it loses the rows written since the
  backup.
- *The replica is behind:* the check names exactly the objects and manifests
  the DR region lacks, one pair for each batch; after the repair no record is
  missing and none archived twice.
- RTO, from step 2 until the engine has archived the source, is inside
  `VTOP_DR_REGION_LOST_RTO_S`.

### What the drill measured

The ledger backup is taken after six batches; the primary region dies after
nine.

| Leg | The backup held | The DR bucket held at the disaster | Ledger rows lost | Check in step 4 | First commit | Caught up | Missing | Archived twice |
|---|---|---|---|---|---|---|---|---|
| Replica complete | 6 batches | 180 of 180 archived records | 3 batches | passes | 2.9 s | 9.9 s | 0 | 0 |
| Replica behind (cut off for the last three batches) | 9 batches | 120 of 180 archived records | 0 batches | fails: 6 objects, naming exactly the last three batches | 13.4 s (the repair) | 13.4 s | 0 | 0 |

The records the replica lacked were archived again from the source: that is
the RPO of the archive, 60 records here, and it is zero losses only because the
source still held them.

## A disk is full

One of the three disks the engine depends on is full: its work directory, the
disk its ledger is on, or the store's own disk.

**What the engine does by itself.** The batches that need the full disk fail
and are read again, and the engine keeps running. The log says which disk:
`compression failed: ... No space left on device (os error 28)` for the work
directory, `state store error: ... database or disk is full` for a SQLite
ledger, and `put_object ... XMinioStorageFull (http 507)` for a store (another
store says it in its own words). Nothing is committed that is not verified.
When there is space again it resumes by itself, with no restart.

**What it cannot rebuild.** Nothing is lost. A batch that was uploaded but
whose commit could not be written is archived again, so at most one batch's
records are in the bucket twice.

### Steps

1. Find the disk from the engine's log (above): `journalctl -u vtop-engine
   --since '1 hour ago' | grep -iE 'no space|disk is full|StorageFull'`, or
   `kubectl logs` for a pod. Match the message to a path: the work directory
   (`engine.work_dir`), the disk the SQLite ledger is on (the path in
   `engine.state_store`), or the store's own volume. `df -h <path>` shows a
   full disk; `df -i <path>` shows one out of inodes.
2. Free space on it: grow the volume, move what else lives there, or rotate
   logs. Do not delete the work directory's staged batches or the ledger's
   `-wal` file by hand.
3. If the engine has exited (it did not in the drill; `systemctl status
   vtop-engine` says), start it: `systemctl start vtop-engine`, or `vtopctl run
   --config /etc/vtop/config.yaml`. Otherwise nothing: give it up to the
   bound below (90 s; the drill took 12 to 15 s, most of it the engine's retry
   back-off) before you decide it is stuck, then read its log.
4. Run the [three checks](#before-any-recovery).

### Pass criteria

- No batch is committed that the store does not hold: `audit-coverage
  --check-storage` and `scrub` pass at the end.
- Every record is archived, with at most one batch's records twice.
- The engine's log names the full disk.
- The time from freeing the space to every record committed is inside
  `VTOP_DR_DISK_FULL_RTO_S`.

### What the drill measured

A file source of 40 records archived, then a disk filled to the last byte for
20 seconds while 60 more were appended, batches of 20.

| Disk | The engine survived | Restart needed | RTO | Missing | Archived twice | Committed while full |
|---|---|---|---|---|---|---|
| Work directory | yes | no | 14.1 s | 0 | 0 | 0 batches |
| SQLite ledger | yes | no | 15.0 s | 0 | 0 | 0 batches |
| Store | yes | no | 12.2 s | 0 | 0 | 0 batches |

Most of the RTO is the engine's retry back-off, not the work.

**Not drilled:** the PostgreSQL server's own data volume. A PostgreSQL server
that cannot write its WAL stops, and what happens next belongs to PostgreSQL:
free the space, start it, and run the three checks. A managed PostgreSQL
service grows or pages you before that.

## Verification fails

### While the engine verifies

The engine uploads a batch and reads the stored object and manifest back before
it commits. If the store answers wrongly, the batch is not committed: no
`SOURCE_COMMITTED` row is written without a strong verification.

- **The store's own checksum disagrees with the engine's** (what a faulty
  gateway or a store bug looks like). The batch is marked `FAILED`, its range
  is read again, the ERROR line says `object verification failed`, and
  `vtop_verification_failures_total` counts it, which fires
  [VtopVerificationFailures](runbooks.md#vtopverificationfailures).
- **The store returns other bytes than it holds while the engine reads them
  back** (checksum `blake3`, which verifies by reading the body). The
  response's own integrity check stops the read, and the engine treats it the
  same way: `object verification failed: stored content failed the checksum the
  store sent with it while it was read back`, counted in the same metric. (An engine built
  before #735 logged `io error: streaming error` for this, counted nothing and
  left the batches' ledger rows in flight until the next start; the batch was
  never committed either way.)

**Steps.** Fix the store, or whatever sits between the engine and the store. Do
not restart the engine: it commits by itself once the store is right. Then run
the [three checks](#before-any-recovery). The coverage audit reports the
objects the failed attempts left as `unreferenced objects`: storage cost, never
loss, and the command deletes nothing.

### An archived object changes at rest

A committed object is overwritten or deleted in the store. The engine does not
read it again; the [scrub](scrub-and-audit.md#scrub) finds it.

**Steps.**

1. `vtopctl scrub --config /etc/vtop/config.yaml` exits non-zero and names the
   batch: `mismatched` for another content, `missing` for a deleted object.
2. Find the object's key: `vtopctl --json list-batches --config
   /etc/vtop/config.yaml` has each batch's `object_uri` (`s3://<bucket>/<key>`)
   beside its `batch_id`; the scrub names only the batch.
3. If the bucket keeps versions, list them and remove the bad one, so that the
   previous one is current:

   ```bash
   aws s3api list-object-versions --bucket <bucket> --prefix <key>
   aws s3api delete-object --bucket <bucket> --key <key> --version-id <bad version id>
   ```

   With MinIO: `mc ls --versions <alias>/<bucket>/<key>` and `mc rm
   --version-id <bad version id> <alias>/<bucket>/<key>`. The bad version is
   the newest one (the top of the listing). Under S3 Object Lock a locked
   version cannot be removed before its retention date; the drill's bucket has
   no lock, so that case is not drilled. Scrub again: it passes.
4. If there is no earlier version, or no replica to copy from, the object is
   lost. `scrub` and `audit-coverage --check-storage` report it exactly, with
   the batch id; the ledger's `record_count` for that batch is the number of
   records lost. The engine will not archive a committed range again: record
   the loss, and if the source still holds the range use the repair under [the
   bucket is lost](#the-bucket-is-lost).

### Pass criteria

- *While the engine verifies:* nothing is committed during the fault; the
  engine is still running; once the store is right it commits every record with
  no restart, inside `VTOP_DR_VERIFICATION_RTO_S`; the committed objects hold
  every record once; no ledger row is left in flight.
- *At rest:* the scrub reports exactly one `mismatched` object and names the
  batch; after the bad version is removed it is clean, inside
  `VTOP_DR_VERIFICATION_REPAIR_S`; a deleted object with no version is reported
  as one `missing` and the audit fails on it too.

### What the drill measured

A file source of 100 records in batches of 20, the fault held for 20 seconds
while the last 60 were appended.

| Leg | Altered responses | Committed during the fault | Failures counted | RTO | Rows left in flight | Unreferenced objects |
|---|---|---|---|---|---|---|
| The store's checksum is wrong | 19 | 0 | 7 | 13.1 s | 0 | 6 |
| The store returns other bytes | 13 | 0 | 7 | 15.3 s | 0 | 6 |

At rest: the scrub reported the one overwritten object (of five), and removing
the bad version took the scrub from the finding to clean in 0.21 s. A deleted
object lost one batch of 20 records, reported as `missing=1`.

## Key rotation

[Rotate the manifest MAC key](manifest-key-rotation.md) is the procedure. The
drill runs it literally: an archive signed the way every archive was before key
ids, then steps 1, 3, 4 and 5.

### Pass criteria

- After step 4 the scrub exits 0, every manifest is `ok`, and the count by key
  is the number of batches each key signed, the ones with no key id counted
  under the key marked `legacy`.
- After step 5 the scrub reports exactly the manifests the withdrawn key signed
  (and the ones with no key id) as `unauthenticated`, with the messages the
  page documents, and none of the active key's.
- Taking the id out of `retired` and back into `keys` makes it clean again.
- A keyring that dropped the old key, instead of tombstoning it, fails with the
  page's `not in this keyring` and `no key ... is marked legacy` messages.
- Nothing is lost: the archive holds every record once.

### What the drill measured

Three phases of 40 records in batches of 20, six manifests: four signed under
the old key (two with no key id) and two under the new. The scrub counted
`q3-2026=4 q4-2026=2`, and after the withdrawal reported four `unauthenticated`
and two `ok`. The procedure's commands took 1.3 s, inside
`VTOP_DR_KEY_ROTATION_S`.

## Running the drills

Each drill is a script under
[`scripts/dr-drill/`](https://github.com/allamiro/vtop-engine/tree/main/scripts/dr-drill)
that starts its own containers, runs, and removes them:

| Drill | Script | Needs |
|---|---|---|
| The ledger is lost | `ledger-lost.sh` | PostgreSQL, Kafka, MinIO |
| The bucket is lost | `bucket-lost.sh` | PostgreSQL, Kafka, MinIO |
| A region is lost | `region-lost.sh` | PostgreSQL, Kafka, two MinIO |
| A disk is full | `disk-full.sh` | MinIO; root or passwordless `sudo` to mount a tmpfs |
| Verification fails | `verification-failed.sh` | MinIO |
| Key rotation | `key-rotation.sh` | MinIO |

```bash
cargo build -p vtop-cli --features postgres --bin vtopctl
scripts/dr-drill/ledger-lost.sh target/debug/vtopctl
```

They need bash 5 (macOS's `/bin/bash` is 3.2: run them with Homebrew's), docker,
python3, curl and sqlite3, and about 1 GB of memory for each container the
drill starts (MinIO, PostgreSQL, Kafka: the region drill starts two MinIO
servers as well). The lab's MinIO image is
built from `docker/minio` when it is not present (`docker compose build
minio`). `DR_DRILL_NETWORK=host` makes every helper container share the host's
network, as CI does; `DR_DRILL_WORKDIR`, `DR_DRILL_KEEP` and the image
variables are described at the top of
[`scripts/dr-drill/lib.sh`](https://github.com/allamiro/vtop-engine/blob/main/scripts/dr-drill/lib.sh).
Each drill prints one `PASS` or `FAIL` line for every criterion above, writes
its measurements to `result.json`, and exits non-zero if any criterion failed.
For a second person's run, each takes `--handover`, `--check` and `--teardown`
([The second person](#the-second-person)).

CI runs them from the `disaster-recovery drills` workflow, nightly and on
dispatch, never on a pull request: each run builds `vtopctl` and starts
containers. The workflow keeps each drill's `result.json` and logs as an
artifact. The scripts are shellchecked, and their helpers unit-tested, on every
pull request that touches them.

### Conditions of the measurements

Every figure above is from one run of the drill: a `vtopctl` debug build run in
a container capped at two CPUs on Docker Desktop on an arm64 laptop; MinIO
(built by `docker/minio`), PostgreSQL 18 and Apache Kafka 4.3.1 in containers
at one CPU each; batches of 20 records of about 60 bytes, gzip level 1, SHA-256
(BLAKE3 where it says so); a ledger of a few dozen rows. They show that the
procedures work and what their parts cost on a small archive. They are not a
promise about yours, and the first CI runs will add their own column.

## What the drills do not cover

- **The source's own recovery.** The drills keep Kafka up. A region loss that
  also loses the source loses whatever the source held and the archive did not.
- **A source that deletes after commit**, and a Kafka topic whose retention is
  shorter than your RTO plus the time to notice.
- **The PostgreSQL server's own data volume filling**, and a PostgreSQL
  failover; see [Losing the engine's host](../deploy/engine-host-loss.md).
- **Ledgers and archives of any size.** The ledgers hold a few dozen rows; the
  time to read a large source again, and to copy a large bucket back, grows
  with them.
- **S3 Object Lock.** A locked object cannot be removed or overwritten before
  its date; the at-rest leg runs on a bucket without it.
- **Rebuilding across an hour boundary**, as described above.
- **A SQLite ledger.** Only the full-disk drill uses one; the lost-ledger,
  lost-bucket and lost-region drills run on PostgreSQL. The steps for a SQLite
  ledger differ only in the ledger step (no database to create), and were not
  run.
- **Two database identities.** The drills run one PostgreSQL role that owns
  everything; the roles and grants of [PostgreSQL in
  depth](../POSTGRES_DEPLOYMENT.md#deployment-sequence) are not exercised.
- **An object store other than MinIO.** The drills' store is MinIO; the AWS
  commands in the steps are not run by them.
- **Managed services.** A managed PostgreSQL or store has its own backup and
  recovery; use it, then run the three checks.

## The second person

The runbooks above are run by their author, and the drill is the same author
following them. What remains is a person who did not write them running each
one on a lab, within a stated time, with their notes folded back into the
page. Each drill has a mode for that: it builds the lab, causes the disaster,
and stops, so that the second person starts from the same place the drill's
recovery steps do and has to follow only this page.

```bash
# 1. The lab, and the disaster. Nothing is recovered.
scripts/dr-drill/bucket-lost.sh target/debug/vtopctl --handover

# 2. The second person follows the page's section for that disaster,
#    in the lab, and stops the engine when they are done.

# 3. Only the pass criteria, against what they left: RPO, RTO, pass or fail.
DR_DRILL_WORKDIR=/tmp/vtop-dr-handover-bucket-lost \
  scripts/dr-drill/bucket-lost.sh target/debug/vtopctl --check

# 4. Remove the lab: containers, volumes, mounts, processes, the directory.
DR_DRILL_WORKDIR=/tmp/vtop-dr-handover-bucket-lost \
  scripts/dr-drill/bucket-lost.sh target/debug/vtopctl --teardown
```

`--handover` prints, and keeps in `HANDOVER.txt` in the lab's directory: the
section of this page to follow; what the disaster destroyed and what the lab
holds (the databases, topics, buckets and files by name, the configuration, the
backup where the scenario has one); every endpoint with its credentials and
how to reach it; `lab.env`, a file to source for the store's credentials, the
ledger's connection string and any key; and the exact `--check` and
`--teardown` commands. It runs none of the recovery steps, and it refuses to
hand over a lab that is not in the state the drill expects (the disaster did
not take). Names the person must use are given, so that the configuration and
the check find what they make: the new database, the file the DR
configuration is written to.

`--check` runs only the pass criteria of the drill's outcome, against the lab
as the person left it, and exits non-zero if one fails. It reports the RPO,
which is the records lost, and the RTO, which is the time from the handover to
the start of the check, so it includes the person's reading and typing: it is
printed beside the bound above and not enforced, because the bound is the
automated procedure's. It is safe to run again after a fix, and it removes
nothing. What it cannot judge is what the person saw on the way (the audit's
`FAIL storage` lines, for example): those are criteria of the drill's own run.

A drill with legs hands over the first leg, or the one `--leg` names, and
`--check` and `--teardown` follow the leg in the lab. The legs are:
`ledger-lost` `file`, `kafka`, `kafka-rebuild`; `region-lost` `replicated`,
`lagging`; `disk-full` `work`, `ledger`, `store`; `verification-failed`
`engine`, `readback`, `rest`. `DR_DRILL_WORKDIR` names the lab's directory and
defaults to `$TMPDIR/vtop-dr-handover-<drill>`; it must be empty for a
handover. The key-rotation lab is the archive before the rotation: the person
runs the procedure, writes the configuration they end step 4 with to
`rotated.yaml` in the lab's directory, and `--check` judges that and the
archive. The full-disk legs that mount a tmpfs need root or passwordless
`sudo`, as in a run.

For each runbook, the second person:

1. starts the lab with `--handover`, and reads what it prints;
2. follows only this page, notes every step that needed a guess, and times the
   run from the first step to the last check;
3. runs `--check`, and sends the notes and the check's report, which are
   folded into the page's steps.

| Runbook | Run by | Date | Time taken | Notes folded back |
|---|---|---|---|---|
| The ledger is lost | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | Variables and tools defined; roles and grants, MAC key for `migrate`, the audit-trail end as a routine record, the group reset's preconditions. |
| The bucket is lost | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | New store first (the audit needs a store that answers); bucket creation commands; renamed-database loss report that runs (psql variables, topic and partition). |
| A region is lost | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | Roles on the new server; credentials and region; mirror offsets named as not drilled. |
| A disk is full | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | How to find the log and the disk; what to free; how long to wait. |
| Verification fails | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | Batch id to object key; AWS commands beside `mc`; Object Lock caveat. |
| Key rotation | desk run; no hands-on run yet | 2026-11-22 | not timed (nothing was run) | Key file recipe, restarts, the checking shell's access: see [the rotation page](manifest-key-rotation.md). |

The six rows above are a **desk run**: each page was read, step by step,
against the code, the CLI and the drill scripts, and the gaps found are folded
into the steps. Nothing was executed, so no time is recorded, and a desk run
is not the second person's run: no row is closed until a person who did not
write the runbook runs it on a lab and writes their own row.

### Causing each disaster by hand

The script headers describe each drill's own mechanics. For a second person
without the drill's lab, this is what each one does to cause the disaster, so
that the page's steps can be followed on another host. In every case the engine
is running and has archived a first stretch of records (120 in the drills)
before the disaster, and the source keeps receiving records after it.

| Runbook | Cause it by | Script |
|---|---|---|
| The ledger is lost | Stop the engine, `DROP DATABASE vtop WITH (FORCE)`, remove the digest directory and the work directory. Keep the bucket. Append records to the source meanwhile. | `ledger-lost.sh` |
| The bucket is lost | Destroy the store's data while the engine runs (delete the bucket and everything in it, including versions). Go on producing. Let the broker expire the topic's first records: `kafka-delete-records.sh --bootstrap-server "$BROKERS" --offset-json-file trim.json` with `{"partitions":[{"topic":"<topic>","partition":0,"offset":40}],"version":1}`. Stop the engine. | `bucket-lost.sh` |
| A region is lost | Replicate the versioned bucket to a second one (S3 cross-region replication; MinIO `mc replicate add`). After a few batches take the ledger backup (`pg_dump --format=custom` and the digest directory). Let more batches archive. Then kill the engine, the ledger server and the primary bucket together. For "the replica is behind", stop replication for the last batches before the disaster and take the backup afterwards. | `region-lost.sh` |
| A disk is full | Put the directory on a small volume (a 4 MB tmpfs: `mount -t tmpfs -o size=4m tmpfs <dir>`, needs root) and fill it to the last byte with `dd if=/dev/zero of=<dir>/filler bs=512`. For the store, give it a small volume of its own and fill that. Append records for 20 seconds, then delete `filler`. | `disk-full.sh` |
| Verification fails, while the engine verifies | Put `scripts/dr-drill/fault-proxy.py <listen port> <store host:port> <control file> <stats file>` between the engine and the store (point `upload.endpoint_url` at the proxy), then `echo head > <control file>` (the store's own checksum is wrong) or `echo get > <control file>` (a read-back returns another first byte; use `checksum.algorithm: blake3`). Remove the control file to end the fault. | `verification-failed.sh` |
| Verification fails, at rest | Overwrite a committed object (`aws s3 cp - s3://<bucket>/<key> <<< 'not the archived object'`, or `mc pipe`), or delete every version of it. | `verification-failed.sh` |
| Key rotation | Nothing to break: archive under the old key with `manifest_mac_key_env` first, then follow [the rotation page](manifest-key-rotation.md). | `key-rotation.sh` |


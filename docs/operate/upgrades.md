# Upgrades

VTOP is at 0.x: a minor version may break compatibility. Two rules hold for
every upgrade.

- **Upgrades are tested.** CI checks that a data directory written by the
  newest patch of each released minor series (v0.2 onwards) opens under the
  current build, serves its records byte for byte, and still serves them after
  a restart ([`scripts/upgrade-compat.sh`](../../scripts/upgrade-compat.sh)).
- **Downgrades are not supported**, except for what the
  [compatibility matrix](../reference/compatibility.md#downgrade) records for
  the previous release: the state ledger, its prune digests and the audit
  trail. A newer release may write data an older one cannot read, and nothing
  else tests that path. Going back means a fresh directory, or a verified
  backup taken before the upgrade.

The source for this page is the release notes in the [roadmap](../ROADMAP.md);
read the section for the release you are moving to. What each format
promises, and the rules for changing one, are in the
[compatibility policy](../reference/compatibility.md); which releases get
fixes is in [Versioning and support](../SUPPORT.md).

## Upgrading from v0.10.4 to v0.11.0

v0.11.0 keeps sealed batches in flight across the archive engine's cycles,
turns the Helm chart's admin authorization on by default, refuses a node TLS
private key other users can read, caps the connections each cluster listener
holds, wires group commit into the data node (off by default), and adds
metadata feature level 9. The full notes are in
[v0.11.0](../ROADMAP.md#v0110--batches-in-flight-across-cycles-a-cluster-plane-that-is-secure-by-default-and-group-commit-wired-into-the-data-node)
in the roadmap.

1. **Helm: name your operators first.** `meta.adminAuthorization.enabled` now
   defaults to `true`, and a render with no
   `meta.adminAuthorization.operatorCommonNames` stops with a message naming
   the three ways out; nothing is changed in the cluster. Find the Common Name
   of each certificate your `vtopctl` uses against the admin plane
   (`openssl x509 -noout -subject -in operator.pem`) and set
   `--set 'meta.adminAuthorization.operatorCommonNames={vtop-admin}'`. A lab
   can set `enabled: false`; a plaintext admin plane needs that opt-out.
2. **Make every `vtop-node` TLS private key owner-only** (`chmod 0400`, or a
   `0700` directory the node's user owns) before its node restarts onto this
   release. A node refuses a key other users can read, with no override
   ([Secrets](secrets.md#upgrading-to-the-release-with-this-check)).
3. **Before restarting a data node, check `vtop_broker_epoch_history_broken`.**
   A replica that broke under v0.10.4 has no mark on disk, so a restart forgets
   its break; repair it with `vtopctl node repair` instead of restarting it.
4. **Replace the binaries or the image** on every archive engine, metadata
   voter and data node, in any order. Below feature level 9 no wire frame,
   schema marker or on-disk format differs from v0.10.4's.
5. **Then raise the feature level to 9**, only if you want a cluster command's
   actor in the replicated log and a data node's lease release to be immediate
   under the admin policy: `vtopctl meta node-versions --level 9`, then
   `vtopctl meta enable-features --level 9`. The raise is permanent, and a
   v0.10.4 voter (which supports up to level 8) cannot read what level 9
   writes. Below level 9 a data node's release is refused by the policy and
   the lease lapses on its deadline (15 s by default), so a planned handover
   waits that long.
6. **Then use the new settings**, once every engine and node runs this
   release: the Kafka source's `ssl_certificate_location`, `ssl_key_location`
   and `ssl_key_password_env` (an older engine ignores them and connects without
   a client certificate), and the node keys `group_commit:`,
   `max_peer_connections`, `max_admin_connections`, `max_replica_connections`
   and `kafka.brokers` (an older node refuses to start with any of them).

- **Archive engine.** It reads while batches upload, so it can hold up to
  `batching.max_concurrent_batches` times `batching.max_bytes` of sealed
  batches, and uses more CPU for the same work in less time.
  `vtop_sealed_batches_in_flight` is new. On a long link to the object store
  set `net.ipv4.tcp_slow_start_after_idle=0` on the engine's host
  ([A long link to the object store](../deploy/single-node.md#a-long-link-to-the-object-store)).
- **`vtopctl tier copy` under `upload.object_lock`** now writes the copy under
  the lock and checks its retention; before, it refused the config. A
  `compliance` copy cannot be removed by anyone before its date.
- **Reports and images.** Every `vtopctl --json` report that is an object gains
  `"report_version": 1`. The container image's `vtopctl` opens a PostgreSQL
  state store; the release archives do not.
- **Connection caps.** The metadata peer, metadata admin and replica listeners
  hold at most 256, 1024 and 256 connections and close one over the cap at
  accept; `vtop_listener_connections_refused_total{plane,reason}` counts them.
- Downgrade is not supported, except for what the
  [compatibility matrix](../reference/compatibility.md#downgrade) records. A
  metadata directory raised to level 9 cannot be opened by v0.10.4.
- **Helm chart 0.7.0** (`appVersion` 0.11.0). A values-contract change: see
  step 1.

## Upgrading from v0.10.3 to v0.10.4

v0.10.4 fixes a lost range in the archive engine that v0.7.0 through v0.10.3
carry, pauses writes to a bucket whose versioning is suspended under a running
engine, and fixes four cluster-plane faults: a full disk during a segment
roll, a follower a new leader could not catch up, a candidate that kept
winning the lease without a quorum, and a broken epoch history nobody was
told about. The full notes are in
[v0.10.4](../ROADMAP.md#v0104--two-failed-windows-of-one-source-no-longer-lose-the-lower-one-and-a-bucket-that-stops-versioning-pauses-writes)
in the roadmap.

- **Nothing to do for the lost-range fix (#891).** In v0.7.0 through
  v0.10.3, when two or more windows of one file, spool file or Kafka
  partition failed in the same cycle, the source rewound to the highest
  failed start and the lower range was never archived. To check a ledger
  those releases wrote, run `vtopctl audit-coverage` (with `--check-storage`
  to also probe the objects) and look for a unit with an unarchived range
  below a later commit. A range the bug lost still has its FAILED or SUPERSEDED row (or a prune digest of one), so it is reported as a failed hole and the command fails, for Kafka partitions as for files; a Kafka gap that no failed row explains is reported without failing, so read the report as well as the exit status. See
  [Scrub and coverage audit](scrub-and-audit.md#coverage-audit).
- **With `upload.require_object_versioning` set, a bucket whose versioning
  is suspended is no longer written to until it is re-enabled (#883).** The
  engine logs the ERROR `bucket_versioning_paused`, sets
  `vtop_bucket_versioning_paused`, and replays the batches once versioning
  is Enabled. Load the new alert rules so `VtopBucketVersioningPaused` pages.
- **New metrics and alerts.** `vtop_bucket_versioning_paused`,
  `vtop_bucket_versioning_paused_batches_total`,
  `vtop_broker_follower_below_catch_up_horizon` and
  `vtop_broker_epoch_history_broken` are new. The Prometheus rules gain
  `VtopBucketVersioningPaused`, `VtopStateStoreWritesSlow` and
  `VtopEpochHistoryBroken`; the Grafana alerts gain "Follower below the
  catch-up horizon". Reload `observability/prometheus/vtop-rules.yaml`, or
  upgrade the chart if it renders the PrometheusRule.
- **Otherwise a drop-in replacement.** Replace the binaries in any order;
  v0.10.3 and v0.10.4 engines, voters and data nodes can run side by side.
  No config key or default, schema marker, wire format or cluster feature
  level changes, and no one-way step is added.
- **Helm chart 0.6.3** (`appVersion` 0.10.4). No template or values change;
  the chart's copy of the alert rules gains the three alerts above.
- Downgrade is not supported.

## Upgrading from v0.10.2 to v0.10.3

v0.10.3 fixes where a Kafka partition resumes, makes the `localfs` backend
fsync before a write is reported done, unsticks a source under a long
directory, binds a native client certificate to the principal it acts as,
and adds encryption at rest through S3 server-side encryption. The full notes are in
[v0.10.3](../ROADMAP.md#v0103--a-kafka-resume-that-survives-a-recreated-topic-durable-local-writes-and-client-certificates-bound-to-their-principal)
in the roadmap.

- **Reissue native client certificates whose CN is not the node's
  `principal_id`, before upgrading a data node.** A v0.10.3 data node admits
  a native TLS session only when the client certificate's Common Name is its
  `principal_id` (#854). Compare each client's
  `openssl x509 -noout -subject -in <cert>` with the node's `principal_id` and
  reissue the ones that differ with `CN=<principal_id>`. A v0.10.2 node does
  not read the CN, so reissue first, then upgrade. A refused client gets
  `Unauthorized`, and the node writes a `native.session` audit record naming
  the CN it saw. A plaintext native plane is unaffected.
- **On PostgreSQL, run v0.10.3's `vtopctl migrate` before its engines
  start.** It adds two partial indexes on the Kafka rows of `batches` that
  pruning uses; the schema marker stays 3. `CREATE INDEX` blocks writes to
  `batches` while it builds, so on a large ledger run it in a quiet window.
  Until it runs, pruning is correct and slower. SQLite creates the indexes on
  connect.
- **Kafka sources need nothing.** New ledger rows record the topic's id
  (`topic_id`) and a partition resumes from the highest committed end of the
  topic's current id; rows written before the upgrade keep the old rule until
  rows with the id exist (#855). An older engine ignores the field.
- **Sources under long paths.** A name over 200 bytes is now shortened, so a
  replay of a range archived under a name of 201 to about 241 bytes writes a
  new object instead of reusing the old one: a duplicate, never a loss (#851).
- **`localfs` writes are slower**, by an fsync of the file and its
  directories (#827).
- **Upgrade every engine before setting `upload.server_side_encryption`
  (#856).** A v0.10.2 engine ignores the key without a word and writes
  objects unencrypted, so keep a bucket default encryption as the floor.
  Objects written before the mode was set stay as they were, and `vtopctl
  scrub` reports each as a mismatch. With `sse-kms` the identity needs
  `kms:GenerateDataKey` and `kms:Decrypt`.
- **Otherwise a drop-in replacement.** Replace the binaries in any order;
  v0.10.2 and v0.10.3 engines, voters and data nodes can run side by side,
  apart from the native client certificates above. No schema marker, wire
  format or cluster feature level changes, and no one-way step is added. The
  one new key is `upload.server_side_encryption`.
- **Helm chart 0.6.2** (`appVersion` 0.10.3). No template or values change.
- Downgrade is not supported.

## Upgrading from v0.10.1 to v0.10.2

v0.10.2 fixes the size of the batches `vtopctl run` seals, recovers a replay
over an Object Lock that ended, and refuses key, ledger and audit files that
other local users can reach. The full notes are in
[v0.10.2](../ROADMAP.md#v0102--batches-stay-within-max_bytes-and-a-linux-archive-for-every-linux)
in the roadmap.

- **Run `vtopctl validate` with the v0.10.2 binary first.** The engine now
  refuses to start when a manifest key file, the state-store secret file, the
  SQLite ledger (or its `-wal` and `-shm`) or the `VTOP_AUDIT_LOG` file can be
  reached by other local users (#775). A ledger written 0644 by an earlier
  release is refused unless its directory is owned by the engine's user and
  closed (0700); a group- or world-writable file is always refused. `validate`
  lists every refused file and changes nothing. Fix them with `chmod`, or
  accept readable files with `security.allow_loose_secret_permissions: true`,
  which is announced and audited. See
  [Secret files and their permissions](secrets.md).
- **Otherwise a drop-in replacement.** Replace the binaries in any order;
  v0.10.1 and v0.10.2 engines, voters and data nodes can run side by side. No
  schema, on-disk or wire format, or cluster feature level changes, and no
  one-way step is added. A raise of the feature level to 8 or above now asks
  every metadata member first, so a cluster still at level 7 needs every voter
  on v0.10.2 before that raise.
- **Batches are smaller in `run`.** They now hold at most `batching.max_bytes`,
  where they held about twice that, so expect more objects for the same bytes.
- **The Linux archives** run on glibc 2.17 or later; the new `-linux-musl`
  archives run on any Linux. See [binaries](../install/binaries.md).
- **Helm chart 0.6.1** (`appVersion` 0.10.2). No template or values key
  changes; the shipped alert rules gain `VtopObjectLockFallback`.
- Downgrade is not supported.

## Upgrading from v0.10.0 to v0.10.1

v0.10.1 fixes the Linux archives: v0.10.0's did not start on a host with a
glibc older than 2.39, such as Debian 12, Ubuntu 22.04 or RHEL 8 and 9
(#808). The full notes are in
[v0.10.1](../ROADMAP.md#v0101--the-linux-archives-run-on-glibc-228) in the
roadmap.

- **A drop-in replacement.** Replace the binaries in any order; v0.10.0 and
  v0.10.1 engines, voters and data nodes can run side by side. No schema,
  on-disk or wire format, `config.yaml` key or cluster feature level changes.
- **The Linux archives need glibc 2.28 or later**: Debian 10, Ubuntu 20.04,
  and RHEL, AlmaLinux and Rocky Linux 8 and later. musl distributions such as
  Alpine are not supported; use the container image.
- **Helm chart 0.6.0** (`appVersion` 0.10.1). The new
  `metrics.prometheusRule` values are optional and off by default, and an
  existing values file renders as before. Turning `enabled` on needs the
  Prometheus Operator CRDs; without them, load
  `observability/prometheus/vtop-rules.yaml` with `rule_files`.

## Upgrading from v0.9.0 to v0.10.0

The full notes, including what an older engine does with each new key, are in
[v0.10.0](../ROADMAP.md#v0100--an-archive-that-can-be-audited-receipts-object-lock-key-rotation-an-audit-trail-and-replays-that-land-on-the-object-already-archived)
in the roadmap. The steps that need an operator, in order:

1. **PostgreSQL: run `vtopctl migrate` from v0.10.0, with the migration
   identity, before starting v0.10.0 engines.** It adds the nullable
   `sealed_at` column and the append-only `audit_events` table, and v0.10.0
   engines refuse a schema without them. Grant the runtime and maintenance
   roles `SELECT, INSERT` on `audit_events` and `USAGE` on
   `audit_events_seq_seq` ([PostgreSQL deployment](../POSTGRES_DEPLOYMENT.md)).
   The schema marker stays 3, so v0.9.0 engines keep running on the migrated
   schema. SQLite adds both on open.
2. **Upgrade every engine, metadata voter and data node.** On Kubernetes,
   install chart `0.5.3` from the v0.10.0 checkout (`appVersion` `0.10.0`).
3. **Raise the cluster feature level to 8 only if you want
   `vtopctl node promote-unproven`**: `vtopctl meta node-versions --level 8`,
   then `vtopctl meta enable-features --level 8`. The raise is permanent.
4. **Then turn on the new settings**, once every engine, `vtopctl` and
   auditor that reads the archive runs v0.10.0. Older engines silently ignore
   them, and two are not harmless in a mixed fleet: under `manifest.mac` an
   older engine keeps signing with its one key and cannot verify manifests
   signed with a new one, and under `upload.object_lock` it writes unlocked
   objects. `manifest.write_version: "0.4"` writes manifests that readers
   before v0.8.0 report as tampered.

Check before upgrading: `vtopctl prune-ledger`, `replay` and `migrate` now
fail when their audit record cannot be written, and need the manifest MAC key
in their environment when one is configured; `upload.require_object_versioning`
against a store without versioning refuses to start; and a delete under
`delete_without_open_check` with no lease is counted `deleted_unchecked`, not
`deleted`.

## Upgrading from v0.8.0 to v0.9.0

v0.9.0 is a bug-fix release. The full notes are in
[v0.9.0](../ROADMAP.md#v090--bug-fixes-deletes-that-wait-for-proof-kafka-partitions-that-are-never-left-unread-portable-builds)
in the roadmap; the steps that need an operator are:

- **PostgreSQL: run `vtopctl migrate` before starting v0.9.0 engines.** It
  adds the nullable `verification_class` column, and the schema marker stays
  3, so v0.8.0 engines keep working against the migrated database. Restart
  any v0.8.0 engine that was running during the migration: until its
  connections are replaced, its whole-row reads fail. SQLite migrates on
  connect.
- **`delete_after_commit` keeps files it cannot prove are closed**: on
  macOS, NFS or FUSE, another user's file, and in any container without
  `CAP_LEASE`. Watch disk use and `vtop_file_delete_decisions_total{outcome}`.
- **Kerberos (SASL GSSAPI) for the Kafka source is no longer in the default
  build.** If you use `sasl_mechanism: GSSAPI`, build with
  `--features kafka-gssapi`.
- **New key older engines silently ignore:**
  `sources.file.delete_without_open_check`. Upgrade every engine before
  relying on the new delete rules.

On Kubernetes, install chart `0.5.2` from the v0.9.0 checkout (its
`appVersion`, and so its default image tag, is `0.9.0`).

## Upgrading from v0.7.0 to v0.8.0

### 1. Upgrade every node first

Replace the binaries or image on every archive engine, every metadata voter
and every data node before you raise the feature level or set any of the new
keys below.

On Kubernetes, install chart `0.5.1` from the v0.8.0 checkout (its
`appVersion`, and so its default image tag, is `0.8.0`) with `helm upgrade`,
or set `image.tag: "0.8.0"`.

### 2. Then raise the cluster feature level

Cluster plane only. v0.7.0 supports feature levels up to 5 and v0.8.0 up to 7.
Raising is permanent: there is no lowering. v0.11.0 and later refuse to open,
or to install a snapshot or apply a raise from, a metadata directory recorded
at a level above the one they support. The message names both levels and says
to run a newer binary; the directory is left as it was. Releases before
v0.11.0 do not have this check: v0.8.0 and v0.9.0, for example, open a level-8
directory and report level 3 or 8 depending on where the raise sits, so keep
every voter on a release that supports the level before you raise it.

```bash
# What every registered node reports it runs.
vtopctl meta node-versions --config admin.yaml

# Raise once every metadata voter runs v0.8.0.
vtopctl meta enable-features --level 7 --config admin.yaml
```

Level 7 lets data nodes report their release to metadata. From then on, a
raise above 7 (in a later release) is refused while any counted node has not
reported that level, and the refusal names each one. `node-versions --level N`
answers whether level `N` could be raised now, and exits non-zero if not.

From level 8 the metadata voters are checked too (#706): the leader asks every
voter and learner what it runs before it proposes the raise, and refuses,
proposing nothing, unless each answers that it supports the level. A voter
still on v0.9.0 or older cannot answer, so the refusal names it ("closed
without answering"); upgrade it and raise again. The leader must run this
release for the check to happen: raise once `vtopctl meta status` shows an
upgraded node leads.

### 3. Then use the new settings

Engines older than v0.8.0 silently ignore these keys, so set them only once
every engine runs v0.8.0:

| Key | What it does |
|---|---|
| `upload.request_min_bytes_per_second` | A request body's deadline grows with its size. |
| `upload.max_egress_bytes_per_second` | An operator ceiling on upload bandwidth. |
| A stream's `checksum` block | Per-stream checksum algorithm. Keep the global `checksum.algorithm` strong during the rollout. |

### One-way changes

After these, a v0.7.0 binary can no longer use the data:

- Once the engine writes a `SUPERSEDED` row, the state store carries schema
  marker 3 and v0.7.0 refuses it.
- An older binary refuses to start while a kept dispute is pending.
- Fencing journal and sidecar rewrites stage through `<file>.rewrite` and a
  rename.

### Manifests

v0.8.0 still writes manifest version `0.3`. Its readers check the version
first, read `0.1` to `0.4`, and report a newer manifest as newer rather than
as tampered: recovery leaves such a batch VERIFIED and pending, and
`vtopctl scrub` reports `unsupported_version`. Readers before v0.8.0 cannot
read `0.4`, so upgrade every engine and auditor before a later release turns
`0.4` on. The details are under
[v0.10.0 in detail](../ROADMAP.md#v0100-in-detail) in the roadmap.

Verification receipts (`manifest.verification_receipts`, from v0.10.0)
need no reader upgrade: a reader that does not know them never fetches them. Older engines ignore the key and write
none, so turn on `vtopctl scrub --require-receipts` only once every engine
writes them; see [Verification receipts](verification-receipts.md).

## Secret-file permissions (v0.10.2, #733)

Engines of v0.10.2 and later refuse to start when a manifest key file, the
`engine.state_store` secret file, the SQLite ledger (or its `-wal` or `-shm`
file) or the `VTOP_AUDIT_LOG` file can be read or written by other local
users, where earlier releases started. A `0644` ledger inside a `0700` data
directory still starts, and so does a Kubernetes Secret mounted with
`defaultMode: 0400` and `fsGroup`. Before upgrading, run the new release's
`vtopctl validate` against each engine's config on its host or in its pod:
it names every file that would be refused, with the `chmod` that fixes it.
The rule, the mounting recipes and the
`security.allow_loose_secret_permissions` override are in
[Secret files and their permissions](secrets.md#upgrading-to-the-release-with-this-check).

## Before any upgrade

- Read the release's section in the [roadmap](../ROADMAP.md), including its
  known limitations.
- [Verify the release](../RELEASE_VERIFICATION.md) you are installing.
- Back up the state ledger (the SQLite file and its `.ledger-digests`
  directory, or the PostgreSQL database), since you cannot downgrade.
- On PostgreSQL, the engine never changes the schema itself. Run
  `vtopctl migrate` from the new release with the migration identity before
  the new engines start, as in the
  [deployment sequence](../POSTGRES_DEPLOYMENT.md#deployment-sequence).
- Run the new release's `vtopctl validate` against each engine's config,
  where the engine runs: it reports what the new engine would refuse at
  startup (since v0.10.2, the
  [secret-file permissions](secrets.md) too).
- Stop each engine with SIGTERM and let its shutdown flush finish.

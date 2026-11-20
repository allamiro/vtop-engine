# Auditing

An audit record proves who changed engine or cluster state, when, from
where, and what changed. Logs explain what the engine decided; metrics show
trends. The audit trail is a separate record (#694), built the same way by
`vtopctl` and by every `vtop-node`.

## What is recorded

Every operator command in the table below writes one record per outcome.
The destructive ledger commands write an `attempt` record **before** they
act, and are refused when it cannot be written.

| Action | Written by | When |
|---|---|---|
| `ledger.prune` | `vtopctl prune-ledger` | `attempt` before a row is deleted; then `success` (rows deleted, digest file) or `failure`. |
| `ledger.replay` | `vtopctl replay` | `attempt` before recovery moves a row; then `success` (how many rows were committed, sent back to be re-read, superseded or left pending) or `failure`. The target is the ledger: recovery covers every incomplete batch, and `params.batch_id` is only the batch `--batch-id` asked about. The work-directory and abandoned-multipart cleanup the engine runs when it opens happens before the attempt and is not covered. |
| `ledger.migrate` | `vtopctl migrate` | `attempt` to the log and the audit file (the table does not exist yet); after the migration, the attempt and the outcome rows in the new table. |
| `scrub.cursor` | `vtopctl scrub --cursor-file` | After the run: whether the cursor moved, and whether the scrub passed. A scrub without a cursor changes nothing and writes no record. |
| `cluster.init`, `cluster.membership` | `vtopctl meta init`, `add-learner`, `change-membership`; the admin endpoint; every metadata replica | The client's record, the endpoint's record with the caller's certificate identity, and each replica's record of the membership entry it applied. |
| `cluster.enable_features` | `vtopctl meta enable-features`; the admin endpoint; every replica | As above, with the level it was raised from (`before.level`, when the cluster answered) and to. |
| `cluster.create_topic` | `vtopctl meta create-topic`; the admin endpoint; every replica | As above. |
| `cluster.propose` | any other audited `vtopctl meta` or `vtopctl tier` command; the admin endpoint; every replica | `params.command` names the command (`SetNodeState`, `ProposeRebalance`, `CommitTierEvidence`, ...). |
| `promotion.attest` | `vtopctl node promote-unproven` (#636); the admin endpoint; every replica | Including a refusal before anything was sent (no `--i-accept-data-loss`). |
| `node.repair` | `vtopctl node repair` (`params.side: client`); the leader serving the transfer (`params.side: serving`) | The client's outcome; the leader's grant or refusal of the listing that starts a transfer, its refusal of any chunk, and the outcome of a `seal_tail`. |
| `node.reconfigure_range` | `vtopctl node reconfigure-range` | The thresholds written, or the failure. |
| `audit.gap` | every metadata replica (#706) | A range of applied records this replica owes and cannot re-derive: `params.from_index`, `through_index` and `reason` (`snapshot_install`, `log_purged`). Outcome `failure`. See "Applied records: at least once" below. |
| `security.override` | Any `vtopctl` command that reads a key, state-store, ledger or audit file other local users can read, accepted only because `security.allow_loose_secret_permissions` is true (#733) | Once per file per process, outcome `success`. The target is the file; `params` are `file_kind`, `mode`, `exposed_to` and `setting`. See [Secret files](secrets.md#the-override). |

Not audited: the archive path itself (`vtopctl run` and `process-once`
archive and commit batches; that is the batch lifecycle, in the ledger and
the logs), and the object-store work of `vtopctl tier copy`, `rehydrate` and
`cleanup-abandoned` (the tier evidence they commit to the metadata plane is
audited, as `cluster.propose` `CommitTierEvidence`).

On the cluster plane, every **refusal** is recorded, whatever the command:
a node certificate asking for another node's lease, a non-operator asking
for a membership change. Successful commands are recorded for the operator
command kinds only. The commands the nodes propose themselves all the time
(lease acquire and renew, promotion and version reports, a leaseholder's
split) and the consumer-group commands the Kafka gateway proposes for its
clients are not: they are recorded as range transitions already, and they
run at client rate. The list is an exhaustive match in
`crates/vtop-meta/src/audit.rs`; a new command does not compile until it is
put on one side.

## The record

One JSON object per record, with a stable `audit_version`. Fields are only
ever added to version 1, and a reader ignores what it does not know.

```json
{
  "audit_version": 1,
  "audit_id": "6f1c0a52-2a43-4d87-9b8e-1b2f5d0c7a02",
  "time": "2026-11-14T10:00:01.000Z",
  "actor": {"kind": "os", "uid": 1000, "name": "ops-alice", "name_source": "passwd",
            "host": "edge-1", "source": "os"},
  "action": "ledger.prune",
  "stage": "local",
  "target": {"kind": "ledger"},
  "params": {"digest_dir": "/var/lib/vtop/ledger-digests", "limit": "500", "older_than_days": "30"},
  "outcome": "success",
  "after": {"deleted": 12, "digest": "/var/lib/vtop/ledger-digests/prune-20261114T100000Z.jsonl"},
  "correlation": {"request_id": "0d6e7a2e-...", "attempt_id": "6f1c0a52-...-7a01",
                  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "span_id": "00f067aa0ba902b7"},
  "engine": "edge-1"
}
```

| Field | Meaning |
|---|---|
| `outcome` | `attempt`, `success`, `failure` or `refused`. `reason` says why for the last two. |
| `stage` | `local` (a command that acted itself), `client` (a command that asked a cluster), `edge` (the admin endpoint, or a leader serving a transfer) or `applied` (a metadata replica applying the committed entry). |
| `params` | An allowlist per action, never a dump of the command: a connection string, a key, a key variable's value or a URL is never on it. Every value and reason is redacted again (URLs cut to their host, long hex runs, access key ids, `password=` and `token=` values) and bounded to 1 KiB. |
| `before`, `after` | What changed, where it is cheap to say. |
| `correlation.request_id` | Joins the client, edge and applied records of one cluster command (its envelope's request id), and the attempt and outcome of a ledger command. Bootstrap and membership requests carry none on the wire: join their records by time and, for the applied ones, `log_index`. |
| `correlation.attempt_id` | On an outcome, the `audit_id` of its attempt. |
| `correlation.trace_id`, `span_id` | The OpenTelemetry trace the command ran in, when export is on (#683): the same ids its JSON log lines carry. |
| `correlation.log_index` | The metadata log index a cluster command committed at. Dedupe applied records on the engine and this index: a replica delivers each of its applied records at least once (#706), so a crash can repeat one, marked `replayed`. A follower that redirects a request to the leader writes no edge record; the leader does. |
| `correlation.log_term` | On an applied record, the entry's term (#706). With the index it names the entry, the same on every replica. |
| `time` | When the record was written. On an applied record of a command, the entry's own `issued_at_ms` as its proposer stamped it, never the replica's clock, so every replica writes the same time (#706). That stamp is the proposer's claim, not checked against any clock. An entry stamped 0 carries no time: `vtopctl meta` sends 0 unless given `--issued-at-ms`, except for `enable-features`, which stamps the current time. Its applied records, membership records and `audit.gap` records are timed by the replica that writes them. |
| `replayed` | `true` on a record delivered again after a crash, a refusing sink or a restart (#706); it may duplicate one this replica already wrote. Absent means false. |
| `engine` | The engine's `engine.name`, `meta-<id>` for a metadata node, or a data node's UUID. |

### What the actor proves

| `actor.source` | What it is |
|---|---|
| `os` | A local command: the numeric `uid` the kernel reports, the user name that uid has in `/etc/passwd` (`name_source: passwd`, else `$USER`, labelled `env`), the host name, and `$SUDO_USER` when set. As strong as the host's logins, and **not authentication**: anyone who can run `vtopctl` as a user is that user. |
| `certificate` | The verified client certificate's Common Name on the admin endpoint (`kind: certificate` for an operator, `node` for a node), or the peer's certificate on the replica plane. |
| `claimed` | A peer on a plaintext replica plane, which only declared its UUID. |
| `raft` | The replica that applied a cluster command. The requester is in the `edge` record with the same request id. |
| `none` | A write on a plaintext admin endpoint (`kind: anonymous`). The endpoint warns once per connection that its audit records name no actor. |

If an auditor needs more than a host login for a cluster change, run it
through the admin endpoint over mTLS with one certificate per operator
(`meta.adminAuthorization`): the edge record then names the certificate.
The actor is recorded at the edge only; making it durable in the replicated
log is a format change of its own, not part of this release.

### Applied records: at least once

Each metadata replica keeps an audit frontier, `meta.audited`, in its data
directory: every applied record it owes at or below it was accepted by its
sinks (#706). The frontier is saved only after delivery, so:

* **A crash** between applying an entry and writing its record no longer
  loses the record. The node delivers it at its next start, before Raft
  runs, marked `replayed`.
* **A sink that refuses** (an audit file on a full disk) does not stop the
  node or the apply. The frontier stays behind and the records are
  delivered again, in order and marked `replayed`, at the next apply, by a
  retry every 10 seconds, and at restart.
  `vtop_audit_delivery_lag_entries` stays above 0 while that lasts, and a
  rate-limited warning names the index, the error and what to fix.
* **What the log no longer holds** cannot be re-derived on that replica. A
  replica that catches up by installing a snapshot, or whose log was purged
  past a frontier that fell behind while its sink was failing, writes one
  `audit.gap` record naming the index range and why, and a warning. The other
  replicas wrote their own records for those entries: copy their audit files
  and dedupe on `engine` and `correlation.log_index`.

While the sink refuses, an apply does not try again within the 10-second
retry interval, and a backlog is delivered at most 256 entries at a time, so
neither a failing sink nor its recovery stalls the metadata tier. A write to
the audit file that fails part-way is cut off again, or the next record
starts on a new line, so a re-delivered record is never glued to a fragment.

`meta.audited` also keeps the highest index a snapshot install brought the
node past; a replay never reads the log at or below it, because the log there
may be a branch the snapshot replaced.

If `meta.audited` is damaged, the node refuses to start and names the file.
Removing it re-pins the frontier at the applied index on the next start:
the records it still owed are not re-delivered by that node, and the other
replicas' files hold theirs.

With no `VTOP_AUDIT_LOG`, the log stream is the only sink, and the guarantee
covers a process crash only: what the log pipeline does after it is outside
the node. A data directory written by a release before #706 has no
`meta.audited`: the first start pins it at the applied index and replays
nothing from before. Older releases ignore the file.

## Where records go

| Sink | When | Notes |
|---|---|---|
| The log stream, target `vtop::audit` | Always | Written whatever `RUST_LOG` or `--log-level` say, never sampled or rate-limited. INFO for `attempt` and `success`, WARN for `failure` and `refused`. The message is `audit: <action> <outcome>` (closed sets, so a pipeline may index it); `summary` says who did it and on what, and `audit_id`, `action`, `outcome` and `actor` sit beside the full record in `record`. A sink's own error line on this target carries `audit_sink_error=true` and no `record`. |
| An append-only JSONL file | `VTOP_AUDIT_LOG=/path` | One record per line, synced before the command goes on. Created with mode `0600` (an existing file keeps its mode). A set but unwritable path stops the command (or the node) at startup, naming the path. Several processes may append to one file (a node and `vtopctl` runs): each append holds an exclusive lock on the file, so a failed append is cut back without touching another process's record, and a record never starts inside another writer's fragment (a writer from v0.10.1 or earlier takes no lock). `vtopctl` also refuses an existing file group or other can write, or other users can read outside a closed directory; group read is accepted, for a log shipper (#733, [Secret files](secrets.md)). Rotate it by moving the file away while nothing writes to it (between `vtopctl` runs; for a node, at a restart); `logrotate`'s `copytruncate` can lose a record appended between the copy and the truncate. |
| The state store's `audit_events` table | The ledger commands | Append only, hash-chained (below). `prune-ledger` never touches it; there is no retention for it yet. |

`vtopctl meta`, `node` and `tier` now install a log subscriber: WARN and
above unless `--log-level` says otherwise, and audit records always.

### When a sink cannot be written

* **Before a destructive ledger command** (`prune-ledger`, `replay`,
  `migrate`): the command is refused. Nothing is changed, a `refused` record
  goes to whichever sinks still work, and the command exits non-zero:
  `could not record the audit attempt (...); prune-ledger was not run and
  the ledger was not changed. Fix the audit sink that failed ...`. There is
  no switch to run it unrecorded.
* **After it acted**: the command exits non-zero saying it ran and which
  record could not be written; the attempt record and the log stream hold
  what was asked.
* **Anywhere else** (a cluster client, a node, a scrub): the record is in the
  log stream, the file failure is logged at ERROR and counted, and the
  command or node goes on.

Every failure is counted on `vtop_audit_write_failures_total{sink}`.

## Reading it

```bash
# The ledger's table (the ledger commands' records):
vtopctl audit list --config /etc/vtop/config.yaml
# An audit file a node or an engine wrote:
vtopctl audit list --file /var/log/vtop/audit.jsonl --action cluster.membership
# Filters: --since/--until (RFC 3339), --actor (name, uid or sudo user),
# --action, --target (id or kind), --outcome; --limit (the newest N, default 100).
vtopctl --json audit list --config /etc/vtop/config.yaml --outcome refused
```

`--json` prints each record exactly as stored, one per line. A record from
a newer release (a newer `audit_version`, or an action this build does not
know) is printed as stored with a warning on stderr, never dropped.

In Loki, with JSON logs:

```logql
{service=~"vtop.*"} | json | target="vtop::audit" | fields_record != "" | line_format "{{.fields_record}}"
```

A cluster command is read from each metadata node's audit file (or log
stream): the edge record of the node that served the request and the applied
records of every replica, joined by `correlation.request_id`.

## Tamper evidence

Each `audit_events` row stores the previous row's link and its own:
`link = BLAKE3(previous link, newline, the record as stored)`. With a
manifest MAC key configured (`manifest_mac_key_env` or the
[`manifest.mac` keyring](manifest-key-rotation.md)), the link is keyed with a
key derived from the signing key for this purpose alone, and the row's
`chain` names the key id (`blake3-keyed:<id>`), so a writer who can edit the
table cannot recompute the chain without the key, and a rotated key keeps
verifying the rows it wrote while it stays configured.

```bash
vtopctl audit verify --config /etc/vtop/config.yaml
# audit chain: 42 row(s), 42 keyed; last row 42 link 9f86...
# verdict: PASSED (keep the row count and last link elsewhere: ...)
```

It exits non-zero and names each row where an edited, deleted, inserted or
reordered row breaks the chain, and an unkeyed row after a keyed one; with a
signing key configured, a table that holds rows but no keyed row at all (a
table rewritten without the key) fails too. A keyed row whose key this
configuration does not hold (a retired key, or none configured) is counted
under `NOT CHECKED`: its links to the rows around it are still checked, and
it does not fail the verdict, so retiring a key does not fail every later
verify.

`prune-ledger`, `replay` and `migrate` key their rows, so with a key
configured they need it in their environment, as the engine does, and stop
naming the variable when it is missing. `vtopctl audit list` does not need
the key. A chain cannot show rows removed
from its **end**: keep the printed count and last link somewhere the
database's writers cannot reach, and compare.

Append only is also a database property:

* **PostgreSQL**: a trigger refuses `UPDATE`, `DELETE` and `TRUNCATE` on
  `audit_events` for every role but the table's owner (the migration
  identity), so a maintenance role granted `DELETE` for `prune-ledger` still
  cannot remove a row.
* **SQLite**: triggers refuse `UPDATE` and `DELETE`. Anyone who can write the
  file can drop them; the chain is what shows that.

## PostgreSQL

`vtopctl migrate` creates the table (and its sequence and trigger). Engines
of this release refuse a schema without it at startup, naming
`audit_events` and `vtopctl migrate`. Grant the runtime role:

```sql
GRANT SELECT, INSERT ON TABLE audit_events TO vtop_runtime;
GRANT USAGE ON SEQUENCE audit_events_seq_seq TO vtop_runtime;
```

and the same to the maintenance role that runs `prune-ledger`. See
[PostgreSQL deployment](../POSTGRES_DEPLOYMENT.md).

## Metrics

| Metric | Labels |
|---|---|
| `vtop_audit_events_total` | `action` (the actions above), `outcome` |
| `vtop_audit_write_failures_total` | `sink`: `log`, `file`, `ledger` |
| `vtop_audit_replayed_total` (metadata nodes) | `reason`: `restart`, `retry`, `snapshot` |
| `vtop_audit_gaps_total` (metadata nodes) | `reason`: `snapshot_install`, `log_purged`, `tail_evicted` |
| `vtop_audit_delivery_lag_entries` (metadata nodes) | none: applied entries whose records are not delivered yet |

On the engine's `/metrics` (`vtopctl run` with `VTOP_METRICS_ADDR`) and every
node's. A one-shot `vtopctl` command has no registry: its records are in the
log, the file and the table. Alert on any increase of
`vtop_audit_write_failures_total`, and look at a rising
`vtop_audit_events_total{outcome="refused"}`. On metadata nodes, alert when
`vtop_audit_delivery_lag_entries` stays above 0 for more than a minute (a
failing sink), and on any increase of `vtop_audit_gaps_total{reason="log_purged"}`
(copy the other replicas' audit files for the range the `audit.gap` record
names). `reason="snapshot_install"` is routine: a node added or replaced
catches up by snapshot, and its gap names entries it never applied. It is
worth a look only when no node was added or replaced.

## Compatibility

* The record, the file and the table are new; no wire format, Raft entry,
  snapshot or manifest changes, and the cluster upgrade procedure is
  unchanged.
* `meta.audited` (#706) is a new file in a metadata data directory; older
  releases ignore it. The record gains `replayed` and `correlation.log_term`
  and the action `audit.gap`, all additions to `audit_version` 1: an older
  `vtopctl audit list` prints an `audit.gap` record as stored, with a warning.
* An older engine or node writes no audit records: in a mixed fleet the trail
  is only as complete as its least-upgraded node.
* An older release opening an upgraded SQLite file or PostgreSQL database
  ignores the table. Downgrade stays unsupported; the table is left in place.

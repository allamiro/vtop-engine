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
| `correlation.log_index` | The metadata log index a cluster command committed at. Dedupe applied records on it. Each replica writes its applied record once, after the entry is applied: a replica that crashes between applying an entry and writing the record, or that catches up by installing a snapshot, writes none for those entries. A follower that redirects a request to the leader writes no edge record; the leader does. |
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

## Where records go

| Sink | When | Notes |
|---|---|---|
| The log stream, target `vtop::audit` | Always | Written whatever `RUST_LOG` or `--log-level` say, never sampled or rate-limited. INFO for `attempt` and `success`, WARN for `failure` and `refused`. The message is `audit: <action> <outcome>` (closed sets, so a pipeline may index it); `summary` says who did it and on what, and `audit_id`, `action`, `outcome` and `actor` sit beside the full record in `record`. A sink's own error line on this target carries `audit_sink_error=true` and no `record`. |
| An append-only JSONL file | `VTOP_AUDIT_LOG=/path` | One record per line, synced before the command goes on. Created with mode `0600` (an existing file keeps its mode). A set but unwritable path stops the command (or the node) at startup, naming the path. Rotate it by moving the file away while nothing writes to it (between `vtopctl` runs; for a node, at a restart); `logrotate`'s `copytruncate` can lose a record appended between the copy and the truncate. |
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

On the engine's `/metrics` (`vtopctl run` with `VTOP_METRICS_ADDR`) and every
node's. A one-shot `vtopctl` command has no registry: its records are in the
log, the file and the table. Alert on any increase of
`vtop_audit_write_failures_total`, and look at a rising
`vtop_audit_events_total{outcome="refused"}`.

## Compatibility

* The record, the file and the table are new; no wire format, Raft entry,
  snapshot or manifest changes, and the cluster upgrade procedure is
  unchanged.
* An older engine or node writes no audit records: in a mixed fleet the trail
  is only as complete as its least-upgraded node.
* An older release opening an upgraded SQLite file or PostgreSQL database
  ignores the table. Downgrade stays unsupported; the table is left in place.

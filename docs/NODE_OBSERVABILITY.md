# Node operational surface

Every long-running VTOP process — the archive engine (`vtopctl run`) and the
cluster nodes (`vtop-node meta`, `vtop-node data`) — exposes the same three HTTP
endpoints and the same log encoding. This document is the contract: what is
served, what each metric means, and what `/readyz` is allowed to claim.

Tracked by [#224](https://github.com/allamiro/vtop-engine/issues/224); the
nodes it applies to come from [#215](https://github.com/allamiro/vtop-engine/issues/215).

## Endpoints

| Path | Meaning |
|---|---|
| `GET /metrics` | Prometheus text format (`version=0.0.4`), `vtop_`-prefixed. |
| `GET /healthz` | Process liveness. Always `200` while the accept loop turns. |
| `GET /readyz` | Readiness level. `200 ready`, or `503 not ready: <reason>`. |

Only `GET` is answered; every other method gets `405` with an `Allow: GET`
header. The endpoint is unauthenticated, so it should not respond to verbs it
does not implement as though it did.

`/healthz` deliberately never consults the node's opinion of itself. A wedged
node must stay up to be inspected, so liveness answers `200` and the difference
from `/readyz` is the diagnostic.

The endpoint is **unauthenticated** (#78). Bind it to loopback or a management
interface, never a public one. It enforces a 16-connection cap and a
10-second per-connection deadline so a client that can reach the port cannot
spawn unbounded work.

### Enabling it

The engine is opt-in through the environment, because a single-binary lab
should not open a port nobody asked for:

```bash
VTOP_METRICS_ADDR=127.0.0.1:9090 vtopctl run --config config.yaml
```

Cluster nodes name the address in their config, and a configured address that
cannot bind is a **fatal** startup error — a node that silently came up
unscrapeable would pass its own health gate while being invisible to the one
thing meant to watch it:

```yaml
observability:
  listen: "127.0.0.1:9500"
```

Omitting the `observability` block keeps a node exactly as it was before #224,
so every existing config file stays valid.

## Readiness semantics per role

Readiness is a **level with a reason**, not a one-shot startup marker. The
reason is served in the body so a failing health gate names what to look at.

| Role | Ready when | Notably NOT gated on |
|---|---|---|
| `meta` | Raft store opened; peer and admin listeners bound. | Having a leader. A fresh cluster has no leader until the admin `init` RPC lands, and that RPC arrives over the very endpoint being gated — requiring leadership would deadlock bringup. Leadership is published as `vtop_meta_raft_state{state="leader"}` instead, where it is alertable without being a startup precondition. |
| `data` leader / standalone | Segment opened, native listener bound, **and this process still holds the live lease** — the metadata epoch equals the epoch it was granted. | Follower connectivity. A scenario may deliberately start a leader against a dead follower. |
| `data` follower | Segment opened and replica listener bound. | — |

The lease condition is the one that earns the split: a leaseholder that
metadata has fenced keeps its process healthy and its listener bound, but must
stop advertising itself as ready, because every write sent to it from that
moment on is one it is obliged to refuse.

Note that the test is ownership, not existence. After a lease *steal* the
metadata lease is very much alive — for the new holder. Reporting "a lease is
active" there would tell an operator this node can take writes at the exact
moment it began rejecting them, so both `/readyz` and `broker_lease_active`
require the metadata epoch to equal the epoch this broker was granted.

**Scope caveat:** no production path publishes committed metadata grants into
the broker's lease view yet — the Raft applied-state watcher that drives it is
follow-up work. Until it lands, a data leader reports ready for its configured
epoch and the fenced branch fires only under test. The predicate is wired now so
readiness becomes correct the moment the watcher does.

## Metric catalogue

All names carry the `vtop_` prefix. Labels are closed sets: `role`, `state`,
`reason`, and `scope` are enums; `topic`/`range` are bounded by the ranges a
process hosts and `peer`/`follower` by cluster membership. Offsets, request ids,
and paths are never labels — they belong in logs.

Metrics are collected at scrape time from live process state. There is no
sampling task, so a panel cannot show a value the node stopped believing
minutes ago. The one consequence worth knowing: offsets and the fencing view are
read through non-blocking accessors, so if the append path holds its state lock
across an fsync the gauge simply does not advance that scrape. **A frozen offset
gauge is the honest signal that writes have stalled**, not a broken exporter.
The alternative — blocking — would park a runtime worker behind the stalling
disk and take the endpoint down under exactly the failure it exists to diagnose.

Counters are exported as the broker's own running total rather than mirrored
into a Prometheus counter by delta. Mirroring is racy: concurrent scrapes can
each add the same delta and leave the export permanently above its source.

### Every node

| Metric | Type | Meaning |
|---|---|---|
| `vtop_node_info{role,node_id}` | gauge | Always 1; identifies the process. |

### Metadata nodes

| Metric | Type | Meaning |
|---|---|---|
| `vtop_meta_raft_running` | gauge | 0 once a fatal error stopped the Raft core, even though the process still answers `/healthz`. |
| `vtop_meta_raft_term` | gauge | Current Raft term. |
| `vtop_meta_raft_state{state}` | gauge | One-hot across `learner\|follower\|candidate\|leader\|shutdown`, so `sum by (state)` counts leaders across a cluster directly. |
| `vtop_meta_raft_leader_id` | gauge | Believed leader; `-1` when none is known. |
| `vtop_meta_raft_last_log_index` | gauge | Last local log index, in the 1-based VTOP index space `vtopctl meta status` prints. |
| `vtop_meta_raft_last_applied_index` | gauge | Last index applied to the metadata state machine. |
| `vtop_meta_raft_snapshot_index` | gauge | Last index in the newest local snapshot. |
| `vtop_meta_raft_purged_index` | gauge | Last purged index, inclusive. |
| `vtop_meta_raft_voters` / `_learners` | gauge | Committed membership sizes — the numbers a 3→5 growth scenario asserts against. |
| `vtop_meta_raft_millis_since_quorum_ack` | gauge | Milliseconds since a quorum acknowledged this leader; `-1` when not leading. **A climbing value on a self-declared leader is the signature of a partition** — it is what separates "isolated" from "slow". |
| `vtop_meta_raft_peer_matched_index{peer}` | gauge | Highest index each peer acknowledged, or `-1` for a peer that has acknowledged nothing. Leader only. |
| `vtop_meta_raft_peer_lag_entries{peer}` | gauge | Entries each peer trails the leader by; `-1` when the peer has never acknowledged, since its lag is unknown rather than total. |

Absent values report `-1`, never `0`: a Raft node with no log has no last
index, and `0` is a real value in the neighbouring series. The same rule covers
a freshly added learner that has replied to nothing — reporting `0` there would
render "no contact at all" as real replication progress.

Per-peer series are cleared when a node stops leading, so a demoted node does
not keep publishing the follower lag it saw at the instant of the failover.

### Data-plane nodes

| Metric | Type | Meaning |
|---|---|---|
| `vtop_broker_local_committed_offset{topic,range}` | gauge | This replica's durable commit boundary. |
| `vtop_broker_next_offset{topic,range}` | gauge | Next offset to assign, including records not yet durable. |
| `vtop_broker_cluster_committed_offset{topic,range}` | gauge | Quorum-committed HWM; fetch never exposes above it. |
| `vtop_broker_held_fencing_epoch{topic,range}` | gauge | Epoch this process was granted as leaseholder. |
| `vtop_broker_meta_fencing_epoch{topic,range}` | gauge | Latest metadata-committed epoch observed. |
| `vtop_broker_lease_active{topic,range}` | gauge | 1 while *this* broker holds the live lease (metadata epoch equals its granted epoch), 0 once fenced or stolen. |
| `vtop_broker_follower_durable_offset{follower}` | gauge | Per-follower acknowledged offset (leader only). |
| `vtop_broker_follower_lag_records{follower}` | gauge | Records each follower trails the leader by. |
| `vtop_broker_follower_connected{follower}` | gauge | 1 while a live replication stream exists. |
| `vtop_broker_follower_online{topic,range}` | gauge | Follower's own view of whether it accepts appends. |
| `vtop_broker_group_commits_total` | counter | Commit groups sealed and fsynced. |
| `vtop_broker_group_commit_requests_total` | counter | Produce requests folded into groups. |
| `vtop_broker_group_commit_records_total` / `_bytes_total` | counter | Volume folded into groups. |
| `vtop_broker_group_commit_sync_nanoseconds_total` | counter | Divide by commits for mean fsync cost. |
| `vtop_broker_group_commit_queue_wait_nanoseconds_total` | counter | Time requests waited to join a group. |
| `vtop_broker_group_commit_last_batch_records` / `_bytes` | gauge | Size of the most recent group. |
| `vtop_broker_memory_used_bytes{scope}` | gauge | Bytes charged to each budget ledger (`process`, `shard`, `fetch_queue`, `replica`). |
| `vtop_broker_memory_queue_depth` | gauge | Admissions blocked waiting for budget. |
| `vtop_broker_memory_rejections_total{reason}` | counter | Admissions refused, by which ledger refused (#187). |
| `vtop_broker_backpressure_nanoseconds_total` / `_events_total` | counter | Time and count spent waiting on admission. |

### Integrity

The differentiator neither Kafka nor Northguard exposes: verification health is
alertable, not just throughput.

| Metric | Type | Meaning |
|---|---|---|
| `vtop_broker_segment_recovered` | gauge | 1 when this process re-opened an existing segment, 0 when it created a fresh one — so an all-zero recovery report cannot be mistaken for a clean recovery. |
| `vtop_broker_segment_recovery_truncated_bytes` | gauge | Bytes discarded past the durable boundary at open. Non-zero after a crash is expected and benign; **non-zero on every restart means the fsync story is wrong**. |
| `vtop_broker_segment_recovery_recovered_bytes` / `_records` | gauge | What recovery accepted as durable. |

## Structured logs

Both binaries honour `VTOP_LOG_FORMAT=json` (`vtopctl` also accepts `--json`),
emitting `{"level":…}` lines a Loki pipeline can parse without a regex. Logs go
to **stderr** only: stdout carries machine-readable command output and the
live-cluster ready markers the chaos harness parses.

In pretty mode, ANSI colour is emitted only to a real terminal. Writing escape
codes into a container's captured stderr corrupts every downstream parser — a
`level=~"WARN"` filter then matches nothing because the field names are wrapped
in escape sequences.

## What is not here yet

* Produce/fetch **rate and latency histograms** and session counts require
  instrumenting the broker request path; they are not derived from existing
  state and are tracked as follow-up work on #224.
* Proof-verification and evidence-gate counters cover segment recovery today;
  the retention/tier evidence lag metrics are follow-up work.
* The starter Grafana dashboard and the compose scrape wiring land with the
  same issue.

"""Cluster dashboard for the native VTOP nodes (#224).

Imported by build-dashboards.py. Separate from `dashboards_vtop` because it
answers a different question about a different process: that one is the archive
engine's pipeline, this one is the metadata Raft group and the native data plane
(`vtop-node`, #215).

All queries are PromQL against Mimir, scraped from each node's `/metrics`
endpoint. Conventions carried over from the engine dashboards:

  * rates are computed with rate()/increase(), never exported as gauges;
  * latency is asked as p99 from histograms, because an average hides exactly
    the tail that pages someone;
  * incident counters use `or vector(0)`. Prometheus only creates a series on
    the FIRST increment, so a healthy cluster would otherwise render "No data"
    on the panels that matter most — visually identical to a broken scrape.

The one convention specific to this dashboard: the node metrics use `-1` as the
"no value yet" sentinel (a Raft node with no log has no last index, and 0 is a
real index). Panels that would otherwise plot the sentinel as a real number
filter it out with `>= 0`, so an empty cluster reads as absent rather than as a
negative offset.
"""

from dashboards_common import GRAFANA_PLUGIN_VERSION

MIMIR = {"type": "prometheus", "uid": "mimir"}

# Every panel filters by node, so one dashboard serves a whole cluster and can
# also be narrowed to the single node being investigated. `instance` is added by
# the scraper and is present on every series, unlike any metric label.
SEL = '{instance=~"$instance"}'


def _q(expr, legend=""):
    return [{"datasource": MIMIR, "expr": expr, "legendFormat": legend}]


def _panel(title, gp, targets, unit=None, kind="timeseries", desc="", extra=None):
    p = {
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": MIMIR,
        "gridPos": gp,
        "targets": targets,
        "fieldConfig": {"defaults": {"custom": {"fillOpacity": 10}}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if extra:
        p.update(extra)
    return p


def _stat(title, gp, targets, desc="", unit=None, thresholds=None):
    p = _panel(title, gp, targets, unit=unit, kind="stat", desc=desc)
    # Identical option set to the engine dashboards; see dashboards_vtop for why
    # every field here (especially pluginVersion) has to be emitted in full.
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNonNull"], "fields": "", "values": False},
        "colorMode": "background" if thresholds else "value",
        "graphMode": "none",
        "text": {"valueSize": 22},
        "textMode": "value",
        "justifyMode": "center",
        "orientation": "auto",
        "percentChangeColorMode": "standard",
        "showPercentChange": False,
        "wideLayout": True,
    }
    p["pluginVersion"] = GRAFANA_PLUGIN_VERSION
    p["fieldConfig"]["defaults"].pop("custom", None)
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    return p


def _row(title, y):
    return {"type": "row", "title": title, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "collapsed": False, "panels": []}


def _templating():
    return {
        "list": [
            {
                "name": "instance",
                "label": "Node",
                "type": "query",
                "datasource": MIMIR,
                # Sourced from node_info, which every node process exports
                # unconditionally. A node whose broker has served nothing yet -
                # precisely the one being debugged after a restart - would be
                # unselectable if this came off a request counter.
                "query": {"query": "label_values(vtop_node_info, instance)", "refId": "instance"},
                "refresh": 2,
                "includeAll": True,
                "allValue": ".*",
                "multi": True,
                "current": {"text": "All", "value": "$__all"},
            }
        ]
    }


def _dash(uid, title, desc, panels, tags):
    return {
        "uid": uid, "title": title, "description": desc, "tags": tags,
        "timezone": "browser", "schemaVersion": 39, "version": 1,
        "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
        "templating": _templating(), "panels": panels,
    }


RED_ON_ANY = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
AMBER_ON_ANY = [{"color": "green", "value": None}, {"color": "orange", "value": 1}]
# Exactly one leader is healthy. Zero is an outage; two is a split brain, and
# both must be red — a threshold ladder that only reddened at zero would show a
# split-brain cluster in green.
EXACTLY_ONE = [
    {"color": "red", "value": None},
    {"color": "green", "value": 1},
    {"color": "red", "value": 2},
]

cluster = _dash(
    "vtop-cluster", "VTOP Cluster — nodes",
    "The metadata Raft group and the native data plane, from each node's own "
    "/metrics endpoint (#224). Start here for a cluster incident; the 'VTOP "
    "Engine' dashboard covers the archive pipeline instead.",
    [
        # ---- Metadata group ------------------------------------------------
        _row("Metadata group", 0),
        _stat("Meta nodes running", {"h": 4, "w": 4, "x": 0, "y": 1},
              _q(f'sum(vtop_meta_raft_running{SEL}) or vector(0)', "running"),
              desc="Nodes whose Raft core is alive. A node can answer /healthz "
                   "with a dead Raft core, which is why this is counted "
                   "separately from process liveness."),
        _stat("Leaders", {"h": 4, "w": 4, "x": 4, "y": 1},
              _q(f'sum(vtop_meta_raft_state{{instance=~"$instance",state="leader"}}) or vector(0)',
                 "leaders"),
              thresholds=EXACTLY_ONE,
              desc="Must be exactly 1. Zero means no metadata progress is "
                   "possible; two means split brain and is the more dangerous "
                   "of the two."),
        _stat("Highest term", {"h": 4, "w": 4, "x": 8, "y": 1},
              _q(f'max(vtop_meta_raft_term{SEL}) or vector(0)', "term"),
              desc="A term that climbs steadily means repeated elections — "
                   "usually timers set too tight for the network, or a node "
                   "that keeps losing contact."),
        _stat("Voters", {"h": 4, "w": 4, "x": 12, "y": 1},
              _q(f'max(vtop_meta_raft_voters{SEL}) or vector(0)', "voters"),
              desc="Committed membership size. This is the number a 3->5 "
                   "growth scenario asserts against."),
        _stat("Learners", {"h": 4, "w": 4, "x": 16, "y": 1},
              _q(f'max(vtop_meta_raft_learners{SEL}) or vector(0)', "learners"),
              desc="Replicating without voting — a node being caught up before "
                   "it joins quorum."),
        _stat("Quorum ack age (max ms)", {"h": 4, "w": 4, "x": 20, "y": 1},
              _q(f'max(vtop_meta_raft_millis_since_quorum_ack{SEL} >= 0) or vector(0)', "ms"),
              thresholds=AMBER_ON_ANY,
              desc="Milliseconds since a quorum acknowledged the leader. A "
                   "value that CLIMBS on a self-declared leader is the "
                   "signature of a partition: it is what separates 'isolated' "
                   "from 'slow'. -1 (not leading) is filtered out."),
        _panel("Raft log progress", {"h": 8, "w": 12, "x": 0, "y": 5},
               _q(f'vtop_meta_raft_last_log_index{SEL} >= 0', "{{instance}} log")
               + _q(f'vtop_meta_raft_last_applied_index{SEL} >= 0', "{{instance}} applied"),
               desc="Last log index against last applied. A widening gap means "
                    "the state machine is falling behind the log rather than "
                    "the cluster falling behind its peers."),
        _panel("Peer replication lag", {"h": 8, "w": 12, "x": 12, "y": 5},
               _q(f'vtop_meta_raft_peer_lag_entries{SEL} >= 0', "{{instance}} -> peer {{peer}}"),
               unit="none",
               desc="Entries each peer trails the leader by, published by the "
                    "leader only. -1 means the peer has acknowledged nothing "
                    "at all and is filtered out — unknown is not zero lag."),

        # ---- Ranges --------------------------------------------------------
        _row("Ranges and replication", 13),
        _stat("Fenced leaseholders", {"h": 4, "w": 6, "x": 0, "y": 14},
              _q(f'count(vtop_broker_lease_active{SEL} == 0) or vector(0)', "fenced"),
              thresholds=AMBER_ON_ANY,
              desc="Brokers that no longer hold their range's lease. They "
                   "refuse writes by design, so this is expected DURING a "
                   "handover and a problem if it persists."),
        _stat("Disconnected followers", {"h": 4, "w": 6, "x": 6, "y": 14},
              _q(f'count(vtop_broker_follower_connected{SEL} == 0) or vector(0)', "disconnected"),
              thresholds=RED_ON_ANY,
              desc="Followers with no live replication stream from their "
                   "leader. Quorum durability degrades before produce starts "
                   "failing, so this leads the incident."),
        _panel("Committed offsets", {"h": 8, "w": 12, "x": 12, "y": 14},
               _q(f'vtop_broker_local_committed_offset{SEL}', "{{instance}} {{topic}} durable")
               + _q(f'vtop_broker_cluster_committed_offset{SEL}', "{{instance}} {{topic}} quorum"),
               desc="Each replica's durable boundary against the quorum "
                    "high-water mark. A LINE THAT FLATTENS is the honest "
                    "signal that writes have stalled: offsets are read "
                    "non-blocking, so a stuck fsync stops the gauge advancing "
                    "rather than hanging the scrape."),
        _panel("Follower lag", {"h": 8, "w": 12, "x": 0, "y": 18},
               _q(f'vtop_broker_follower_lag_records{SEL}', "{{instance}} -> {{follower}}"),
               unit="none",
               desc="Records each follower trails its leader's durable "
                    "boundary by."),

        # ---- Throughput ----------------------------------------------------
        _row("Throughput and latency", 26),
        _panel("Request rate", {"h": 8, "w": 12, "x": 0, "y": 27},
               _q(f'sum by (kind, outcome) (rate(vtop_broker_requests_total{SEL}[5m]))',
                  "{{kind}} {{outcome}}"),
               unit="reqps",
               desc="Served and refused are separate series on purpose: a "
                    "fencing rejection is the system working, and folding it "
                    "into one error rate teaches an operator to ignore the "
                    "panel during a correct failover."),
        _panel("p99 request latency", {"h": 8, "w": 12, "x": 12, "y": 27},
               _q('histogram_quantile(0.99, sum by (kind, le) '
                  f'(rate(vtop_broker_request_duration_seconds_bucket{SEL}[5m])))',
                  "{{kind}} p99"),
               unit="s",
               desc="Measured around the broker's own work, NOT the socket "
                    "write, so a slow consumer's backpressure cannot be "
                    "mistaken for a slow log."),
        _panel("Record throughput", {"h": 8, "w": 12, "x": 0, "y": 35},
               _q(f'sum(rate(vtop_broker_produced_records_total{SEL}[5m]))', "produced")
               + _q(f'sum(rate(vtop_broker_fetched_records_total{SEL}[5m]))', "fetched"),
               unit="reqps",
               desc="Volume on SUCCESSFUL requests only; a refused append is "
                    "not throughput."),
        _panel("Sessions", {"h": 8, "w": 12, "x": 12, "y": 35},
               _q(f'sum by (role) (vtop_broker_sessions_active{SEL})', "{{role}} active")
               + _q(f'sum by (reason) (rate(vtop_broker_sessions_refused_total{SEL}[5m]))',
                    "refused: {{reason}}"),
               desc="Open sessions by role, beside the connections that never "
                    "became one. A rising `capacity` refusal rate means "
                    "max_sessions, not a client bug."),

        # ---- Backpressure --------------------------------------------------
        _row("Backpressure", 43),
        _stat("Budget rejections (1h)", {"h": 4, "w": 6, "x": 0, "y": 44},
              _q(f'sum(increase(vtop_broker_memory_rejections_total{SEL}[1h])) or vector(0)',
                 "rejections"),
              thresholds=AMBER_ON_ANY,
              desc="Admissions refused by a memory budget (#187). Failing "
                   "closed is correct behaviour under load; a sustained rate "
                   "means the ceilings are wrong for the workload."),
        _panel("Memory in use", {"h": 8, "w": 9, "x": 6, "y": 44},
               _q(f'sum by (scope) (vtop_broker_memory_used_bytes{SEL})', "{{scope}}"),
               unit="bytes",
               desc="Bytes charged to each budget ledger."),
        _panel("Time spent waiting for budget", {"h": 8, "w": 9, "x": 15, "y": 44},
               _q(f'sum(rate(vtop_broker_backpressure_nanoseconds_total{SEL}[5m])) / 1e9',
                  "seconds waiting per second"),
               desc="Above 1 means more than one request-second per wall "
                    "second is parked on admission — the point at which "
                    "backpressure is the bottleneck rather than a safety net."),

        # ---- Integrity -----------------------------------------------------
        _row("Integrity", 52),
        _stat("Segments recovered", {"h": 4, "w": 6, "x": 0, "y": 53},
              _q(f'sum(vtop_broker_segment_recovered{SEL}) or vector(0)', "recovered"),
              desc="Replicas that re-opened an existing segment rather than "
                   "creating a fresh one. Expected after a restart."),
        _stat("Recovery truncated bytes", {"h": 4, "w": 6, "x": 6, "y": 53},
              _q(f'sum(vtop_broker_segment_recovery_truncated_bytes{SEL}) or vector(0)', "bytes"),
              unit="bytes",
              thresholds=AMBER_ON_ANY,
              desc="Bytes discarded past the durable boundary at open. "
                   "Non-zero after a crash is expected and benign - it is the "
                   "torn tail. Non-zero on EVERY restart means the fsync story "
                   "is wrong, and this is the differentiator neither Kafka nor "
                   "Northguard exposes."),
        _panel("Fencing epochs", {"h": 8, "w": 12, "x": 12, "y": 53},
               _q(f'vtop_broker_meta_fencing_epoch{SEL}', "{{instance}} metadata")
               + _q(f'vtop_broker_held_fencing_epoch{SEL}', "{{instance}} held"),
               desc="Where the two diverge, that broker has been fenced: "
                    "metadata has granted the range to someone else and this "
                    "one must refuse writes."),
    ],
    ["vtop", "cluster", "raft", "broker"],
)

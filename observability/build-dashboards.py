#!/usr/bin/env python3
"""Generate the VTOP Grafana dashboards.

Dashboard JSON is verbose and unreviewable by hand, so the queries — the part
that carries the meaning — are written here in readable form and the JSON is
generated. Regenerate with:

    python3 observability/build-dashboards.py

ENGINE PANELS USE REAL METRICS
-----------------------------
The engine now exports its own Prometheus metrics (issue #46), so the VTOP
dashboards query Mimir directly rather than reconstructing numbers from logs.
Logs remain the place for high-cardinality detail (batch_id, object URIs) that
must never become a metric label.

Rates are computed here with rate()/increase() rather than exported as gauges:
a gauge of a rate is a snapshot that lies between scrapes.
"""

import json
import os

from dashboards_common import GRAFANA_PLUGIN_VERSION

LOKI = {"type": "loki", "uid": "loki"}
MIMIR = {"type": "prometheus", "uid": "mimir"}

# The engine's log stream. `event` is promoted to a label by Alloy; the
# high-cardinality fields (batch_id, uris) deliberately are not.
ENGINE = '{service="vtop-engine"}'


def panel(title, gp, targets, ds, unit=None, kind="timeseries", desc="", extra=None):
    p = {
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": ds,
        "gridPos": gp,
        "targets": targets,
        "fieldConfig": {"defaults": {"custom": {}}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if extra:
        p.update(extra)
    return p


def logql(expr, legend=""):
    return [{"datasource": LOKI, "expr": expr, "legendFormat": legend, "queryType": "range"}]


def promql(expr, legend=""):
    return [{"datasource": MIMIR, "expr": expr, "legendFormat": legend}]


def stat(title, gp, targets, ds, desc="", unit=None, thresholds=None, text_mode="value"):
    p = panel(title, gp, targets, ds, unit=unit, kind="stat", desc=desc)
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNonNull"], "fields": "", "values": False},
        "textMode": text_mode,
        "colorMode": "background" if thresholds else "value",
        # No sparkline and an explicit value size: these tiles are only 4 rows
        # tall, and with graphMode "area" competing for the space Grafana
        # auto-shrank the number until it vanished — the tile rendered as a bare
        # colour and the value only reappeared in the (much larger) edit view.
        "graphMode": "none",
        "text": {"valueSize": 22},
        "justifyMode": "center",
        # Grafana normalises a stat panel to this exact option set. Emitting it
        # in full - and, critically, emitting `pluginVersion` - stops Grafana
        # running the stat panel's schema-MIGRATION handler on load, which
        # rewrites `options` and was silently clobbering reduceOptions.calcs.
        # That is why a tile rendered as a bare colour until the panel was opened
        # in the editor and saved: saving writes back the normalised model.
        "orientation": "auto",
        "percentChangeColorMode": "standard",
        "showPercentChange": False,
        "wideLayout": True,
    }
    p["pluginVersion"] = GRAFANA_PLUGIN_VERSION
    # `custom` holds TIMESERIES-only field options (fillOpacity); on a stat panel
    # it is meaningless and contributes to the migration rewrite above.
    p["fieldConfig"]["defaults"].pop("custom", None)
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    return p


def dashboard(uid, title, desc, panels, tags):
    return {
        "uid": uid,
        "title": title,
        "description": desc,
        "tags": tags,
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "time": {"from": "now-30m", "to": "now"},
        "panels": panels,
    }


def row(title, y):
    return {"type": "row", "title": title, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "collapsed": False, "panels": []}


# ---------------------------------------------------------------------------
# 1. Overview — one screen: is data flowing end to end?
# ---------------------------------------------------------------------------
OK = [{"color": "red", "value": None}, {"color": "green", "value": 1}]
BAD = [{"color": "green", "value": None}, {"color": "red", "value": 1}]

overview = dashboard(
    "vtop-overview", "VTOP — Overview",
    "One screen: is telemetry flowing from source to verified object, and is "
    "anything failing verification? Engine panels use the engine's own metrics; "
    "see the 'VTOP Engine' dashboard for depth.",
    [
        row("Is it working?", 0),
        stat("Batches committed (5m)", {"h": 5, "w": 6, "x": 0, "y": 1},
             promql('sum(increase(vtop_commits_total[5m])) or vector(0)', "committed"),
             MIMIR, thresholds=OK,
             desc="A commit means a batch completed the FULL verified pipeline. "
                  "Zero while data is flowing means the pipeline is stalled."),
        stat("Verification failures (5m)", {"h": 5, "w": 6, "x": 6, "y": 1},
             promql('sum(increase(vtop_verification_failures_total[5m])) or vector(0)', "failures"),
             MIMIR, thresholds=BAD,
             desc="ANY non-zero value is an incident: an object did not match "
                  "its manifest. The engine refuses to commit, so data is safe "
                  "but stuck."),
        stat("Replays required (5m)", {"h": 5, "w": 6, "x": 12, "y": 1},
             promql('sum(increase(vtop_replay_required_total[5m])) or vector(0)', "replays"),
             MIMIR, thresholds=BAD,
             desc="Batches sent back to be re-read from source. A steady rate "
                  "means work is being redone."),
        stat("Objects in MinIO", {"h": 5, "w": 6, "x": 18, "y": 1},
             promql('sum(minio_cluster_usage_object_total)', "objects"),
             MIMIR, desc="Objects actually landed in object storage — the "
                         "destination side of the same story."),
        row("Flow", 6),
        panel("Batches by state (per sec)", {"h": 8, "w": 12, "x": 0, "y": 7},
              promql('sum by (state) (rate(vtop_batches_total[5m]))', "{{state}}"),
              MIMIR,
              desc="The pipeline funnel. verified and source_committed should "
                   "track each other; where the lines diverge is where batches "
                   "are stopping."),
        panel("Kafka consumer lag", {"h": 8, "w": 12, "x": 12, "y": 7},
              promql('sum by (topic) (kafka_consumergroup_lag{consumergroup="vtop-engine"})', "{{topic}}"),
              MIMIR,
              desc="Records produced but not yet committed by VTOP. Offsets "
                   "advance only after VERIFIED, so lag falling to 0 proves the "
                   "whole verified path is working end to end."),
    ],
    ["vtop", "overview"],
)

# ---------------------------------------------------------------------------
# 2. Safety / invariant — the dashboard that actually matters
# ---------------------------------------------------------------------------
safety = dashboard(
    "vtop-safety", "VTOP — Safety & invariant",
    "The engine's whole purpose: SOURCE_COMMITTED is forbidden until VERIFIED. "
    "This dashboard watches the invariant and its failure modes.",
    [
        row("The invariant", 0),
        stat("Verification failures (1h)", {"h": 5, "w": 8, "x": 0, "y": 1},
             promql('sum(increase(vtop_verification_failures_total[1h])) or vector(0)', "failures"),
             MIMIR, thresholds=BAD,
             desc="An uploaded object did not match its manifest checksum/size. "
                  "The engine refuses to commit the source, so nothing is lost — "
                  "but the batch is stuck and someone must look."),
        stat("Backend-limited verifications (1h)", {"h": 5, "w": 8, "x": 8, "y": 1},
             promql('sum(increase(vtop_verification_backend_limited_total[1h])) or vector(0)', "size-only"),
             MIMIR, thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}],
             desc="Verified by SIZE ONLY, not checksum. The default policy "
                  "refuses this result; any non-zero value means an explicit "
                  "require_strong_verification: false compatibility/lab "
                  "opt-out is active."),
        stat("Replay required (1h)", {"h": 5, "w": 8, "x": 16, "y": 1},
             promql('sum(increase(vtop_replay_required_total[1h])) or vector(0)', "replays"),
             MIMIR, thresholds=BAD,
             desc="Batches returned to the source for re-reading after a "
                  "failure. Safe by design, but a steady rate means repeated "
                  "wasted work."),
        row("Recovery", 6),
        panel("Recovery scans (engine restarts)", {"h": 7, "w": 12, "x": 0, "y": 7},
              logql(f'sum(count_over_time({ENGINE} |= "recovery_scan" [5m]))', "recovery_scan"),
              LOKI,
              desc="Emitted at startup for each incomplete batch found. A spike "
                   "means the engine restarted with work in flight — check that "
                   "those batches then reached source_committed."),
        panel("Verified vs committed (committed must never exceed verified)", {"h": 7, "w": 12, "x": 12, "y": 7},
              [
                  {"datasource": MIMIR, "expr": 'sum(rate(vtop_verified_total[5m]))', "legendFormat": "verified/sec"},
                  {"datasource": MIMIR, "expr": 'sum(rate(vtop_commits_total[5m]))', "legendFormat": "committed/sec"},
              ],
              MIMIR,
              desc="verification_passed should be immediately followed by "
                   "source_committed. A persistent gap means batches are "
                   "verifying but not committing — the exact state the "
                   "VERIFIED-but-not-committed recovery path exists for."),
    ],
    ["vtop", "safety"],
)

# ---------------------------------------------------------------------------
# 3. Pipeline performance
# ---------------------------------------------------------------------------
pipeline = dashboard(
    "vtop-pipeline", "VTOP — Pipeline performance",
    "Throughput, compression and per-stage latency from the engine's own "
    "Prometheus metrics (#46). These were previously reconstructed from "
    "batch_metrics log events; they are now native counters and histograms, so "
    "there is no log-parsing fragility and the numbers survive log rotation.",
    [
        row("Throughput", 0),
        panel("Records/sec", {"h": 7, "w": 8, "x": 0, "y": 1},
              promql('sum(rate(vtop_records_total[1m])) or vector(0)', "records/sec"),
              MIMIR, desc="Records leaving the engine into archived objects."),
        panel("Bytes in vs out", {"h": 7, "w": 8, "x": 8, "y": 1},
              [
                  {"datasource": MIMIR, "expr": 'sum(rate(vtop_bytes_in_total[1m])) or vector(0)', "legendFormat": "uncompressed in"},
                  {"datasource": MIMIR, "expr": 'sum(rate(vtop_bytes_out_total[1m])) or vector(0)', "legendFormat": "compressed out (on the wire)"},
              ],
              MIMIR, unit="Bps",
              desc="The gap between the two lines is what compression is "
                   "saving on the wire."),
        panel("Compression ratio", {"h": 7, "w": 8, "x": 16, "y": 1},
              [
                  {"datasource": MIMIR, "expr": 'histogram_quantile(0.5, sum by (le) (rate(vtop_compression_ratio_bucket[5m]))) and (sum(rate(vtop_compression_ratio_bucket[5m])) > 0) or vector(0)', "legendFormat": "p50"},
                  {"datasource": MIMIR, "expr": 'histogram_quantile(0.95, sum by (le) (rate(vtop_compression_ratio_bucket[5m]))) and (sum(rate(vtop_compression_ratio_bucket[5m])) > 0) or vector(0)', "legendFormat": "p95"},
              ],
              MIMIR, desc="uncompressed/compressed; higher is better. A sudden "
                          "drop toward 1.0 suggests the data shape changed or "
                          "compression is effectively off. Guarded so an idle "
                          "histogram reads 0, not NaN."),
        row("Latency by stage", 8),
        panel("Per-stage duration (p95)", {"h": 8, "w": 12, "x": 0, "y": 9},
              [
                  {"datasource": MIMIR, "expr": 'histogram_quantile(0.95, sum by (le, stage) (rate(vtop_stage_duration_seconds_bucket[5m]))) and (sum by (stage) (rate(vtop_stage_duration_seconds_bucket[5m])) > 0)', "legendFormat": "{{stage}}"},
              ],
              MIMIR, unit="s",
              desc="Which stage owns the time. Upload usually dominates - it is "
                   "the documented bottleneck, not the state store. No vector(0) "
                   "fallback here: this series is grouped by stage, and vector(0)'s "
                   "empty labelset never matches a {stage=...} series, so it would "
                   "add a phantom 0 line that is always on. An idle per-stage "
                   "breakdown reading No data is the honest rendering."),
        panel("Total batch latency", {"h": 8, "w": 12, "x": 12, "y": 9},
              [
                  {"datasource": MIMIR, "expr": 'histogram_quantile(0.5, sum by (le) (rate(vtop_batch_duration_seconds_bucket[5m]))) and (sum(rate(vtop_batch_duration_seconds_bucket[5m])) > 0) or vector(0)', "legendFormat": "p50"},
                  {"datasource": MIMIR, "expr": 'histogram_quantile(0.95, sum by (le) (rate(vtop_batch_duration_seconds_bucket[5m]))) and (sum(rate(vtop_batch_duration_seconds_bucket[5m])) > 0) or vector(0)', "legendFormat": "p95"},
              ],
              MIMIR, unit="s",
              desc="Batch start to source-committed. NOTE: batches seal on "
                   "max_batch_age (60s by default), so an idle lab shows a "
                   "sawtooth - that is the timer, not slowness."),
    ],
    ["vtop", "performance"],
)

# ---------------------------------------------------------------------------
# 5. MinIO
# ---------------------------------------------------------------------------
minio = dashboard(
    "vtop-minio", "VTOP — Object storage (MinIO)",
    "The destination. Native Prometheus metrics (auth disabled in the lab). "
    "This is where verified objects and manifests actually land.",
    [
        row("Archive", 0),
        stat("Objects stored", {"h": 5, "w": 8, "x": 0, "y": 1},
             promql('sum(minio_cluster_usage_object_total)', "objects"), MIMIR,
             desc="Each verified batch writes TWO objects: the compressed data "
                  "object and its manifest."),
        stat("Bytes stored", {"h": 5, "w": 8, "x": 8, "y": 1},
             promql('sum(minio_cluster_usage_total_bytes)', "bytes"), MIMIR, unit="bytes"),
        stat("Free capacity", {"h": 5, "w": 8, "x": 16, "y": 1},
             promql('sum(minio_cluster_capacity_raw_free_bytes)', "free"), MIMIR, unit="bytes",
             desc="A full backend fails uploads, which the engine treats as a "
                  "pre-VERIFIED failure - safe (no commit), but everything stops."),
        row("Traffic", 6),
        panel("S3 requests/sec", {"h": 7, "w": 12, "x": 0, "y": 7},
              promql('sum by (api) (rate(minio_s3_requests_total[1m]))', "{{api}}"),
              MIMIR, desc="PutObject dominates during ingest; GetObject/StatObject "
                          "appear during verification."),
        panel("S3 errors/sec", {"h": 7, "w": 12, "x": 12, "y": 7},
              promql('sum by (api) (rate(minio_s3_requests_errors_total[1m]))', "{{api}}"),
              MIMIR, desc="Upload errors keep batches pre-VERIFIED, so they are "
                          "replayed rather than lost - but they stall progress."),
    ],
    ["vtop", "minio"],
)

# ---------------------------------------------------------------------------
# 6. Logs explorer
# ---------------------------------------------------------------------------
logs = dashboard(
    "vtop-logs", "VTOP — Logs",
    "Raw structured events from every lab component. The engine's metrics now "
    "come from its own /metrics endpoint, so logs are where the "
    "high-cardinality detail lives: batch_id, object URIs, error text — the "
    "things that must never become metric labels. When a stat panel goes red, "
    "the reason is here.",
    [
        row("Engine", 0),
        panel("Errors and warnings", {"h": 8, "w": 24, "x": 0, "y": 1},
              logql(f'{{service="vtop-engine"}} | json | level=~"WARN|ERROR"'),
              LOKI, kind="logs",
              extra={"options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}},
              desc="Look here first when a stat panel goes red. The engine emits "
                   "JSON logs (VTOP_LOG_FORMAT=json), so `| json` exposes `level` "
                   "and the structured fields as filterable labels."),
        panel("Committed batches — commit + object URI", {"h": 8, "w": 24, "x": 0, "y": 9},
              logql(f'{{service="vtop-engine", event=~"source_committed|object_uploaded"}} | json'),
              LOKI, kind="logs",
              extra={"options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}},
              desc="The high-cardinality detail metrics deliberately omit — batch "
                   "ids and object URIs. Two correlated events per batch_id: "
                   "object_uploaded carries the object `uri` (where it landed); "
                   "source_committed marks the commit that only happens after "
                   "VERIFIED. The audit trail behind the committed-count stat."),
        row("All components", 17),
        panel("All lab logs", {"h": 10, "w": 24, "x": 0, "y": 18},
              logql('{job="docker"}'),
              LOKI, kind="logs",
              extra={"options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}},
              desc="Engine, Kafka, MinIO and the collector together — useful "
                   "when the question is which component broke first."),
    ],
    ["vtop", "logs"],
)

from dashboards_kafka import kafka  # noqa: E402
from dashboards_pipeline import pipeline as pipeline_flow  # noqa: E402
from dashboards_flow import flow as flow_drawio  # noqa: E402
from dashboards_vtop import engine as vtop_engine  # noqa: E402


def assign_panel_ids(dash):
    """Give every panel a unique numeric `id`.

    Grafana REQUIRES this. Without it the grid is mis-laid-out (panels render
    full-width and stacked, ignoring gridPos) and query results are not bound
    back to their panel, so a stat tile shows its threshold colour but no value.
    Opening the panel in the editor forces Grafana to mint an id, which is why
    the number appeared only on edit. Generated dashboards had no ids at all.
    """
    next_id = 1
    for p in dash.get("panels", []):
        p["id"] = next_id
        next_id += 1
        # Panels nested inside a collapsed row need ids too.
        for sub in p.get("panels", []) or []:
            sub["id"] = next_id
            next_id += 1
    return dash


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "grafana", "dashboards")
    os.makedirs(out, exist_ok=True)
    for name, d in [
        # The engine's own dashboard first: it is the one that matters.
        ("vtop-engine", vtop_engine),
        ("vtop-pipeline-flow", pipeline_flow),
        ("vtop-flow-drawio", flow_drawio),
        ("vtop-overview", overview),
        ("vtop-safety", safety),
        ("vtop-pipeline", pipeline),
        ("vtop-kafka", kafka),
        ("vtop-minio", minio),
        ("vtop-logs", logs),
    ]:
        path = os.path.join(out, f"{name}.json")
        assign_panel_ids(d)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(d, fh, indent=2)
            fh.write("\n")
        print(f"wrote {path} ({len(d['panels'])} panels)")

"""VTOP — Pipeline flow, drawn with the andrewbmchugh Flow panel.

Imported by build-dashboards.py.

A second, draw.io-style rendering of the telemetry pipeline (the first is the
core Canvas dashboard in dashboards_pipeline.py). This one uses the community
**Flow** panel (andrewbmchugh-flow-panel): you author an SVG in draw.io, tag the
cell ids, and drive labels / colours / connector flow-animations from queries
via a YAML panelConfig.

Why this plugin and not agenty-flowcharting-panel: the older flowcharting plugin
is AngularJS, which Grafana 11+ removed, so it does not load on Grafana 13. Flow
is React, signed, and requires Grafana >= 10 — it is the maintained successor and
renders on the lab's Grafana 13.1.

The SVG and panelConfig live next to this file as editable source and are inlined
into the dashboard JSON so the dashboard is self-contained (no external URLs to
host). The plugin must be installed — docker-compose.observability.yml sets
GF_INSTALL_PLUGINS=andrewbmchugh-flow-panel.
"""

import os

MIMIR = {"type": "prometheus", "uid": "mimir"}

_HERE = os.path.dirname(os.path.abspath(__file__))

# (dataRef, promql). dataRef is the legendFormat the Flow cells bind to; it must
# match the `dataRef:` values in vtop-flow-panelconfig.yaml. Same queries as the
# Canvas pipeline, including the idle-NaN guard on the histogram quantiles.
TARGETS = [
    ("msgin",    'sum(rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m])) or vector(0)'),
    ("bytesin",  'sum(rate(vtop_bytes_in_total[1m])) or vector(0)'),
    ("bytesout", 'sum(rate(vtop_bytes_out_total[1m])) or vector(0)'),
    ("ratio",    'histogram_quantile(0.5, sum by (le) (rate(vtop_compression_ratio_bucket[5m]))) '
                 'and (sum(rate(vtop_compression_ratio_bucket[5m])) > 0) or vector(0)'),
    ("p95",      'histogram_quantile(0.95, sum by (le) (rate(vtop_stage_duration_seconds_bucket[5m]))) '
                 'and (sum(rate(vtop_stage_duration_seconds_bucket[5m])) > 0) or vector(0)'),
    ("recs",     'sum(rate(vtop_records_total[1m])) or vector(0)'),
    ("commits",  'sum(vtop_commits_total) or vector(0)'),
    ("failed",   'sum(increase(vtop_failed_total[1h])) or vector(0)'),
    ("lag",      'sum(kafka_consumergroup_lag{consumergroup="vtop-engine"}) or vector(0)'),
    ("inflight", 'vtop_inflight_batches or vector(0)'),
]


def _read(name):
    with open(os.path.join(_HERE, "grafana", "flow", name), encoding="utf-8") as fh:
        return fh.read()


def _panel():
    return {
        "type": "andrewbmchugh-flow-panel",
        "title": "VTOP telemetry pipeline (draw.io / Flow)",
        "description": (
            "The telemetry path as a draw.io SVG driven live by the Flow panel: "
            "labels show messages/sec and bytes/sec in, compression and stage "
            "p95 in the engine, bytes/sec out, and commits / failed / lag. "
            "Connectors animate at a speed set by throughput. verify-before-"
            "commit is the centre. Rendered with the React Flow panel, which "
            "loads on Grafana 13 (the AngularJS flowcharting plugin does not)."
        ),
        "datasource": MIMIR,
        "gridPos": {"h": 18, "w": 24, "x": 0, "y": 1},
        "targets": [
            {"datasource": MIMIR, "refId": chr(65 + i), "expr": expr,
             "legendFormat": ref, "range": True}
            for i, (ref, expr) in enumerate(TARGETS)
        ],
        "options": {
            "svg": _read("vtop-flow.svg"),
            "panelConfig": _read("vtop-flow-panelconfig.yaml"),
            "siteConfig": "",
            "testDataEnabled": False,
            "timeSliderEnabled": True,
            "animationsEnabled": True,
            "animationControlEnabled": True,
            "panZoomEnabled": True,
            "highlighterEnabled": False,
            "debuggingCtr": {"colorsCtr": 0, "dataCtr": 0, "displaySvgCtr": 0,
                             "mappingsCtr": 0, "timingsCtr": 0},
        },
    }


flow = {
    "uid": "vtop-flow-drawio",
    "title": "VTOP — Pipeline flow (draw.io)",
    "description": "draw.io SVG pipeline driven live by the Flow panel.",
    "tags": ["vtop", "pipeline", "flow", "drawio"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": [
        {"type": "row", "title": "draw.io pipeline", "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0}},
        _panel(),
    ],
}

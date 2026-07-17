"""VTOP — Pipeline flow (SCADA-style live diagram).

Imported by build-dashboards.py.

A Canvas panel draws the telemetry path left-to-right with a technology logo at
each stage and LIVE NUMBERS on the arrows between them: messages/sec and bytes/sec
in from the sources, compression and stage latency inside the engine, bytes/sec
out to object storage, and the verify->commit decision with commits / failures /
lag underneath.

Built with the Canvas panel, NOT the flowchart plugin: agenty-flowcharting-panel
is an AngularJS plugin and Grafana 11+ removed AngularJS, so it will not load on
Grafana 13 (confirmed absent from /api/plugins after install). Canvas is core
Grafana's supported successor for "diagram with live values" and needs no plugin.

See PIPELINE_DIAGRAM.md for the ASCII design this renders.
"""

MIMIR = {"type": "prometheus", "uid": "mimir"}

# Logos are mounted into Grafana's public path (see docker-compose.observability.yml)
# so the icons resolve offline, not from a CDN.
def _logo(name):
    return f"public/img/vtop/{name}.svg"


# --- Canvas element builders ------------------------------------------------

def _icon(name, x, y, w, h, path):
    """A logo."""
    return {
        "type": "icon",
        "name": name,
        "config": {"path": {"fixed": path}, "fill": {"fixed": "#ffffff00"}},
        "background": {"color": {"fixed": "transparent"}},
        "placement": {"top": y, "left": x, "width": w, "height": h},
    }


def _label(name, x, y, w, h, text, size=14, color="#d8d9da", align="center"):
    """Static text (stage names, arrows)."""
    return {
        "type": "text",
        "name": name,
        "config": {
            "text": {"fixed": text},
            "color": {"fixed": color},
            "size": size,
            "align": align,
            "valign": "middle",
        },
        "background": {"color": {"fixed": "transparent"}},
        "placement": {"top": y, "left": x, "width": w, "height": h},
    }


def _metric(name, x, y, w, h, field, prefix="", suffix="", size=18, color="#73bf69"):
    """A number bound to a query field, shown ON the diagram."""
    return {
        "type": "metric-value",
        "name": name,
        "config": {
            # Canvas matches on the field's DISPLAY name, which Prometheus sets
            # from legendFormat (verified: displayNameFromDS == the legend). The
            # raw schema name is "Value" for every query and would collide, so we
            # bind to the unique legend instead.
            "text": {"mode": "field", "field": field, "fixed": ""},
            "color": {"fixed": color},
            "size": size,
            "align": "center",
            "valign": "middle",
            "prefix": prefix,
            "suffix": suffix,
        },
        "background": {"color": {"fixed": "transparent"}},
        "placement": {"top": y, "left": x, "width": w, "height": h},
    }


def _box(name, x, y, w, h, color="#ffffff22"):
    """A faint container rectangle so stages read as blocks."""
    return {
        "type": "rectangle",
        "name": name,
        "config": {"backgroundColor": {"fixed": color}, "radius": 6},
        "background": {"color": {"fixed": color}},
        "border": {"color": {"fixed": "#ffffff44"}, "width": 1},
        "placement": {"top": y, "left": x, "width": w, "height": h},
    }


# Each query feeds one or more metric-value elements by field name. refId order
# matters only for the "A/B/C" field references.
# (field_name, promql). field_name is the legendFormat AND what the Canvas
# elements bind to - it must be unique per query.
TARGETS = [
    ("msgin",    'sum(rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m])) or vector(0)'),
    ("bytesin",  'sum(rate(vtop_bytes_in_total[1m])) or vector(0)'),
    ("bytesout", 'sum(rate(vtop_bytes_out_total[1m])) or vector(0)'),
    # histogram_quantile over a histogram that has series but zero observations
    # in the window returns NaN, and NaN is still a series, so `or vector(0)`
    # would NOT replace it - the field would render blank once the lab goes idle.
    # Guard with `and (sum(rate) > 0)` so the quantile is kept ONLY when there
    # were observations; otherwise it drops out and the fallback shows 0.
    ("ratio",    'histogram_quantile(0.5, sum by (le) (rate(vtop_compression_ratio_bucket[5m]))) '
                 'and (sum(rate(vtop_compression_ratio_bucket[5m])) > 0) or vector(0)'),
    ("p95",      'histogram_quantile(0.95, sum by (le) (rate(vtop_stage_duration_seconds_bucket[5m]))) '
                 'and (sum(rate(vtop_stage_duration_seconds_bucket[5m])) > 0) or vector(0)'),
    ("commits",  'sum(vtop_commits_total) or vector(0)'),
    ("failed",   'sum(increase(vtop_failed_total[1h])) or vector(0)'),
    ("lag",      'sum(kafka_consumergroup_lag{consumergroup="vtop-engine"}) or vector(0)'),
    ("inflight", 'vtop_inflight_batches or vector(0)'),
    ("recsout",  'sum(rate(vtop_records_total[1m])) or vector(0)'),
]


def _panel():
    elements = [
        # ---- SOURCES block ----
        _box("src-box", 20, 40, 200, 260, "#1f2a4022"),
        _label("src-title", 20, 45, 200, 24, "SOURCES", 16, "#8ab4f8"),
        _icon("kafka-logo", 70, 80, 44, 44, _logo("apachekafka")),
        _label("kafka-l", 20, 128, 200, 20, "Kafka", 13),
        _label("files-l", 20, 170, 200, 20, "Files", 13, "#9aa0a6"),
        _label("syslog-l", 20, 210, 200, 20, "Syslog spool", 13, "#9aa0a6"),

        # ---- arrow SOURCES -> ENGINE, with live in-rates ----
        _label("arr1", 230, 150, 120, 24, "──▶", 22, "#8ab4f8"),
        _metric("m-msgin", 228, 100, 130, 26, "msgin", suffix=" msg/s", size=16, color="#8ab4f8"),
        _metric("m-bytesin", 228, 190, 130, 26, "bytesin", suffix=" B/s in", size=15, color="#8ab4f8"),

        # ---- ENGINE block ----
        _box("eng-box", 360, 30, 300, 300, "#12331e22"),
        _label("eng-title", 360, 36, 300, 22, "VTOP ENGINE — verify before commit", 14, "#73bf69"),
        _icon("rust-logo", 480, 66, 56, 56, _logo("rust")),
        _label("eng-stages", 360, 130, 300, 20,
               "seal → compress → checksum → upload", 12, "#c8c8c8"),
        _label("ratio-l", 375, 168, 130, 18, "compression", 11, "#9aa0a6"),
        _metric("m-ratio", 375, 186, 130, 26, "ratio", suffix="x", size=20, color="#73bf69"),
        _label("p95-l", 520, 168, 130, 18, "stage p95", 11, "#9aa0a6"),
        _metric("m-p95", 520, 186, 130, 26, "p95", suffix=" s", size=20, color="#f2cc0c"),

        # ---- arrow ENGINE -> MINIO, with live out-rate ----
        _label("arr2", 670, 150, 120, 24, "──▶", 22, "#f2a600"),
        _metric("m-bytesout", 668, 100, 140, 26, "bytesout", suffix=" B/s out", size=15, color="#f2a600"),
        _label("arr2-l", 668, 200, 140, 20, "object + manifest", 11, "#9aa0a6"),

        # ---- ARCHIVE block ----
        _box("arc-box", 810, 40, 190, 200, "#33240022"),
        _label("arc-title", 810, 46, 190, 22, "OBJECT STORAGE", 14, "#f2a600"),
        _icon("minio-logo", 870, 84, 60, 60, _logo("minio")),
        _label("arc-l", 810, 156, 190, 20, "MinIO / S3", 13),
        _metric("m-recs", 810, 190, 190, 24, "recsout", suffix=" rec/s archived", size=13, color="#f2a600"),

        # ---- VERIFY decision + commit (the invariant) ----
        _box("ver-box", 360, 350, 300, 110, "#12331e22"),
        _label("ver-title", 360, 356, 300, 22, "VERIFIED?  (commit ONLY if true)", 13, "#73bf69"),
        _label("commits-l", 375, 392, 130, 18, "commits", 11, "#9aa0a6"),
        _metric("m-commits", 375, 410, 130, 26, "commits", size=20, color="#73bf69"),
        _label("failed-l", 520, 392, 130, 18, "failed", 11, "#9aa0a6"),
        _metric("m-failed", 520, 410, 130, 26, "failed", size=20, color="#e0533d"),

        # ---- STATE STORE ----
        _box("st-box", 810, 300, 190, 150, "#20203022"),
        _label("st-title", 810, 306, 190, 22, "STATE STORE", 13, "#a78bfa"),
        _icon("sqlite-logo", 875, 336, 50, 50, _logo("sqlite")),
        _label("lag-l", 810, 392, 95, 18, "kafka lag", 11, "#9aa0a6"),
        _metric("m-lag", 810, 408, 95, 26, "lag", size=18, color="#f2cc0c"),
        _label("inflight-l", 905, 392, 95, 18, "in-flight", 11, "#9aa0a6"),
        _metric("m-inflight", 905, 408, 95, 26, "inflight", size=18, color="#a78bfa"),
    ]

    return {
        "type": "canvas",
        "title": "VTOP telemetry pipeline (live)",
        "description": (
            "The telemetry path with a logo at each stage and live numbers on the "
            "arrows. Verify-before-commit is the centre: nothing reaches the "
            "committed count until it is VERIFIED. Built with the core Canvas "
            "panel - the flowchart plugin is AngularJS and does not load on "
            "Grafana 13."
        ),
        "datasource": MIMIR,
        "gridPos": {"h": 18, "w": 24, "x": 0, "y": 1},
        "targets": [
            {
                "datasource": MIMIR,
                "refId": chr(65 + i),
                "expr": expr,
                "instant": True,
                "legendFormat": field,
            }
            for i, (field, expr) in enumerate(TARGETS)
        ],
        "options": {
            "inlineEditing": True,
            "showAdvancedTypes": True,
            "panZoom": False,
            "root": {"elements": elements, "background": {"color": {"fixed": "transparent"}}},
        },
    }


pipeline = {
    "uid": "vtop-pipeline-flow",
    "title": "VTOP — Pipeline flow (live)",
    "description": "SCADA-style live diagram of the telemetry path, built with Canvas.",
    "tags": ["vtop", "pipeline", "flow"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": [
        {"type": "row", "title": "Live pipeline", "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0}},
        _panel(),
    ],
}

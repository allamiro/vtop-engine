"""VTOP-engine dashboards, built on the engine's OWN Prometheus metrics.

Imported by build-dashboards.py. Kept separate because these are the dashboards
that matter: every other component (Kafka, MinIO) is supporting cast, and the
engine is the thing whose correctness the whole protocol is about.

All queries here are PromQL against Mimir, scraped from the engine's /metrics
endpoint (issue #46). Nothing is reconstructed from logs.

Conventions:
  * rates are computed with rate()/increase(), never exported as gauges;
  * latency is asked as p95/p99 from histograms, because an average hides the
    tail that actually pages someone;
  * $tenant/$source_type/$format template variables come from the metric labels,
    which are deliberately low-cardinality;
  * failure counters use `or vector(0)`. Prometheus only creates a series on the
    FIRST increment, so a healthy system would otherwise render "No data" on the
    incident panels - visually identical to a broken scrape. An incident counter
    must read 0, not blank.
"""

MIMIR = {"type": "prometheus", "uid": "mimir"}
LOKI = {"type": "loki", "uid": "loki"}

# Every panel filters by the template variables so one dashboard serves all
# tenants/sources rather than needing a copy per deployment.
SEL = '{tenant=~"$tenant", source_type=~"$source_type", format=~"$format"}'


def _q(expr, legend=""):
    return [{"datasource": MIMIR, "expr": expr, "legendFormat": legend}]


def _panel(title, gp, targets, unit=None, kind="timeseries", desc="", ds=MIMIR, extra=None):
    p = {
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": ds,
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


def _stat(title, gp, targets, desc="", unit=None, thresholds=None, ds=MIMIR):
    p = _panel(title, gp, targets, unit=unit, kind="stat", desc=desc, ds=ds)
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNonNull"], "fields": "", "values": False},
        "colorMode": "background" if thresholds else "value",
        "graphMode": "area",
        "textMode": "value",
        "justifyMode": "center",
    }
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    return p


def _row(title, y):
    return {"type": "row", "title": title, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "collapsed": False, "panels": []}


def _templating():
    def var(name, label):
        return {
            "name": name,
            "label": label,
            "type": "query",
            "datasource": MIMIR,
            # Sourced from batches_total, NOT commits_total: a stream that has
            # never successfully committed - precisely the one being debugged -
            # would otherwise be unselectable, because its label values would
            # not exist on a commit counter.
            "query": {"query": f"label_values(vtop_batches_total, {name})", "refId": name},
            "refresh": 2,
            "includeAll": True,
            "allValue": ".*",
            "multi": True,
            "current": {"text": "All", "value": "$__all"},
        }
    return {"list": [var("tenant", "Tenant"), var("source_type", "Source"), var("format", "Format")]}


GREEN_IS_GOOD = [{"color": "red", "value": None}, {"color": "green", "value": 1}]
RED_ON_ANY = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
AMBER_ON_ANY = [{"color": "green", "value": None}, {"color": "orange", "value": 1}]


def dash(uid, title, desc, panels, tags):
    return {
        "uid": uid, "title": title, "description": desc, "tags": tags,
        "timezone": "browser", "schemaVersion": 39, "version": 1,
        "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
        "templating": _templating(), "panels": panels,
    }


# ===========================================================================
# VTOP ENGINE — the single dashboard for the engine itself
# ===========================================================================
engine = dash(
    "vtop-engine", "VTOP Engine",
    "The engine itself, from its own Prometheus metrics (/metrics). Everything "
    "here is measured by VTOP, not inferred from Kafka or MinIO. Start here.",
    [
        # ---- Health line -------------------------------------------------
        _row("Is the engine healthy?", 0),
        _stat("Engine up", {"h": 4, "w": 4, "x": 0, "y": 1},
              _q('up{job="vtop-engine"}', "up"),
              thresholds=GREEN_IS_GOOD,
              desc="Scrape health. If this is 0 every panel below is stale — "
                   "check the engine and VTOP_METRICS_ADDR before believing "
                   "anything else."),
        _stat("Verification failures (1h)", {"h": 4, "w": 5, "x": 4, "y": 1},
              _q(f'sum(increase(vtop_verification_failures_total{SEL}[1h])) or vector(0)', "failures"),
              thresholds=RED_ON_ANY,
              desc="ANY non-zero value is an incident: an uploaded object did "
                   "not match its manifest. The engine refuses to commit, so no "
                   "data is lost - but the batch is stuck and needs a human."),
        _stat("Batches failed (1h)", {"h": 4, "w": 5, "x": 9, "y": 1},
              _q(f'sum(increase(vtop_failed_total{SEL}[1h])) or vector(0)', "failed"),
              thresholds=RED_ON_ANY,
              desc="Batches that hit FAILED for any reason (compression, "
                   "upload, verification). Source progress was never advanced, "
                   "so they are replayable."),
        _stat("Replays required (1h)", {"h": 4, "w": 5, "x": 14, "y": 1},
              _q(f'sum(increase(vtop_replay_required_total{SEL}[1h])) or vector(0)', "replays"),
              thresholds=AMBER_ON_ANY,
              desc="Batches re-read from source after a failure. Safe by "
                   "design, but a sustained rate means work is being repeated."),
        _stat("In-flight batches", {"h": 4, "w": 5, "x": 19, "y": 1},
              _q('vtop_inflight_batches or vector(0)', "in-flight"),
              desc="Accumulated but not yet sealed. Climbing without bound "
                   "means sealing has stalled; a steady sawtooth is just the "
                   "max_batch_age timer doing its job."),

        # ---- The invariant ------------------------------------------------
        _row("The core rule: SOURCE_COMMITTED is forbidden until VERIFIED", 5),
        _panel("Verified vs committed (committed must never exceed verified)",
               {"h": 8, "w": 12, "x": 0, "y": 6},
               [
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_verified_total{SEL}[5m]))', "legendFormat": "verified/sec"},
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_commits_total{SEL}[5m]))', "legendFormat": "committed/sec"},
               ],
               desc="The protocol's whole promise, made measurable. Commits "
                    "happen only after verification, so the committed line must "
                    "never rise above the verified line. If it does, the "
                    "guarantee is broken - that is a P0, not a graph anomaly."),
        _panel("Invariant margin: verified - committed (must stay >= 0)",
               {"h": 8, "w": 12, "x": 12, "y": 6},
               _q(f'sum(vtop_verified_total{SEL}) - sum(vtop_commits_total{SEL})', "verified - committed"),
               desc="The same rule as a single number. >= 0 always. Small "
                    "positive values are normal (a batch verified but not yet "
                    "committed). A NEGATIVE value means a commit happened "
                    "without a verification."),

        # ---- Funnel --------------------------------------------------------
        _row("Pipeline funnel", 14),
        _panel("Batches entering each state (per sec)", {"h": 8, "w": 12, "x": 0, "y": 15},
               _q(f'sum by (state) (rate(vtop_batches_total{SEL}[5m]))', "{{state}}"),
               desc="Where batches stop. verified and source_committed should "
                    "track each other; a gap that persists means batches are "
                    "verifying but not committing."),
        _panel("Verification strength", {"h": 8, "w": 12, "x": 12, "y": 15},
               [
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_verified_total{SEL}[5m]))', "legendFormat": "verified (total)"},
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_verification_backend_limited_total{SEL}[5m]))', "legendFormat": "backend-limited (size only)"},
               ],
               desc="Backend-limited means verified by SIZE/EXISTENCE only, "
                    "with no checksum - a weaker guarantee than the protocol "
                    "intends. In production set "
                    "upload.require_strong_verification: true so these are "
                    "refused rather than committed."),

        # ---- Throughput -----------------------------------------------------
        _row("Throughput", 23),
        _panel("Records/sec", {"h": 7, "w": 8, "x": 0, "y": 24},
               _q(f'sum by (format) (rate(vtop_records_total{SEL}[5m]))', "{{format}}"),
               desc="Derived from a counter with rate(), not exported as a "
                    "gauge - a gauge of a rate lies between scrapes."),
        _panel("Bytes in vs out", {"h": 7, "w": 8, "x": 8, "y": 24},
               [
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_bytes_in_total{SEL}[5m]))', "legendFormat": "uncompressed in"},
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_bytes_out_total{SEL}[5m]))', "legendFormat": "compressed out (on the wire)"},
               ], unit="Bps",
               desc="The gap between the lines is what compression saves on the "
                    "wire - and upload is the documented bottleneck, so this is "
                    "the number that matters for throughput."),
        _panel("Compression ratio (p50 / p95)", {"h": 7, "w": 8, "x": 16, "y": 24},
               [
                   {"datasource": MIMIR, "expr": f'histogram_quantile(0.5, sum by (le) (rate(vtop_compression_ratio_bucket{SEL}[5m])))', "legendFormat": "p50"},
                   {"datasource": MIMIR, "expr": f'histogram_quantile(0.95, sum by (le) (rate(vtop_compression_ratio_bucket{SEL}[5m])))', "legendFormat": "p95"},
               ],
               desc="uncompressed/compressed; higher is better. A collapse "
                    "toward 1.0 means the data shape changed or compression is "
                    "effectively off."),

        # ---- Latency --------------------------------------------------------
        _row("Latency (histograms, so the tail is visible)", 31),
        _panel("Per-stage p95", {"h": 8, "w": 12, "x": 0, "y": 32},
               _q(f'histogram_quantile(0.95, sum by (le, stage) (rate(vtop_stage_duration_seconds_bucket{SEL}[5m])))', "{{stage}}"),
               unit="s",
               desc="Which stage owns the time. object_upload usually "
                    "dominates - the state store is NOT the bottleneck, which "
                    "is why the HA plan puts effort into uploads rather than "
                    "the database."),
        _panel("End-to-end batch latency (p50/p95/p99)", {"h": 8, "w": 12, "x": 12, "y": 32},
               [
                   {"datasource": MIMIR, "expr": f'histogram_quantile(0.5, sum by (le) (rate(vtop_batch_duration_seconds_bucket{SEL}[5m])))', "legendFormat": "p50"},
                   {"datasource": MIMIR, "expr": f'histogram_quantile(0.95, sum by (le) (rate(vtop_batch_duration_seconds_bucket{SEL}[5m])))', "legendFormat": "p95"},
                   {"datasource": MIMIR, "expr": f'histogram_quantile(0.99, sum by (le) (rate(vtop_batch_duration_seconds_bucket{SEL}[5m])))', "legendFormat": "p99"},
               ], unit="s",
               desc="Batch start to source-committed. NOTE: batches seal on "
                    "max_batch_age (60s by default), so on an idle lab this "
                    "measures the timer, not slowness."),

        # ---- Sources ---------------------------------------------------------
        _row("Sources", 40),
        _panel("Source read errors/sec", {"h": 7, "w": 12, "x": 0, "y": 41},
               _q('sum by (source_type) (rate(vtop_source_read_errors_total'
                  '{tenant=~"$tenant", source_type=~"$source_type"}[5m]))',
                  "{{source_type}}"),
               desc="A failed read is skipped and retried next cycle, so it is "
                    "survivable - but a steady rate means a source is unhealthy "
                    "and nobody would otherwise notice. Not labelled by source "
                    "name: file/syslog names are full paths and a rotated file "
                    "set would mint a series per file - the path is in the log "
                    "panel beside this one. (No $format filter: a read fails "
                    "before the format is known.)"),
        _panel("Recent engine warnings and errors", {"h": 7, "w": 12, "x": 12, "y": 41},
               [{"datasource": LOKI, "expr": '{service="vtop-engine"} | json | level=~"WARN|ERROR"', "queryType": "range"}],
               ds=LOKI, kind="logs",
               extra={"options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}},
               desc="Logs carry the high-cardinality detail (batch_id, object "
                    "URI) that must never be a metric label. When a stat above "
                    "goes red, the reason is here."),
    ],
    ["vtop", "engine"],
)

"""VTOP — Kafka source dashboard.

Imported by build-dashboards.py.

WHERE THESE NUMBERS COME FROM (checked, not assumed)
----------------------------------------------------
`kafka-exporter` talks to the broker over the Kafka protocol and exposes
**offsets**, not bytes:

    kafka_topic_partition_current_offset / _oldest_offset
    kafka_consumergroup_current_offset / _lag

So message rates here are derived from offset deltas — accurate, because an
offset increments once per message.

**Bytes/sec comes from the ENGINE's own counters**, not from Kafka. That is
deliberate and it is the more honest number for this dashboard: broker-level
`BytesInPerSec` lives only in JMX, which this lab does not scrape, and what
actually matters for VTOP is the volume it *ingested and archived* — which the
engine measures directly, split by whether it was compressed on the way out.

If broker-level byte counters are ever needed (e.g. to compare produced vs
consumed bytes), that requires adding a JMX exporter alongside the broker; it is
not obtainable from kafka-exporter.
"""

MIMIR = {"type": "prometheus", "uid": "mimir"}

GROUP = 'consumergroup="vtop-engine"'
# Engine-side Kafka volume. source_type is a bounded label, so this is cheap.
ENG_KAFKA = '{source_type="kafka"}'


def _q(expr, legend=""):
    return [{"datasource": MIMIR, "expr": expr, "legendFormat": legend}]


def _panel(title, gp, targets, unit=None, kind="timeseries", desc=""):
    return {
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": MIMIR,
        "gridPos": gp,
        "targets": targets,
        "fieldConfig": {
            "defaults": {"custom": {"fillOpacity": 10}, **({"unit": unit} if unit else {})},
            "overrides": [],
        },
        "options": {},
    }


def _stat(title, gp, targets, desc="", unit=None, thresholds=None):
    p = _panel(title, gp, targets, unit=unit, kind="stat", desc=desc)
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNonNull"], "fields": "", "values": False},
        "colorMode": "background" if thresholds else "value",
        # No sparkline and an explicit value size: these tiles are only 4
        # rows tall, and with graphMode "area" competing for the space
        # Grafana auto-shrank the number until it vanished - the tile
        # rendered as a bare colour and the value reappeared only in the
        # (much larger) edit view.
        "graphMode": "none",
        "text": {"valueSize": 22},
        "textMode": "value",
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


def _row(title, y):
    return {"type": "row", "title": title, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "collapsed": False, "panels": []}


OK_ZERO = [{"color": "green", "value": None}, {"color": "orange", "value": 1000}, {"color": "red", "value": 100000}]

kafka = {
    "uid": "vtop-kafka",
    "title": "VTOP — Kafka source",
    "description": (
        "Kafka throughput and lag. Message rates come from kafka-exporter offset "
        "deltas; BYTES come from the engine's own counters, because "
        "kafka-exporter exposes offsets only and broker BytesInPerSec lives in "
        "JMX. If every panel is empty, check the target in the Alloy UI (:12345) "
        "- an absent exporter looks exactly like zero traffic."
    ),
    "tags": ["vtop", "kafka"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "time": {"from": "now-30m", "to": "now"},
    "panels": [
        # ---- Headline numbers ------------------------------------------------
        _row("Throughput at a glance", 0),
        _stat("Exporter up", {"h": 4, "w": 3, "x": 0, "y": 1},
              _q('up{job="kafka"}', "up"),
              thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
              desc="kafka-exporter scrape health. If this is 0, every Kafka "
                   "metric below is STALE - and 0 lag would then be a lie, not a "
                   "drained group. Check this before trusting anything else."),
        _stat("Messages/sec (produced)", {"h": 4, "w": 4, "x": 3, "y": 1},
              _q('sum(rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m])) or vector(0)', "msg/s"),
              desc="Offsets advance once per message, so the rate of the "
                   "current offset IS the produce rate. Excludes internal "
                   "__consumer_offsets."),
        _stat("Messages/sec (archived by VTOP)", {"h": 4, "w": 4, "x": 7, "y": 1},
              _q(f'sum(rate(vtop_records_total{ENG_KAFKA}[1m])) or vector(0)', "rec/s"),
              desc="What the engine actually committed. Persistently below the "
                   "produce rate means VTOP is falling behind - watch lag."),
        _stat("Bytes/sec in (uncompressed)", {"h": 4, "w": 4, "x": 11, "y": 1},
              _q(f'sum(rate(vtop_bytes_in_total{ENG_KAFKA}[1m])) or vector(0)', "B/s"),
              unit="Bps",
              desc="Kafka payload bytes read into batches, measured by the "
                   "engine. NOT from kafka-exporter, which exposes offsets only."),
        _stat("Bytes/sec out (compressed)", {"h": 4, "w": 4, "x": 15, "y": 1},
              _q(f'sum(rate(vtop_bytes_out_total{ENG_KAFKA}[1m])) or vector(0)', "B/s"),
              unit="Bps",
              desc="Bytes actually written to object storage. The gap to "
                   "bytes-in is what compression saves on the wire."),
        _stat("Total lag", {"h": 4, "w": 5, "x": 19, "y": 1},
              _q(f'sum(kafka_consumergroup_lag{{{GROUP}}}) or vector(0)', "lag"),
              thresholds=OK_ZERO,
              desc="Records produced but not yet COMMITTED by VTOP. Offsets "
                   "advance only after VERIFIED, so lag returning to 0 is proof "
                   "the whole verified path is keeping up."),

        # ---- Rates over time --------------------------------------------------
        _row("Rates", 5),
        _panel("Messages/sec by topic", {"h": 8, "w": 12, "x": 0, "y": 6},
               _q('sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m]))',
                  "{{topic}}"),
               desc="Produce rate per topic, from offset deltas."),
        _panel("Produced vs archived (messages/sec)", {"h": 8, "w": 12, "x": 12, "y": 6},
               [
                   {"datasource": MIMIR,
                    "expr": 'sum(rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m]))',
                    "legendFormat": "produced into Kafka"},
                   {"datasource": MIMIR,
                    "expr": f'sum(rate(vtop_records_total{ENG_KAFKA}[1m]))',
                    "legendFormat": "archived by VTOP"},
               ],
               desc="The two lines should track. A sustained gap means the "
                    "engine cannot keep up and lag will grow - note reads are "
                    "sequential per topic, so many topics cost wall-clock (see "
                    "the HA plan)."),

        # ---- Bytes -------------------------------------------------------------
        _row("Bytes (measured by the engine, not the broker)", 14),
        _panel("Bytes/sec in vs out", {"h": 8, "w": 12, "x": 0, "y": 15},
               [
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_bytes_in_total{ENG_KAFKA}[1m]))',
                    "legendFormat": "uncompressed in"},
                   {"datasource": MIMIR, "expr": f'sum(rate(vtop_bytes_out_total{ENG_KAFKA}[1m]))',
                    "legendFormat": "compressed out"},
               ], unit="Bps",
               desc="Kafka-sourced volume through the engine. Upload is the "
                    "documented bottleneck, so the 'out' line is the one that "
                    "meets the network."),
        _panel("Bytes/sec by format", {"h": 8, "w": 12, "x": 12, "y": 15},
               _q('sum by (format) (rate(vtop_bytes_in_total{source_type="kafka"}[1m]))', "{{format}}"),
               unit="Bps",
               desc="Which telemetry format dominates the Kafka volume - the "
                    "engine detects this per batch."),

        # ---- Lag ---------------------------------------------------------------
        _row("Lag (the invariant, seen from Kafka)", 23),
        _panel("Lag by topic", {"h": 8, "w": 12, "x": 0, "y": 24},
               _q(f'sum by (topic) (kafka_consumergroup_lag{{{GROUP}}})', "{{topic}}"),
               desc="A topic whose lag only grows is not being drained. VTOP "
                    "commits offsets ONLY after VERIFIED, so lag is the backlog "
                    "of records not yet safely archived."),
        _panel("Lag by partition", {"h": 8, "w": 12, "x": 12, "y": 24},
               _q(f'sum by (topic, partition) (kafka_consumergroup_lag{{{GROUP}}})',
                  "{{topic}}-p{{partition}}"),
               desc="Useful when scaling: engine replicas are bounded by "
                    "partition count, so more replicas than partitions adds "
                    "nothing."),

        # ---- Topics ------------------------------------------------------------
        _row("Topics and retention", 32),
        _panel("Messages retained per topic", {"h": 7, "w": 8, "x": 0, "y": 33},
               _q('sum by (topic) (kafka_topic_partition_current_offset{topic!~"__.*"} '
                  '- kafka_topic_partition_oldest_offset{topic!~"__.*"})', "{{topic}}"),
               desc="current - oldest offset = messages still on the broker. "
                    "This is the replay window: VTOP can only re-read what "
                    "Kafka still holds."),
        _panel("Partitions per topic", {"h": 7, "w": 8, "x": 8, "y": 33},
               _q('count by (topic) (kafka_topic_partition_current_offset{topic!~"__.*"})',
                  "{{topic}}"),
               desc="The ceiling on useful engine replicas for that topic."),
        _panel("Committed offset per topic", {"h": 7, "w": 8, "x": 16, "y": 33},
               _q(f'sum by (topic) (kafka_consumergroup_current_offset{{{GROUP}}})', "{{topic}}"),
               desc="How far VTOP has safely committed. This number only ever "
                    "advances after a batch is VERIFIED."),
    ],
}

# VTOP pipeline flow — dashboard design

A SCADA-style live pipeline diagram: the telemetry path drawn left-to-right with
the technology logo at each stage and **live numbers flowing along the arrows**.

## Why Canvas, not the flowchart plugin

The Factry article uses `agenty-flowcharting-panel`. That plugin is an **AngularJS
plugin, and Grafana 11+ removed AngularJS support entirely** — on Grafana 13.1 it
installs but never loads (confirmed: absent from `/api/plugins`). Grafana's own
successor for "diagram with live values" is the **Canvas panel**, which is built
into core, needs no plugin, and does exactly this: a background/free layout with
icon elements (the logos) and metric elements (text + color bound to a query).

So the design below is built with Canvas. It is the modern, supported equivalent
of the flowchart plugin, not a downgrade.

## ASCII design (this is what the Canvas panel renders)

```
        SOURCES                     ENGINE  (verify before commit)                 ARCHIVE
   ┌──────────────┐          ┌───────────────────────────────────────┐        ┌──────────────┐
   │   Kafka      │  142/s   │  seal → compress → checksum → upload   │ 48KB/s │   MinIO /    │
   │   [logo]     │ ───────▶ │           [Rust logo]                  │ ─────▶ │   S3         │
   │   Files      │  bytes   │                                        │ object │   [logo]     │
   │   Syslog     │  in →     │   ratio 5.0x    p95 12ms               │  +      │              │
   └──────────────┘           └──────────────────┬────────────────────┘ manifest└──────┬───────┘
        [kafka]                                   │  verify size+checksum               │
        [files]                                   │                                     │
        [syslog]                        ┌─────────▼─────────┐                           │
                                        │   VERIFIED?       │◀──────────────────────────┘
                                        │   ✓ commits=52    │      2 · read back + verify
                                        │   ✗ failed=0      │
                                        └─────────┬─────────┘
                                                  │  3 · commit ONLY after VERIFIED
                                                  ▼
                                        ┌───────────────────┐
                                        │   State store     │   lag 8998 ▼ (draining)
                                        │   [SQLite logo]    │   in-flight 4
                                        └───────────────────┘
```

Numbers that animate live on the diagram:

| Location on the diagram | Metric | Query |
|---|---|---|
| Source → Engine arrow | messages/sec produced | `sum(rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m]))` |
| Source → Engine arrow | bytes/sec in | `sum(rate(vtop_bytes_in_total[1m]))` |
| Inside Engine | compression ratio | `histogram_quantile(0.5, …vtop_compression_ratio_bucket…)` |
| Inside Engine | stage p95 | `histogram_quantile(0.95, …vtop_stage_duration_seconds_bucket…)` |
| Engine → MinIO arrow | bytes/sec out | `sum(rate(vtop_bytes_out_total[1m]))` |
| VERIFIED node | commits / failed | `sum(vtop_commits_total)` / `sum(increase(vtop_failed_total[1h]))` |
| State store node | lag / in-flight | `sum(kafka_consumergroup_lag)` / `vtop_inflight_batches` |

Colour rules (the SCADA part):
- the **VERIFIED node** is green while `failed == 0`, red the moment a batch fails;
- the **lag** number is green at 0, amber as it climbs, red when large;
- an idle stage shows `0`, never blank — every metric uses `or vector(0)` so a
  scrape gap cannot make a live panel look broken.

## Logos

Downloaded as SVG into `observability/grafana/assets/` and mounted into the
Grafana container at `/public/img/vtop/`, so the Canvas icons reference stable
local URLs (`public/img/vtop/rust.svg`, …) rather than a CDN — the diagram must
render even with no internet.

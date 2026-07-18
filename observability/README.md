# VTOP observability stack (optional)

Grafana **Alloy** (collect) → **Mimir** (metrics) / **Loki** (logs) / **Tempo**
(traces), with **Grafana** on top. Entirely optional: the lab runs fine without
it.

## Bring it up

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

| UI | URL | Login |
|---|---|---|
| **Grafana** | http://localhost:3400 | `admin` / `admin` (anonymous viewing enabled) |
| **Alloy** (which targets are actually UP) | http://localhost:12345 | — |

Datasources are **provisioned as code**; dashboards are **seeded via the API**
(so they stay editable — see [Editing dashboards](#editing-dashboards)). Nothing
to click either way.

> Ports are deliberately off the defaults (3400, not 3000): 3000/8080 collide
> with almost every other local stack.

## Dashboards

Start with **VTOP Engine** — it is the only one about the thing that matters.

| Dashboard | Answers |
|---|---|
| **VTOP Engine** | Is the engine healthy, is the invariant holding, where is time going? Built entirely on the engine's **own** metrics. |
| VTOP — Safety & invariant | Verification failures, backend-limited verifications, replays, recovery. |
| VTOP — Overview | One screen: is data flowing end to end? |
| VTOP — Pipeline performance | Throughput, compression, per-stage latency. |
| VTOP — Pipeline flow (live) | SCADA-style diagram of the whole path with live numbers on the arrows, built with the core **Canvas** panel. |
| VTOP — Pipeline flow (draw.io) | The same path as a draw.io SVG driven by the **Flow** panel (`andrewbmchugh-flow-panel`) — the maintained, React successor to the AngularJS flowcharting plugin (AngularJS is disabled by default in Grafana 11 and removed in Grafana 12+, so the old plugin will not load on the lab's Grafana 13). Auto-installed via `GF_INSTALL_PLUGINS`. |
| VTOP — Kafka source | Consumer lag (the invariant seen from Kafka), topics. |
| VTOP — Object storage | Objects/bytes stored, S3 request and error rates. |
| VTOP — Logs | Raw structured events; where the high-cardinality detail lives. |

## What each component actually exposes

This mattered enough to check rather than assume:

| Component | Metrics | Logs | Traces |
|---|---|---|---|
| **VTOP engine** | ✅ **native Prometheus** (`/metrics`, opt-in via `VTOP_METRICS_ADDR`) | ✅ structured events | ⏳ wired, not emitted yet |
| MinIO | ✅ native Prometheus | ✅ | — |
| Kafka | ⚠️ JMX only → `kafka-exporter` sidecar translates it | ✅ | — |
| rsyslog | ⚠️ `impstats` only | ✅ | — |

The engine used to be the **only** component with no metrics — the one whose
correctness the whole protocol is about. That is what issue #46 fixed.

## The engine's metrics

Exposed at `VTOP_METRICS_ADDR` (the lab sets `0.0.0.0:9090`). Nothing listens
unless it is set.

```
GET /metrics   Prometheus text format
GET /healthz   liveness
GET /readyz    readiness
```

| Metric | Type | Why it exists |
|---|---|---|
| `vtop_batches_total{state}` | counter | The pipeline funnel: where batches stop |
| `vtop_verified_total` | counter | Batches that passed verification |
| `vtop_commits_total` | counter | **Must never exceed `verified_total`** — the invariant, measurable |
| `vtop_verification_failures_total` | counter | **Any non-zero value is an incident** |
| `vtop_verification_backend_limited_total` | counter | Verified by size only, no checksum |
| `vtop_replay_required_total` | counter | Work being repeated |
| `vtop_failed_total` | counter | Batches that hit FAILED |
| `vtop_records_total`, `vtop_bytes_in_total`, `vtop_bytes_out_total` | counters | Volume; rates via `rate()` |
| `vtop_stage_duration_seconds{stage}` | histogram | Per-stage latency, p95-able |
| `vtop_batch_duration_seconds` | histogram | End to end |
| `vtop_compression_ratio` | histogram | Compression effectiveness |
| `vtop_inflight_batches` | gauge | Accumulated but not sealed |
| `vtop_source_read_errors_total` | counter | An unhealthy source, which is otherwise invisible |

### Design decisions worth knowing

- **Labels are bounded**: `tenant`, `source_type`, `format`, `stage`, `state` —
  all small closed sets. `batch_id` and object URIs are **never** labels; they
  are unbounded and would grow the TSDB without limit. They live in logs, which
  is what logs are for. A test enforces this.
- **Rates are derived, not exported.** A gauge holding a rate is a snapshot that
  lies between scrapes, so `records_per_sec` is computed in PromQL from a
  counter.
- **Latency is a histogram**, so p95/p99 are answerable. An average hides the
  tail that actually pages someone.
- **Incident panels use `or vector(0)`.** Prometheus only creates a series on the
  first increment, so a healthy system would otherwise render "No data" — visually
  identical to a broken scrape. An incident counter must read `0`, not blank.
- **Telemetry can never break the data path.** A bad `VTOP_METRICS_ADDR`, a port
  clash, or a registry failure logs an error and the engine keeps archiving.

## Editing dashboards

Dashboards are **seeded through the Grafana API**, not file-provisioned, so they
are ordinary dashboards you can edit and **Save** in the UI.

> Grafana 13 makes a *file-provisioned* dashboard read-only: pressing Save is
> refused with *"Cannot save provisioned dashboard"*, and `allowUiUpdates: true`
> no longer changes that (it was honoured by the legacy provisioning path). A lab
> you cannot poke at is not much of a lab, hence API seeding.

The generated JSON under `observability/grafana/dashboards/` remains the source
of truth for what ships, and CI fails if it drifts from the generators.

```bash
# Reset the lab's dashboards back to the repo version (discards UI edits):
docker compose -f docker-compose.yml -f docker-compose.observability.yml \
    up --force-recreate grafana-seed
```

To make a UI change permanent: edit in the UI, export the JSON, fold the change
into the generator in `observability/`, then regenerate.


Dashboard JSON is unreviewable by hand, so the **queries** are written in Python
and the JSON is generated:

```bash
python3 observability/build-dashboards.py     # regenerate all dashboards
```

- `observability/dashboards_vtop.py` — the VTOP Engine dashboard
- `observability/build-dashboards.py` — the rest

`crates/vtop-cli/tests/integration_metrics_endpoint.rs` pins the metric names the
dashboards query: renaming one **fails the test** instead of silently blanking a
panel.

## Scope: this is a lab, not production

- single-node Mimir/Loki/Tempo on filesystem storage, no retention tuning;
- lab credentials, anonymous Grafana viewing;
- `MINIO_PROMETHEUS_AUTH_TYPE=public` disables auth on MinIO's metrics endpoint —
  **never do that outside a lab**;
- Alloy holds a (read-only) docker socket to discover containers and read logs;
- no alerting rules yet.

See [`docs/PRODUCTION_HA_PLAN.md`](../docs/PRODUCTION_HA_PLAN.md) for what a
production topology requires.

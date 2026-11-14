# Metrics and alerts

The metrics follow the batch lifecycle on [How it works](../concepts/how-it-works.md):
per-stage timings for its steps, and counters for the ways they fail.

## Turn on the endpoint

The engine serves Prometheus metrics only when `VTOP_METRICS_ADDR` is set:

```bash
VTOP_METRICS_ADDR=127.0.0.1:9090 vtopctl run --config config.yaml
```

| Path | Answers |
|---|---|
| `GET /metrics` | Prometheus text format, every series prefixed `vtop_`. |
| `GET /healthz` | Liveness: `200` while the process serves requests. |
| `GET /readyz` | Readiness: `200 ready`, or `503 not ready: <reason>`. |

Only `vtopctl run` serves it; one-shot commands such as `process-once` and
`replay` do not. A bad address or a port clash is logged and the engine keeps
archiving: telemetry never stops the data path.

The endpoint is **unauthenticated**. Bind it to loopback or a management
network. It accepts at most 16 connections, each with a 10-second deadline.
For TLS, set `VTOP_METRICS_TLS_CERT` and `VTOP_METRICS_TLS_KEY`, and add
`VTOP_METRICS_TLS_CLIENT_CA` to require client certificates.

Cluster nodes (`vtop-node`) serve the same three paths from the
`observability.listen` address in their config; their metrics and readiness
rules are in [Node endpoints and metrics](../NODE_OBSERVABILITY.md).

## The engine metrics to watch

The full list, with the reason each one exists, is in the
[observability README](../../observability/README.md#the-engines-metrics).
These carry the commit rule:

| Metric | Read it as |
|---|---|
| `vtop_verification_failures_total` | Any increase is an incident. |
| `vtop_commits_total` vs `vtop_verified_total` | Commits must never exceed verifications. |
| `vtop_verification_mode{mode="size_only"}` | `1` means `upload.require_strong_verification` is off: same-size corruption is accepted. |
| `vtop_verification_backend_limited_total` | Batches committed on a size-only check. |
| `vtop_retention_lost_records_total` | Kafka records deleted by broker retention before the engine read them. Never archived; any increase is an incident. |
| `vtop_source_replay_refused_total` | A failed range its source could not hand back, so it is missing from the archive. |
| `vtop_source_read_errors_total` | A source that cannot be read. |
| `vtop_replay_required_total`, `vtop_failed_total` | Work repeated, batches failed. |
| `vtop_stage_duration_seconds{stage}`, `vtop_batch_duration_seconds` | Where time goes, per stage and end to end. |
| `vtop_upload_throttled_total`, `vtop_upload_width` | The object store asking for less concurrency, and the width in flight. |

Counters are per process: a restart starts them again from zero. Labels are
bounded (`tenant`, `source_type`, `format`, `stage`, `state`); batch ids and
object URIs are in the logs, not in labels.

## Alerts

Two alert sets ship with the repository. Both are starting points, not a
complete production rule set.

**Provisioned in the lab.**
[`observability/grafana/provisioning/alerting/vtop-alerts.yaml`](../../observability/grafana/provisioning/alerting/vtop-alerts.yaml)
holds the Grafana rules the optional observability stack loads:

| Rule | Fires on |
|---|---|
| Records lost to retention | Any increase of `vtop_retention_lost_records_total` |
| Range retention blocked | `vtop_broker_retention_blocked` for 15 minutes |
| Range retention failing | `vtop_broker_retention_last_pass_failed` for 10 minutes |
| Follower disconnected | `vtop_broker_follower_connected` at 0 for 2 minutes |
| Follower lag growing | Rising `vtop_broker_follower_lag_records` for 15 minutes |
| Fencing epoch churn | A range's fencing epoch rising within 30 minutes |
| Promotion boundary stuck | `vtop_broker_boundary_pending` for 2 minutes |
| Metadata leader lost its quorum | `vtop_meta_raft_millis_since_quorum_ack` on the leader past its threshold, for 1 minute |
| Consumer group cursor below log start | A group's committed cursor below the serving replica's log start, for 5 minutes |
| Consumer group near eviction | A group's cursor close to log start, for 15 minutes |

The first rule is about the engine; the rest are about the cluster plane.

**For the scrub.** `vtopctl scrub` writes its own metrics to a file, and the
[observability README](../../observability/README.md#archive-scrub) has an
example Prometheus rule set for it. See [Scrub and coverage audit](scrub-and-audit.md).

Worth adding for any engine (examples; tune them to your deployment):

```yaml
groups:
  - name: vtop-engine
    rules:
      # The series appears at the first failure, so increase() would miss
      # that one: alert on the value. It clears when the engine restarts.
      - alert: VtopVerificationFailures
        expr: sum(vtop_verification_failures_total) > 0
      - alert: VtopSizeOnlyVerification
        expr: vtop_verification_mode{mode="size_only"} == 1
      # Primed at zero at startup, so increase() sees the first one.
      - alert: VtopSourceReplayRefused
        expr: increase(vtop_source_replay_refused_total[15m]) > 0
```

## The lab's Grafana stack

The repository includes an optional Grafana, Alloy, Mimir, Loki and Tempo
stack with provisioned dashboards, for the Docker lab:

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

Grafana is then on `http://localhost:3400`. It is a lab: it uses default
credentials and mounts the Docker socket. The dashboards and how to edit them
are in the [observability README](../../observability/README.md).

## Logs

Logs go to stderr. `VTOP_LOG_FORMAT=json` (or `--json`) writes one JSON
object per line, which is what a log pipeline wants. `RUST_LOG` sets the
filter and overrides `--log-level` and `engine.log_level`.

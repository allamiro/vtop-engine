# Logs and auditing

Metrics show trends; logs explain what the engine decided; audit evidence
shows who changed state. This page covers what exists today and what is
planned.

## Logs

Logs go to stderr.

| Setting | Effect |
|---|---|
| `VTOP_LOG_FORMAT=json`, or `--json` | One JSON object per line, for a log pipeline. Otherwise human-readable text. |
| `RUST_LOG` | The log filter. Wins over `--log-level`, which wins over `engine.log_level` (default `INFO`). |

Each step of a batch logs an event with its `batch_id`: `format_detected`,
`object_uploaded`, `manifest_uploaded`, `verification_passed` and
`source_committed`, then `batch_metrics` with sizes and per-stage timings. A
failure logs with the batch id beside it, for example
`source_commit_failed (will retry on recovery)`.

How to read the levels:

| Level | Means |
|---|---|
| `INFO` | A normal state change: startup settings, a batch committed, recovery's summary. |
| `WARN` | Degraded but safe. The engine refused, held back, retried or fell back, and no data is at risk. |
| `ERROR` | Data is at risk, or you must act: a batch failed verification, or a range cannot be read again. |

Batch ids, paths, topics and offsets appear in log fields, never in metric
labels, so the logs are where you find *which* batch a counter counted.

Secrets never appear in logs: credentials, connection strings and keys are
redacted, and for a secret taken from an environment variable only the
variable's name is printed
([security model §11](../SECURITY_MODEL.md#11-secret-redaction)).

The lab's optional stack ships logs with Grafana Alloy into Loki; see
[Metrics and alerts](monitoring.md#the-labs-grafana-stack).

## Audit evidence today

There is no single audit stream yet. The evidence of who did what lives in
three places:

| Where | What it records |
|---|---|
| **The state ledger** | Every batch's state, source range, object and manifest URIs and checksums, with `owner` (the engine's `engine.name`) and `created_at` / `updated_at` timestamps. |
| **The metadata Raft log** (cluster plane) | Every cluster command and promotion. The admin endpoint authorizes each command by the caller's verified mTLS identity, and `meta.adminAuthorization` can restrict cluster-scoped commands to named certificate CNs. |
| **Manifests** | What was archived: each object's source range and checksum, pinned by version under `upload.require_object_versioning`, and MAC'd when `manifest_mac_key_env` or `manifest.mac` is set; with a keyring, each `0.4` manifest names its key. Until #694, the startup line naming the active key id is the record of a key change. |

Commands that change state on purpose, such as `prune-ledger`, `replay`,
`migrate`, `meta enable-features` and `node repair`, log what they did. Keep
those logs with the ledger backups.

## Planned

| Issue | What it adds | Status |
|---|---|---|
| [#694](https://github.com/allamiro/vtop-engine/issues/694) | A single audit stream: a `vtop::audit` log target, an audit row for CLI actions that change the ledger, and `vtopctl audit list`. | Planned, not released. |
| [#683](https://github.com/allamiro/vtop-engine/issues/683) | Built-in OpenTelemetry export: a span per batch and per stage, metrics over OTLP beside Prometheus, and trace ids in JSON logs. Idle unless an OTLP endpoint is set. | Planned, not released. Today metrics are Prometheus only and no spans are exported. |

Check the [roadmap](../ROADMAP.md) before relying on either.

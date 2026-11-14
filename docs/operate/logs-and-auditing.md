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

## The audit trail

Every operator command that changes engine or cluster state writes an attributable
audit record (#694): who did it, when, from where, what changed and how it
ended, on the `vtop::audit` log target (which `RUST_LOG` cannot silence), in
the file `VTOP_AUDIT_LOG` names, and, for the ledger commands, in the state
store's hash-chained `audit_events` table. `prune-ledger`, `replay` and
`migrate` are refused when their attempt cannot be recorded. Read it with
`vtopctl audit list` and check the table with `vtopctl audit verify`; see
[Auditing](auditing.md).

The other evidence stays where it was:

| Where | What it records |
|---|---|
| **The state ledger** | Every batch's state, source range, object and manifest URIs and checksums, with `owner` (the engine's `engine.name`) and `created_at` / `updated_at` timestamps. |
| **The metadata Raft log** (cluster plane) | Every cluster command and promotion. The admin endpoint authorizes each command by the caller's verified mTLS identity, and `meta.adminAuthorization` can restrict cluster-scoped commands to named certificate CNs. |
| **Manifests** | What was archived: each object's source range and checksum, pinned by version under `upload.require_object_versioning`, and MAC'd when `manifest_mac_key_env` or `manifest.mac` is set; with a keyring, each `0.4` manifest names its key, and the startup line names the active key id. |

Batch lifecycle events (seal, upload, verify, commit) are ordinary log lines
with their `batch_id`, not audit records: the engine's own work, not an
operator's command.

## OpenTelemetry

Built-in OpenTelemetry export (#683): a span per batch and per stage, metrics
over OTLP beside Prometheus, and trace ids in JSON logs, idle unless an OTLP
endpoint is set. An audit record written inside an exported span carries its
`trace_id` and `span_id`. See
[observability/README.md](https://github.com/allamiro/vtop-engine/blob/main/observability/README.md#opentelemetry).

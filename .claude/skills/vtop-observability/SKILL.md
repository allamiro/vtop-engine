---
name: vtop-observability
description: How vtop-engine tells operators what happened and who did what. Covers auditing (attributable records of every state-changing command), operator-facing log messages (what happened, what the engine did, what to do), log levels and rate limiting, Prometheus metric naming and bounded labels, redaction, and the built-in OpenTelemetry export (traces, OTLP metrics, log correlation, never blocking the batch path). Use when adding or changing a command that changes state, a log line, a metric, a failure path, recovery or scrub behaviour, or anything about OpenTelemetry, Prometheus, Grafana or alerts.
---

# Observability in vtop-engine

The engine's promise is that it never commits past unverified data. An operator can only trust that promise if every decision the engine takes on their behalf is visible: what it skipped, held back, retried or refused. A silent skip is a bug, even when the data is safe.

Ground truth in the tree:
- Metrics registry: `crates/vtop-core/src/telemetry.rs`. `telemetry::metrics()` returns an `Option`, and code must behave the same when it is `None`.
- `/metrics` server: `crates/vtop-cli/src/metrics_server.rs`, enabled by `VTOP_METRICS_ADDR`.
- Tracing init: `init_tracing` in `crates/vtop-cli/src/commands.rs`, in text or JSON.
- Rate-limited warnings: `warn_rate_limited` in `crates/vtop-meta/src/transport/maybe_tls.rs`.
- The metric names a test pins: `crates/vtop-cli/tests/integration_metrics_endpoint.rs`.

## Auditing: who did what, when, and what changed

Logs explain behaviour, and metrics show trends. **Audit records prove who changed state.** They are a separate obligation, so treat them that way.

Where the evidence lives today:
- **The state ledger:** every batch's state transitions, with `owner` (the engine name) and timestamps.
- **The metadata Raft log:** every cluster command and promotion transition record. The admin endpoint authorizes each command by the caller's verified mTLS identity.
- **Manifests,** pinned by version and optionally MAC'd, as evidence of what was archived.

There is **no single audit stream yet**. Issue #694 adds one: a `vtop::audit` tracing target, an audit row for ledger-mutating CLI actions, and `vtopctl audit list`. Until it lands, any new command that changes state must at least:
1. **Name the actor.** Use the mTLS identity for admin commands. For local CLI actions, use the user and host, plus the engine name.
2. **Record the action, target, redacted parameters and outcome**, including failures and refusals: a refused destructive action is itself audit-worthy.
3. **Log it at INFO or above, never sampled or rate-limited,** with structured fields. When #694 lands, use the `vtop::audit` target.
4. **Keep it durable where the action is durable.** A cluster action's record comes from the applied Raft command, not only the client. A ledger mutation should leave a trace in the ledger.
5. **Be explicit for anything destructive or one-way:** `prune-ledger`, `replay`, `migrate`, `enable-features`, `node repair`, a promotion attestation. Before and after where cheap, and why.

Audit records follow the same redaction rules below. **Never** rate-limit, sample or drop them under load; if one can't be written, the action should fail rather than run unrecorded, where that is safe.

## Operator messages: three parts, every time

Every message about a decision says:
1. **what happened**;
2. **what the engine did about it**;
3. **what the operator should do**, or that nothing is needed.

Identifiers go in structured fields, not only in the text: `batch_id`, `source_type`, `source_name`, `path`, `inode`, `topic`, `partition`, offsets and byte counts.

```rust
tracing::warn!(
    path = %path.display(), inode, appended_bytes,
    "not deleting the file: bytes were appended after the commit; they will be archived on the next pass"
);
```

Levels:
- **INFO:** a normal state change an operator may want to see, such as startup settings, a topic gaining partitions, or recovery's summary.
- **WARN:** degraded but safe. The engine refused, held back, retried or fell back, and no data is at risk. Say why.
- **ERROR:** data is at risk, or the operator must act. For example, a range that can't be handed back and won't be re-read, or a batch that failed verification.

Rules:
- **Never skip silently.** Every `continue`, refusal or fallback on a data path logs, and counts if it can repeat.
- **Rate-limit anything that can repeat in a loop,** using `warn_rate_limited` or an equivalent, and include how many occurrences were suppressed.
- **Test the important messages.** For a refusal or a data-at-risk path, assert the message or its fields, so a later refactor can't silently drop it.
- **Say "batches are unaffected"** when an observability failure (exporter down, metrics write failed) doesn't touch the archive path. Operators otherwise assume the worst.

## Metrics

- **Names:** `vtop_<noun>_total` for counters, `vtop_<noun>` for gauges, `_seconds` and `_bytes` units in the name. Register them in `vtop-core/src/telemetry.rs` and add new names to the endpoint test.
- **Labels must have bounded cardinality:** `tenant`, `source_type`, `format`, `state`, `mode`. **Never** `batch_id`, a path, a key, a topic-partition from user input, or an error string. Those belong in logs and spans.
- **Every new failure or refusal path gets a counter,** so an alert can catch it without parsing logs. #664 and #674's `vtop_recovery_unsupported_manifest_total` are the pattern.
- **Keep counters self-consistent across restarts.** See the comment in `commit_recovered` on counting a verification at recovery, so that `commits_total` never exceeds `verified_total`.

## Redaction (non-negotiable)

Never put any of these in a log line, a metric label, a span attribute or an error message:
- record content;
- credentials, tokens, passwords;
- MAC keys or MAC values;
- connection strings;
- full URLs with query strings or user info;
- `OTEL_EXPORTER_OTLP_HEADERS` values;
- the value of an environment variable named by config (`*_env` keys).

Print the variable's *name*, never its value. When adding a new field, ask whether it could ever carry one of these, and add a test for any field that could.

## OpenTelemetry (issue #683)

This is the design agreed for #683. Check the merged code before relying on a detail.

- **Built in, idle by default.** It is compiled into every binary, with no opt-in Cargo feature. Export starts only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or a config equivalent) is set. With nothing set there is no exporter and no background task, and startup logs one INFO line saying so and naming the variable.
- **Traces.** One `batch` span per batch, with child spans `seal`, `compress`, `checksum`, `upload_object`, `upload_manifest`, `verify` and `commit`. Recovery's per-row decision and scrub's per-batch check get spans too.
  - Attributes: batch id, source type and name, tenant, bytes, record count, checksum algorithm, verification method and result.
  - A failed batch's span has status ERROR with the redacted message.
  - Resource: `service.name` (default `vtop-engine`), `service.version`, `service.instance.id` (the engine name), `host.name`, plus `OTEL_RESOURCE_ATTRIBUTES`.
  - Sampler from `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`.
- **Metrics over OTLP,** alongside Prometheus. The same instruments; Prometheus stays the default and unchanged.
- **Log correlation.** JSON logs carry `trace_id` and `span_id` when tracing is on.
- **Never block the batch path.** The export queue is bounded. A collector that is down or hangs drops spans and counts them. It never slows a batch or grows memory without bound. A test proves this against a collector that accepts the connection and never answers.
- **Exporter health is itself observable:** counters for dropped spans and metrics, export failures and queue length, on `/metrics`.
- **Messages:**
  - startup: one INFO for "disabled (set X)" or "exporting to <host>, sampler Y";
  - collector down: a rate-limited WARN with the error and the count dropped, saying "batches are unaffected";
  - shutdown: "flushed N spans", or "gave up after Xs, N dropped";
  - invalid settings: fail at startup, naming the variable and the accepted values.
- **Security:** export is new network egress. `docs/SECURITY_MODEL.md` records what is sent and where.

## Review checklist

- [ ] Every new command that changes state records actor, action, target, outcome (success, failure, refused), and is never sampled or rate-limited.
- [ ] Every new skip, refusal, fallback or retry logs all three parts, with structured identifiers.
- [ ] Repeating warnings are rate-limited, and the suppressed count is reported.
- [ ] Every new failure path has a bounded-label counter, registered and pinned in the endpoint test.
- [ ] No secret, record content or unbounded value in a log, label or attribute; the risky fields are tested.
- [ ] The important messages are asserted in tests.
- [ ] Observability failures say the archive path is unaffected, and never block it.
- [ ] Docs updated: `observability/README.md` and the operate pages.

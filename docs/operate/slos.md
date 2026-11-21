# Service level objectives

What the shipped alert rules
([`observability/prometheus/vtop-rules.yaml`](../../observability/prometheus/vtop-rules.yaml))
protect, stated as objectives. The targets are **defaults to tune** to your
sources and store, not measured guarantees: each is tied to a metric the engine
or its lab exports, and to the alert that fires when it is missed. Each alert's
section in [Alert runbooks](runbooks.md) says what to do.

| Objective | Target (default) | Window | Metric | Alert |
|---|---|---|---|---|
| Verification failures | Zero tolerated | Any 10 minutes, and a series that first appears non-zero | `vtop_verification_failures_total` | [VtopVerificationFailures](runbooks.md#vtopverificationfailures) (page) |
| Commit freshness | An engine holding data commits a batch at least every 30 minutes | 10 minutes without a commit, held for 20 | `vtop_commits_total`, `vtop_inflight_batches` | [VtopNoCommits](runbooks.md#vtopnocommits) (page) |
| Replay rate | Below 0.01 replays per second per stream, or below three times the stream's own rate over the previous 6 hours | 15 minutes | `vtop_replay_required_total` | [VtopReplayRateSpike](runbooks.md#vtopreplayratespike) (ticket) |
| Source lag | Kafka consumer lag of the engine's group not rising for more than 30 minutes | 15-minute slope, held for 30 | `kafka_consumergroup_lag` (kafka-exporter, not the engine) | [VtopSourceLagGrowing](runbooks.md#vtopsourcelaggrowing) (ticket) |
| Retention loss | Zero records lost to source retention | Any 15 minutes | `vtop_retention_lost_records_total` | [VtopRetentionLoss](runbooks.md#vtopretentionloss) (page) |
| Ranges missing after a failure | Zero ranges a source refuses to hand back | Any 15 minutes, and a series that first appears non-zero | `vtop_source_replay_refused_total` | [VtopSourceReplayRefused](runbooks.md#vtopsourcereplayrefused) (page) |
| Recovery's replay decisions recorded | Zero ranges held because the state store refused the write | From the start that held them until the next start | `vtop_recovery_held_total{reason="ledger_write_failed"}` | [VtopRecoveryLedgerWriteFailed](runbooks.md#vtoprecoveryledgerwritefailed) (page) |
| Retention under Object Lock | Every check `ok` | Any 15 minutes, and a series that first appears non-zero | `vtop_object_lock_checks_total{result}` | [VtopObjectLockCheckFailed](runbooks.md#vtopobjectlockcheckfailed) (page) |
| Locks kept on replay | No replay finds its object without the lock configured, and no fresh object is withheld | Any hour, and a series that first appears non-zero | `vtop_object_key_decisions_total{decision}` (`fallback_unlocked`, `fallback_lock_mode`, `fallback_hold_missing`, `lock_withheld`) | [VtopObjectLockFallback](runbooks.md#vtopobjectlockfallback) (ticket) |
| Strong verification | On for every production engine | At startup | `vtop_verification_mode{mode}` | [VtopSizeOnlyVerification](runbooks.md#vtopsizeonlyverification) (ticket) |
| Versioned buckets written to | No bucket's writes paused because it stopped keeping object versions (`upload.require_object_versioning`) | Held for 5 minutes (the engine rechecks at most every 60 s) | `vtop_bucket_versioning_paused` | [VtopBucketVersioningPaused](runbooks.md#vtopbucketversioningpaused) (page) |
| Incomplete multipart uploads | Kept sessions resumed or swept within the abandon window plus one sweep interval | a session kept 30 to 36 hours ago and nothing resumed or swept for 36 hours, held for 1 | `vtop_multipart_sessions_total{outcome}` | [VtopMultipartSessionsPiling](runbooks.md#vtopmultipartsessionspiling) (ticket) |
| State-store write latency | At least 95% of committed batches spend under 1 s in their ledger writes | 15 minutes, held for 15 | `vtop_stage_duration_seconds{stage="state_write"}` | [VtopStateStoreWritesSlow](runbooks.md#vtopstatestorewritesslow) (ticket) |

## How to read them

- **Zero-tolerance objectives** (verification failures, retention loss,
  refused replays) have no error budget: each event is either data the engine
  refused to commit or data that is missing from the archive. They page on the
  first event.
- **Commit freshness** is about an engine that has data and is not archiving
  it. An idle source is not a miss: the alert only counts an engine with
  batches in memory (`vtop_inflight_batches` above zero).
- **Replay rate** has both a floor and a baseline so that a stream that always
  replays a little does not page, and one that never replays does not need to
  reach a high rate before it is noticed.
- **State-store write latency** is per batch: the time of all the ledger
  writes one committed batch made, about eight, two of them synced. The
  second is a default for a local disk or a nearby PostgreSQL server; a
  store known to be slower needs its own number. An engine with no committed
  batch in the window has no ratio, so an idle engine never raises it.
- **Source lag** depends on kafka-exporter (the lab runs it in
  `docker-compose.observability.yml`); the engine exports no lag of its own.
  Without the exporter there is no data and the alert stays silent.

## Tuning

The thresholds and windows are listed at the top of the rules file, each with
the one alert that uses it. To change one, edit the rule, copy the file to
`helm/vtop/files/vtop-rules.yaml` (CI fails when the two differ), and update
its unit test in `observability/prometheus/tests/vtop-rules.test.yaml`, which
pins the boundary each alert fires at. Run the tests with:

```bash
docker run --rm --network none -v "$PWD/observability/prometheus:/rules:ro" -w /rules \
  --entrypoint promtool prom/prometheus:v3.15.1 test rules tests/vtop-rules.test.yaml
```

Counters are per engine process and restart at zero, so a restart inside a
window can hide an event from an `increase()`. The durable record of each
event is the engine's log line beside the count.

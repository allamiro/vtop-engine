# Alert runbooks

One section per alert in
[`observability/prometheus/vtop-rules.yaml`](../../observability/prometheus/vtop-rules.yaml).
Each alert's `runbook_url` points at its section here, and
`scripts/check-alert-runbooks.py` fails CI when an alert has no section, a
link points at a heading that does not exist, or a section names an alert that
does not. How to load the rules, and what each objective is, is on
[Metrics and alerts](monitoring.md#alerts) and [Service level objectives](slos.md).

Severity `page` means data is missing, at risk, or not being archived.
Severity `ticket` means degraded or misconfigured, for working hours.

Every engine counter is per process and starts at zero when the engine
restarts. Each alert carries the series' own labels: `instance` names the
engine, and `tenant`, `source_type` and `format` the stream, where the metric
has them.

## VtopVerificationFailures

**What it means.** A batch's stored object or manifest did not match what the
engine uploaded (size or checksum), or, under the
[S3 Object Lock profile](../OBJECT_LOCK_DEPLOYMENT.md), the stored copy did
not carry the retention asked for. This is the check behind the
commit rule: a batch is committed only after it passes.

**What the engine already did.** The batch was not committed. It was marked
`FAILED`, its range was handed back to the source and marked
`REPLAY_REQUIRED`, and it will be read and uploaded again. Its data is still in
the source. The ERROR line `batch failed` carries the batch id and which check
failed (`object verification failed`, `manifest verification failed`), and an
Object Lock failure is also an `object_lock_check_failed` event.

**What to check.**

- The engine's log lines for the batch id, and `vtopctl list-batches --config
  <file>` for its state.
- The object store: is it healthy, is something else writing to the bucket,
  does it return the bytes it was given?
- Whether the failure repeats for the same stream (the alert's labels) or
  across all of them.

**What to do.** Fix the store or whatever rewrites objects in the bucket. Then
run [`vtopctl scrub`](scrub-and-audit.md#scrub) over the affected time range to
confirm what is stored verifies. A replay writes the batch again; nothing has
to be re-sent by hand.

**When it is safe to ignore.** Never on a production archive. One failure
that the replay then verifies still means the store returned something other
than what was written once; find out why.

## VtopNoCommits

**What it means.** The engine has had batches accumulating in memory
(`vtop_inflight_batches` above zero) for the last 30 minutes and has committed
none (`vtop_commits_total` flat, or absent since the engine started). Nothing
is being archived and source progress is not moving.

**What the engine already did.** Nothing specific: this alert reads a symptom.
The cause is usually also counted somewhere else (verification failures,
throttles, failed batches) and logged.

**What to check.**

- `vtopctl status --config <file>` and `vtopctl list-batches --config <file>`:
  where batches stop (sealed, uploaded, verified).
- The engine's WARN and ERROR lines.
- `vtop_failed_total`, `vtop_verification_failures_total`,
  `vtop_upload_throttled_total` and `vtop_stage_duration_seconds{stage}`: which
  stage is failing or slow. A slow `state_write` stage points at the state
  store.
- See [Data is not moving, or is missing](troubleshooting.md#data-is-not-moving-or-is-missing).

**What to do.** Fix the stage that is failing: the object store, its
credentials, the state store, or the network between them. Restarting the
engine does not lose data (recovery re-checks what was in flight) but does
not fix a broken dependency either.

**When it is safe to ignore.** When the engine is deliberately paused against
a store you have taken down for maintenance, and the alert is silenced for
that window. An idle source does not fire this alert: with nothing arriving,
`vtop_inflight_batches` stays at zero.

## VtopReplayRateSpike

**What it means.** Batches of one stream are being marked `REPLAY_REQUIRED`
and re-read from the source faster than about 9 in 15 minutes and more than
three times that stream's rate over the 6 hours before. Replays are how the
engine recovers from a failed batch, so a steady trickle is normal; a jump
means something started failing.

**What the engine already did.** Each failed batch was handed back to its
source and read again. Nothing is lost while the source still holds the data.

**What to check.**

- `vtop_failed_total` and `vtop_verification_failures_total` for the same
  stream, and the engine's WARN lines for why the batches failed (upload
  errors, verification, Object Lock checks, the state store).
- `vtop_upload_throttled_total`: a store asking for less concurrency.
- For a Kafka source, whether the replays keep up with retention
  (`vtop_retention_lost_records_total`).

**What to do.** Fix the cause the log lines name. If the store is throttling,
lower `batching.max_concurrent_batches` or turn on
`batching.adaptive_width.enabled` (see the
[config reference](../configure/reference.md#batching)).

**When it is safe to ignore.** Right after an outage of the object store,
while the backlog it caused is replayed, provided the rate falls back once the
store is healthy.

## VtopSourceLagGrowing

**What it means.** The Kafka consumer-group lag of the engine's group
(`consumergroup="vtop-engine"`, the default `sources.kafka.consumer_group`)
has risen on a topic for 30 minutes: the engine reads more slowly than the
topic is written. Partitions are summed per exporter, so the alert's
`instance` (the kafka-exporter) names the Kafka cluster, and a cluster whose
lag falls does not hide one whose lag rises.

The engine has no lag metric of its own. This alert reads
`kafka_consumergroup_lag` from
[kafka-exporter](https://github.com/danielqsj/kafka_exporter), which the lab
runs in `docker-compose.observability.yml`. Without that exporter (or one that
exports the same series) the alert never fires. If your sources use another
consumer group, change the matcher in the rule.

**What the engine already did.** Nothing: it keeps reading as fast as its
pipeline allows.

**What to check.**

- Upload throughput: `vtop_bytes_out_total`, `vtop_upload_width`,
  `vtop_upload_throttled_total`, and `vtop_stage_duration_seconds{stage}` for
  the slowest stage.
- Whether batches are failing and being replayed (`VtopReplayRateSpike`).
- The topic's retention against the current lag: when lag reaches retention,
  records are deleted before they are read (`VtopRetentionLoss`).

**What to do.** Remove the bottleneck (store throttling, a slow stage), raise
`batching.max_concurrent_batches`, or run more engines over separate streams.
Raise the topic's retention until the lag is back under control.

**When it is safe to ignore.** During a planned backfill or burst that the
engine is known to catch up on, with retention well above the lag.

## VtopSizeOnlyVerification

**What it means.** The engine started with
`upload.require_strong_verification: false`. Verification then compares sizes
only, so a stored object of the right size but the wrong content is committed.

**What the engine already did.** It warned at startup and `vtopctl validate`
warns about the same setting. Every batch it accepts this way is counted in
`vtop_verification_backend_limited_total`.

**What to check.** The config the engine runs with (`vtopctl validate --config
<file> --print-effective`, see [Effective settings](../configure/print-effective.md)),
and whether the backend and checksum algorithm can verify strongly.

**What to do.** Set `upload.require_strong_verification: true` with a backend
and checksum that can hash the stored bytes (see the
[config reference](../configure/reference.md#upload)), then restart the engine.
Run [`vtopctl scrub`](scrub-and-audit.md#scrub) over what was archived while it
was off.

**When it is safe to ignore.** In a lab, or where the deployment has
recorded why size-only checks are acceptable. Silence it with that reason.

## VtopObjectLockCheckFailed

**What it means.** With `upload.object_lock` set, a written or reused object
or manifest was not under the retention asked for (`result` is `missing`,
`mode_mismatch`, `too_short` or `legal_hold_missing`), its retention could not
be read (`error`), or its bucket has no Object Lock (`bucket_not_locked`).

**What the engine already did.** The batch failed and will be replayed. A
missing, shorter, other-mode or hold-less lock is also counted in
`vtop_verification_failures_total`. The ERROR line is an
`object_lock_check_failed` or `object_lock_bucket_refused` event with the
batch id, the object and the result.

**What to check and what to do.** The table in
[S3 Object Lock: what the engine says](../OBJECT_LOCK_DEPLOYMENT.md#what-the-engine-says)
gives each message and its fix: the bucket's Object Lock configuration, and the
credentials' `s3:PutObjectRetention` and `s3:GetObjectRetention` permissions.
A reused object without the lock fails its range on every replay until its
retention is extended or that version is deleted.

**When it is safe to ignore.** Never under a compliance requirement: until it
is fixed, batches of that tenant are not archived under retention.

## VtopRetentionLoss

**What it means.** A Kafka source found that the broker's retention removed
records the engine had not read yet. Those records were never read, never
manifested and never verified, and will never be archived.

**What the engine already did.** It counted the gap in
`vtop_retention_lost_records_total`, moved to the oldest offset still on the
broker, and logged a WARN naming the topic and partition (*retention removed
records this source had not read*). It cannot recover them. See
[retention loss](../../observability/README.md#retention-loss).

**What to check.** The WARN line for the topic, partition and offsets; the
consumer lag against the topic's `retention.ms` (`VtopSourceLagGrowing`); why
the engine fell behind.

**What to do.** Record the loss (topic, partition, offset range, time) for
whoever relies on the archive. Raise the topic's retention above the engine's
worst-case lag, and fix what made the engine fall behind.

**When it is safe to ignore.** Never. The data is gone; the alert is the
notice.

## VtopSourceReplayRefused

**What it means.** A batch failed and its source refused to hand the range
back for re-reading, or refused the re-read. For a file source: the file was
truncated, replaced or rewritten in place, or rotated away and then removed,
compressed or truncated before the re-read. That range is missing from the
archive.

**What the engine already did.** It counted the range in
`vtop_source_replay_refused_total` and logged an ERROR with the path. It did
not archive anything in place of the missing range.

**What to check.** The ERROR line for the path and range, and
[`vtopctl audit-coverage`](scrub-and-audit.md#coverage-audit), which reports
the hole.

**What to do.** Recover the range from another copy of the source data if one
exists. Then look at how files are rotated: a rotation that removes or
truncates a file before the engine has archived it loses data on every failed
batch.

**When it is safe to ignore.** Never: a range is missing from the archive.

## VtopRecoveryLedgerWriteFailed

**What it means.** At startup, recovery decided to re-read a range (a batch
that failed, or one left unfinished by a crash) and the state store refused
the write that records that decision. Recovery held the range instead of
rewinding it. Nothing is lost: a held range holds back every `VERIFIED` range
above it in its source unit, and a rewind above it starts at the held range,
so the range is read again with the ranges above it. Those ranges are archived
twice. The state store refusing writes is the problem to fix.

**What the engine already did.** It counted the range in
`vtop_recovery_held_total{reason="ledger_write_failed"}` and logged an ERROR
with the batch id, the source, the range and the store's error. It left the
row in the ledger as it was; the next start records the decision.

**What to check.** The ERROR line for the batch id and the error. Then the
state store: free space and inodes on the disk that holds the SQLite ledger
and its file and directory permissions, or, for PostgreSQL, connectivity, the
runtime role's grants and whether the server is read-only.

**What to do.** Fix the state store, then restart the engine. Recovery reads
the old rows again and retires them as `SUPERSEDED` or replays them. The
series is not primed, so it exists only on an engine that held a range at its
last start; the alert clears after a restart that holds nothing.

**When it is safe to ignore.** Not while it fires: the next write the state
store refuses may be a commit.

## VtopMultipartSessionsPiling

**What it means.** Uploads failed and kept their multipart sessions so the
replay could resume them, but for over a day none was resumed and none was
swept. Incomplete multipart uploads stay in the bucket, where their parts are
billed, until they complete or are aborted. It looks at sessions kept 30 to
36 hours ago, so a session still inside the abandon window plus one sweep
interval (24 h + 6 h by default) never raises it.

**What the engine already did.** It kept each session after the failed upload
(`vtop_multipart_sessions_total{outcome="kept_after_failure"}`). Sessions
older than `upload.multipart_abandon_after_secs` (default 86400) are aborted at
the store and deleted, at startup and every quarter of that window while the
engine runs. A sweep that fails is a `multipart_sweep_failed` event.

**What to check.** `multipart_sweep_failed` events and their errors; whether
the credentials may abort multipart uploads; the bucket's lifecycle rule for
incomplete multipart uploads ([PRODUCTION_HA.md](../PRODUCTION_HA.md) §7.6).

**What to do.** Fix the sweep's permissions or the store error it reports, and
keep a lifecycle rule that aborts incomplete uploads a little after the
abandon window, as a backstop.

**When it is safe to ignore.** When the bucket's lifecycle rule is known to
clean up incomplete uploads and the cost of a day's parts is acceptable.

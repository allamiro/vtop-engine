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
to be re-sent by hand. The steps, and what the drill measured, are in
[Disaster recovery](disaster-recovery.md#verification-fails).

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
A replay no longer stalls on an object already archived without the lock
asked for (#709): it writes a fresh locked object instead, which
[VtopObjectLockFallback](#vtopobjectlockfallback) reports.

**When it is safe to ignore.** Never under a compliance requirement: until it
is fixed, batches of that tenant are not archived under retention.

## VtopObjectLockFallback

**What it means.** With `upload.object_lock` set, a replay found the object
already archived for its range under its derived key, holding the batch's
bytes, but not locked as configured: it has no lock (`fallback_unlocked`: it
was stored before the profile was enabled, or by a store that dropped the lock
headers), a lock in the other mode (`fallback_lock_mode`), or no legal hold
while `legal_hold: true` (`fallback_hold_missing`). Or (`lock_withheld`) the
fresh object this engine wrote for the same range moments ago is not usable
either, which means the store is not keeping the lock it is asked for.

**What the engine already did.** For a `fallback_*` decision it wrote the
batch as a fresh object under its attempt key, under the configured lock, and
committed it only once that lock was read back on the exact version written.
The old object was left as it is: not written over, deleted or re-locked. For
`lock_withheld` it wrote nothing, failed the batch and will replay it; after
five minutes a replay writes one more fresh object. The WARN line is an
`object_key_fallback` event with the batch id, the old `key`, the new
`fallback_key`, the `reason` and a `suppressed` count (at most one a minute
per source and decision). An ended lock (`fallback_lock_elapsed`) does the
same but does not fire this alert: it is storage cost, not a finding.

**What to check.** Whether objects were archived before the profile was
enabled (expected once per such range), the bucket's default retention and
Object Lock configuration, and that the store keeps the retention and legal
hold it is sent. [`vtopctl scrub --check-retention`](scrub-and-audit.md)
lists every committed object whose lock is missing, short or in the other
mode.

**What to do.** For `fallback_unlocked` on objects from before the profile:
nothing; each range moves to a locked object on its next replay. For
`fallback_lock_mode`, `fallback_hold_missing` or `lock_withheld`: fix the
store or the bucket so that it keeps the lock as configured (see
[S3 Object Lock: what the engine says](../OBJECT_LOCK_DEPLOYMENT.md#what-the-engine-says)).
The old objects can be retired by your own retention process once their
ranges are committed elsewhere.

**When it is safe to ignore.** Right after enabling the profile on a bucket
that already holds archived objects, while the replays of those ranges move
them to locked objects.

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

## VtopBucketVersioningPaused

**What it means.** With `upload.require_object_versioning` set, a bucket
stopped keeping object versions under a running engine: an object or
manifest PUT returned no version to pin, or the store answered
GetBucketVersioning that versioning is not enabled. Someone suspended (or
never enabled) the bucket's versioning. A manifest the bucket does not keep a
version of cannot be pinned, so nothing in that bucket can commit.

**What the engine already did.** It stopped writing to the bucket. Each batch
for it fails before its first PUT and is replayed
(`vtop_bucket_versioning_paused_batches_total`), so the bucket does not grow
with a new manifest per retry. The batches that were already uploading when
versioning was turned off wrote at most one object or manifest each, and
committed nothing. The engine asks the store for the bucket's versioning 5 s
after it stopped, then on a backoff doubling to 60 s, and resumes writing when
the answer is `Enabled`: an INFO `bucket_versioning_resumed` line, and the
gauge drops. The ERROR `bucket_versioning_paused` line names the bucket, the
cause, and repeats at most once a minute per bucket while it stays off.

**What to check.** Who changed the bucket's versioning, and whether on
purpose: an unexpected change to versioning is a tampering signal (see
[Tamper evidence](../reference/tamper-evidence.md)). `GetBucketVersioning` on
the bucket named in the ERROR line.

**What to do.** Re-enable versioning on the bucket (PutBucketVersioning with
Status=Enabled). The engine resumes within about two minutes (its next
check comes at most 60 s after the last, and the batch that makes it at most
60 s after that) and replays what failed meanwhile; nothing to restart. If the bucket is meant to run without
versioning, set `upload.require_object_versioning: false` and restart, which
gives up version pinning ([Security model](../SECURITY_MODEL.md) §8.1).

**When it is safe to ignore.** Never: the engine archives nothing into that
bucket until it is fixed, and the sources keep the data only as long as their
retention does.

## VtopStateStoreWritesSlow

**What it means.** For 15 minutes on end, more than 5% of the batches the
engine committed in the last 15 minutes spent more than a second in their
ledger writes (`vtop_stage_duration_seconds{stage="state_write"}`, the time of
every state-store write a batch made, about eight, two of them synced to
disk). Each batch waits for its writes before its next stage, so the engine
archives more slowly than its sources write, and the backlog grows. No data is
at risk: a write that is slow is still a write, and a write that fails fails
the batch, which is replayed.

**What the engine already did.** Nothing different: it waits for each write,
up to `engine.state_store_timeout_seconds` (default 30), and a write that
takes longer fails its batch. The per-batch total is in each committed
batch's `batch_metrics` log line (`state_write_ms`).

**What to check.**

- For a SQLite ledger: the disk that holds it. Its latency, how full it is,
  and whether something else on it is syncing heavily (a backup, another
  database).
- For PostgreSQL: the server's load, lock waits on `batches`, replication
  waits if the server waits for a synchronous standby, and the network
  round trip from the engine.
- The engine dashboard's "Per-stage p95" panel (the `state_write` line),
  against the batch rate: a rise that follows the batch rate is load; one
  that does not is the store.

**What to do.** Fix what slows the store: move the SQLite ledger to a faster
local disk, or relieve the PostgreSQL server. Larger batches (`batching.
max_records`, `batching.max_bytes`) make fewer ledger writes for the same
data. Pruning the ledger (`vtopctl prune-ledger`) keeps its indexes small.

**When it is safe to ignore.** On a store known to be slow, when the engine
still keeps up with its sources (no `VtopSourceLagGrowing`, no
`VtopNoCommits`): raise the second, or the 5%, in the rule to what that store
does.

## VtopEpochHistoryBroken

**What it means.** A write to a data node replica's epoch journal failed,
usually because its data directory filled up. The journal records which
fencing epoch wrote each stretch of the replica's log; with a write missing,
that record may have a hole, and a replica reconciled against a history with
a hole could be told to keep or discard the wrong records.

**What the node already did.** It marked the history broken and logged one
ERROR line naming the replica, the range and the write that failed. From then
on it answers every fence and promotion probe with no history, so no leader
gives it a replication session and no promotion counts its offset. The range
runs with one replica fewer than configured. Nothing is lost by this, but the
next failure of another replica can leave the range without a quorum.
The mark is kept on disk beside the journal, as `fencing-epochs.broken` in
the replica's data directory, so neither freeing space nor a restart clears
it: a restarted node logs the ERROR again ("marked broken in an earlier run")
and keeps refusing. If the disk was too full to take the mark either, the node
says so in a warning, retries on every fence and append, and writes it as soon
as space is back. Do not restart it before that warning stops.

**What to check.** The ERROR line on that node (`its epoch history could not
be written`), its free disk space, and `vtop_broker_follower_connected` on the
leader, which shows the replica without a session.

**What to do.** Free space on the replica's data directory, or fix the storage
fault. Then stop it and rebuild its range from a replica whose history is
intact with `vtopctl node repair` (see the repair procedure in
[`PRODUCTION_HA.md`](../PRODUCTION_HA.md)), and start it against the repaired
directory. The repaired directory carries the source's history and no mark;
the replica rejoins through an ordinary session and catches up. Do not delete
`fencing-epochs.broken` by hand to bring the old directory back: the history
beside it may have the hole the mark is there to refuse.

**When it is safe to ignore.** Never while the range is meant to run at its
full replication factor. Why the node does not repair itself once space is
back is in [the design note](../EPOCH_HISTORY_AFTER_A_FAILED_WRITE.md).

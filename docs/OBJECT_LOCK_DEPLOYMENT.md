# S3 Object Lock deployment profile

With `upload.object_lock` set, the engine writes every telemetry object and
manifest under an S3 Object Lock retention, refuses a bucket that does not
have Object Lock enabled, and fails any batch whose stored object or manifest
does not carry the retention it asked for. A failed batch is never VERIFIED,
its source position does not advance, and it is replayed. This page covers
the identities, the bucket, the config, what the engine says, and what the
profile does not cover (#684).

Object Lock is a store feature: the engine asks for it and checks it was
applied. Creating a bucket with Object Lock, its default retention, and who
may bypass a governance retention are yours to set up.

## What the engine does

- **Every write carries the lock.** Object PUTs, manifest PUTs and multipart
  creates carry `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`
  (the time of the write plus `retain_days`, in whole seconds rounded up) and,
  with `legal_hold: true`, `x-amz-object-lock-legal-hold: ON`. A multipart
  object's lock is fixed when its upload is created, so after a resume it is
  retained from the creation, not the completion. The resumed session records
  that date.
- **The bucket is checked first.** For a literal `upload.bucket` (as
  sanitized for use: lower case, `.` and `_` become `-`), the engine refuses
  to start when `GetObjectLockConfiguration` says Object Lock is not enabled.
  A check the store cannot answer at startup (a 503, a timeout) is logged as
  a WARN and made again before the first upload to the bucket. For a
  templated bucket (`telemetry-{format}`), each bucket is checked at its
  first upload and the batch fails if it is not locked. Every bucket is
  checked once per process, and a batch is never written to a bucket that
  has not passed.
- **The stored retention is verified.** After the object and the manifest
  pass their content checks, the engine reads their retention with a
  `HeadObject` of the exact versions it wrote. Two cases fall back to the
  object's current version, which is also the version whose content was just
  verified: a reused object, and a multipart object completed by an earlier
  call whose version id the store did not report again. The engine compares
  what it finds with what it asked for: the same mode, a date no earlier, and
  the legal hold. A longer date passes. The comparison is against the date
  the engine sent (and never a date already past), not against its own clock
  plus `retain_days`, so clock skew between the engine and the store cannot
  fail a batch. A resumed multipart session whose retention has already ended
  is not continued: a new upload is started under a lock from now.
- **A reused object is checked more loosely.** A replay of the same bytes
  reuses the object an earlier attempt stored (#626). Its lock was set when
  it was first archived, so it only has to be under the configured mode with
  a date still ahead. The replay's own manifest is new and is checked
  strictly.
- **A reused object that is not locked as configured is not reused (#709).**
  The replay reads that object's retention before it decides to reuse it.
  If the lock has ended, the object has none (it was stored before the
  profile was enabled, or by a store that did not keep the headers), it is
  in the other mode, or it lacks the configured legal hold, the batch is
  written as a fresh object under its per-attempt key, under the configured
  lock, and verified like any write: its retention is read back on the
  exact version written before anything commits. The manifest and the
  ledger name the new key. The old object is never written over, deleted or
  re-locked; the cost is one extra object. A mode mismatch falls back in
  both directions: a compliance lock where governance is configured is
  stronger, but the committed object must be under the mode the profile
  sets. A retention the store cannot report (a 5xx, a timeout, a refused
  `s3:GetObjectRetention`) is **not** a reason to fall back, since during an
  outage every replay would write another object: the batch fails, writes
  nothing, and is replayed. Because S3 answers a HEAD from a caller without
  `s3:GetObjectRetention` (or `s3:GetObjectLegalHold`) with no lock headers
  at all, a missing lock or hold is confirmed with `GetObjectRetention` (or
  `GetObjectLegalHold`) before the engine acts on it; those answer a missing
  permission with `AccessDenied`, which fails the batch, and an object
  without a lock with `NoSuchObjectLockConfiguration`, which falls back. Before #709 such a range failed on every replay
  until an operator extended the old object's retention or deleted it.
- **A fallback is not repeated on every replay.** The engine remembers, per
  process, the attempt key it last wrote for each such range. A replay
  reuses that object if it holds the bytes and is locked as configured (the
  fresh object landed but its manifest failed). If it is there and not
  usable either (a store that does not keep the lock), the replay writes
  nothing and fails (`lock_withheld`) for five minutes, then writes one more:
  at most one extra object per range every five minutes, not one per replay.
  A restart forgets this memory, which costs at most one more object per
  range.
- **The reuse bound is the date, not `retain_days`.** A reused object whose
  lock ends tomorrow is still reused, so that batch's evidence is protected
  until tomorrow, not for `retain_days`. This is the bound #684 set; a
  minimum remaining retention would be a new setting.
- **Recovery and the scrub never fail a batch for an ended retention.** The
  check runs once, before VERIFIED. `vtopctl scrub --check-retention` reads
  retention after the fact and reports an ended one as `elapsed`, which is
  not a finding.

A store that ignores the lock headers fails every batch, loudly. That is the
intended outcome: the profile exists to prevent a silent downgrade to
unlocked objects, so it offers no warn-only mode.

## Identities and permissions

The runtime identity needs what `s3_native` always needs (see
[SECURITY_MODEL.md §5.1](SECURITY_MODEL.md#51-per-backend-least-privilege)),
plus:

| Permission | Why |
|---|---|
| `s3:PutObjectRetention` | To set a retention on a write. S3: "To set a retention period, you must have the `s3:PutObjectRetention` permission." |
| `s3:GetObjectRetention` | `HeadObject` returns `x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date` "only if the requester has the `s3:GetObjectRetention` permission". Without it every batch fails as `missing`. |
| `s3:GetObjectVersion` | To read the exact version written. |
| `s3:GetBucketObjectLockConfiguration` | The bucket check. |
| `s3:GetBucketVersioning` | `upload.require_object_versioning`, which the profile requires. |
| `s3:PutObjectLegalHold`, `s3:GetObjectLegalHold` | Only with `legal_hold: true`. Without it the engine sends no legal-hold header at all, because S3 checks `s3:PutObjectLegalHold` whenever the header is present. |

Do **not** give the runtime identity `s3:BypassGovernanceRetention`. A
principal holding it can shorten or remove a governance retention, so keep it
on a separate break-glass identity. Retention events are recorded in the
store's own audit trail: CloudTrail data events on AWS, the audit log on
MinIO.

The permission rules above are taken from the AWS S3 documentation
(`HeadObject`, `PutObject` and the Object Lock guide). They were not measured
against AWS here. The live tests run against MinIO, whose root credentials
hold every permission.

## The bucket

Object Lock can only be enabled when a bucket is created. Create the bucket
yourself: the profile refuses `upload.create_bucket: true`, so the engine
never creates a bucket and never needs `CreateBucket`. A bucket with Object
Lock is versioned. Set a **default retention as the floor**: it locks any
write that asks for no lock, including writes from an engine older than
v0.10.0 (see below).

MinIO (run against the lab's MinIO release):

```bash
mc mb --with-lock local/telemetry-locked
mc retention set --default GOVERNANCE 30d local/telemetry-locked
```

AWS (from the AWS CLI documentation, not run here):

```bash
aws s3api create-bucket --bucket telemetry-locked --object-lock-enabled-for-bucket
aws s3api put-object-lock-configuration --bucket telemetry-locked \
  --object-lock-configuration '{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":30}}}'
```

A retention the engine sets on an object **replaces** the bucket default for
that object; the two are not combined. On the lab's MinIO, an object written
with a one-day retention into a bucket with a 30-day default was retained for
one day. So set `retain_days` to at least the bucket default, or the engine's
objects end up retained for less time than the floor.

## The config

```yaml
upload:
  backend: s3_native
  bucket: telemetry-locked
  require_object_versioning: true   # required by the profile
  create_bucket: false              # the profile refuses true
  object_lock:
    mode: governance                # or compliance
    retain_days: 90                 # 1 to 3650, from each write
    legal_hold: false
```

Every key is listed in the
[config reference](configure/reference.md#uploadobject_lock). Only `s3_native`
supports the profile. Every other backend's config is refused at load (and
by `vtopctl validate`), naming the key and the backend. Unknown keys inside the block are refused, so a
misspelt `retain_day` is caught. The 3650-day cap is the engine's guard
against a typo, not a limit of S3. A `compliance` retention cannot be taken
back, so an extra digit would otherwise lock every object for a century.

`vtopctl tier copy` refuses an `--upload-config` with the block. It would put
the lock on cold-tier copies without ever checking it. Use a separate upload
config for the tier, and a bucket default retention on the cold-tier bucket if
its copies must be locked.

## What the engine says

At startup, with the profile set:

- INFO: `object lock enabled: mode=governance retain_days=90 legal_hold=false on bucket telemetry-locked (checked)`.
- WARN, under `compliance`: compliance retention cannot be shortened or
  removed by anyone, including the account root, until the date passes, and
  objects from failed attempts are locked too.

Each refusal says what happened, what the engine did, and what to do:

| Event | Message (abridged) | What to do |
|---|---|---|
| Startup, literal bucket without Object Lock | `bucket X does not have Object Lock enabled; the engine did not start. Object Lock can only be enabled when a bucket is created: create a new bucket with it and point upload.bucket at it` | As it says. |
| First batch for a templated bucket without Object Lock (`object_lock_bucket_refused`, `result="bucket_not_locked"`) | the same, with `the batch was not uploaded and will be replayed` | As it says. Nothing was written to the bucket. |
| Startup, a store that does not implement Object Lock (it answers `GetObjectLockConfiguration` with HTTP 501 `NotImplemented`) | `the store does not implement S3 Object Lock (it answered GetObjectLockConfiguration for bucket X with "not implemented"); the engine did not start. No bucket setting can change that: use a store that implements S3 Object Lock, or remove upload.object_lock` | As it says. A templated bucket on such a store fails its first batch with the same words (`result="store_unsupported"`). |
| Startup, a literal bucket without versioning (the profile requires `upload.require_object_versioning`) | `bucket X does not have versioning enabled, so the store would not keep the manifest versions the hardened profile pins; the engine did not start. Enable versioning on the bucket (PutBucketVersioning with Status=Enabled; if the store cannot enable it, use a store that keeps object versions), or set upload.require_object_versioning: false` | A bucket created with Object Lock has versioning on, so this names a different bucket or a store that does not keep it. A store that answers `GetBucketVersioning` with 501 is refused with `the store does not implement S3 bucket versioning`. The Object Lock check runs first, so a store without either is refused for Object Lock. A versioning check the store could not answer is a WARN at startup (`the versioning of bucket X could not be read ...; the engine started anyway`), and an ERROR `bucket_versioning_refused` at a batch. |
| Bucket check could not be made | WARN at startup: `the Object Lock configuration of bucket X could not be read (...); the engine started anyway, and the bucket is checked again before the first upload to it`; at a batch, the same with `the batch was not uploaded and will be replayed` (`result="error"`) | Grant `s3:GetBucketObjectLockConfiguration`, or check the store's health. |
| Startup, a backend that does not apply the lock | `upload.object_lock is set, but the X upload backend does not apply it ...` | Use `s3_native`. |
| Written object or manifest without the lock (`object_lock_check_failed`, `result` = `missing`, `mode_mismatch`, `too_short` or `legal_hold_missing`) | `object X (version V) is not under the requested retention (expected governance until at least D, found ...); the batch was marked failed and will be replayed; check the bucket's Object Lock configuration and the credentials' s3:PutObjectRetention permission, and s3:GetObjectRetention, without which the store answers a HEAD with no lock at all` | Fix the store or the credentials. The replay writes the batch again. |
| A write that asked for no lock (a multipart upload completed by a run without the profile) | `... was written by the s3_native backend without asking for the retention upload.object_lock sets (...); ... the replay checks the stored object itself` | Nothing, unless it repeats. |
| A replay finds its object unlocked, its lock ended, in the other mode, or without the configured legal hold (#709; WARN `object_key_fallback`, `decision` = `fallback_unlocked`, `fallback_lock_elapsed`, `fallback_lock_mode` or `fallback_hold_missing`; at most once a minute per source and kind, with a `suppressed` count) | `range R of S is already archived at X, but it carries no Object Lock retention (...); writing batch B under its attempt key Y instead, as a fresh object locked governance for 90 days, and the engine commits it only once that lock is read back; X is left as it is (not written over, deleted or re-locked); nothing is lost; run `vtopctl scrub --check-retention` ...` (the last part depends on the decision) | Nothing for the batch. If `fallback_unlocked` keeps rising for objects written since the profile was enabled, check that the store keeps the lock headers (`vtopctl scrub --check-retention` lists committed objects without a lock). |
| The fresh object written for the range moments ago is not usable either (WARN `object_key_fallback`, `decision="lock_withheld"`) | `range R of S is archived at X, but ..., and the object this engine wrote for it Ns ago at Y is not usable either (...); nothing was written: the batch was marked failed and will be replayed, and a replay after 300s may write one more object; the store may not be keeping the lock it is asked for: check ...` | Check the bucket's Object Lock configuration and `s3:PutObjectRetention`, `s3:GetObjectRetention`, `s3:GetObjectLegalHold`. |
| A replay reuses the fresh object an earlier fallback of this process wrote (INFO `object_key_reused`, `key_source="earlier_fallback"`) | `range R of S already has a verified, locked object from an earlier attempt of this process at Y ...; reusing it, no upload; nothing to do` | Nothing. |
| The retention of the object a replay would reuse could not be read (`object_lock_check_failed`, `result="error"`) | `the retention of object X, already archived for this batch's range, could not be read (...); it was neither reused nor replaced by a fresh object, ...; the batch was marked failed and will be replayed; check the store's health and the credentials' s3:GetObjectRetention permission` | As it says. Nothing was written. |
| Reused object that lost its lock while the batch was in flight (it passed the check before reuse) | `object X, reused from an earlier attempt, is not under the requested retention (...), though it was when this batch chose to reuse it; ... the replay writes the batch as a fresh locked object under its attempt key, leaving this one as it is; nothing to do unless it repeats` | Nothing, unless it repeats. |
| Retention could not be read (`result="error"`) | `the retention of object X could not be read (...); ...; check the store's health and the credentials' s3:GetObjectRetention permission` | As it says. |

Messages and logs never carry a signed URL or credentials. A store error that
quotes one is redacted before it reaches the ledger or the log.

Metrics: `vtop_object_lock_checks_total{tenant,result}` counts every check,
and every result other than `ok` is a failed batch. The look a replay takes
at an object before reusing it is a key decision, not a check: a fallback is
counted in `vtop_object_key_decisions_total{decision}` (the four
`fallback_*` values and `lock_withheld` above), and only a read that fails is
counted here, as `error`. The shipped rule
[VtopObjectLockFallback](operate/runbooks.md#vtopobjectlockfallback) raises a
ticket on `fallback_unlocked`, `fallback_lock_mode`, `fallback_hold_missing`
and `lock_withheld` (an object that should be locked is not, or the store is
not keeping locks); an ended lock (`fallback_lock_elapsed`) is storage cost,
not a finding. Missing, shorter,
other-mode and hold-less locks are also counted in
`vtop_verification_failures_total`. `vtop_object_lock_mode{tenant,mode}`
reads 1 for the mode in force (`off` without the profile). See
[Monitoring](operate/monitoring.md).

## Checking an archive later

```bash
vtopctl scrub --config /etc/vtop/config.yaml --check-retention
```

This reads each committed batch's retention, from the object at its key and
the manifest at its pinned version, and expects the configured mode. It
reports `locked`, `elapsed` (the retention has ended), `missing` and
`mode_mismatch`. The last two are findings: an object that can now be
deleted or overwritten. Use it after enabling the profile on an existing
fleet, to find objects an older engine wrote unlocked. When the scrub also
checks verification receipts (`manifest.verification_receipts`, or
`--require-receipts`), it reads each existing receipt's retention at its
current key too (one more request per receipt, under the same
`s3:GetObjectRetention` permission), and reports it apart, in
`vtop_scrub_receipt_retention_total` (#710). See
[Scrub and coverage audit](operate/scrub-and-audit.md).

## Storage cost

An object from an attempt that failed after its upload (a manifest upload
that failed, say) is locked like any other. It cannot be removed until its
retention ends, and under `compliance` nobody can remove it. Content-derived
keys (#626) make this rarer, because a replay of the same bytes reuses the
object rather than writing a second one, and since v0.10.0 a replay in a
later hour is sealed at the failed attempt's recorded instant, so it names
the same key and reuses the locked object too, as does a replay over a file,
spool or Kafka partition that grew (it is rebuilt as first cut). What still
writes a second locked object: a replay that could not be rebuilt as first
cut (`refused`, `too_large`), a replay whose bytes
differ (compression or checksum settings changed between the attempts), a
replay of a row written before v0.10.0, and a range whose ledger row a power
loss rolled back. Budget for these objects, and size them with
`vtop_replay_rebuilds_total{result=~"bytes_differ|fallback|refused|too_large"}`
([observability/README.md](../observability/README.md)). The startup WARN
under `compliance` says so.

A replay that finds its object unlocked, its lock ended, in the other mode
or without the configured legal hold writes one more locked object and
leaves the old one (#709). The old one is unlocked or elapsed, so it can be
removed by a lifecycle rule or a delete, except when it is under the other
mode: a compliance lock stays until its date. A crash between that fresh
object's upload and its ledger write, or a restart after a fallback whose
manifest failed, writes one more; these are counted as `fallback_*` in
`vtop_object_key_decisions_total`.

Incomplete multipart uploads are not objects and are not locked. The
abandoned-session sweep and an `AbortIncompleteMultipartUpload` lifecycle rule
still reclaim them.

## Not covered

- **Engines older than v0.10.0** ignore `upload.object_lock` and write
  unlocked objects, with nothing at the object to say so. Drain them before
  relying on the setting, keep the bucket default retention as the floor, and
  run `scrub --check-retention` once they are gone. Downgrade is unsupported:
  a downgraded engine writes unlocked objects.
- **The cluster plane's tier copies** (`vtopctl tier copy` refuses the
  profile).
- **Stores the matrix has not run.** The live tests run against the S3
  compatibility matrix's servers (#722). On the lab's MinIO
  (RELEASE.2025-09-07), RustFS 1.0.5, SeaweedFS 4.53, Versity S3 Gateway v1.11.0
  and Zenko CloudServer 9.5.5 they accept locked
  writes for both checksum paths and for multipart, return the lock headers
  on `HeadObject`, refuse to delete a locked version, and still report an
  ended retention's mode and date. Garage v2.5.0 implements no Object Lock
  (`GetObjectLockConfiguration` answers 501), and the engine refuses it at
  startup. The [S3 compatibility](reference/s3-compatibility.md) table is
  generated from the committed results. Behaviour on AWS S3 comes from its
  documentation until the opt-in AWS workflow has run.

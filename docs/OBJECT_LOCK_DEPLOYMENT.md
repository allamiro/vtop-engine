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
- **The bucket is checked first.** For a literal `upload.bucket`, the engine
  refuses to start when `GetObjectLockConfiguration` says Object Lock is not
  enabled. For a templated bucket (`telemetry-{format}`), each bucket is
  checked at its first upload and the batch fails if it is not locked. Every
  bucket is checked once per process.
- **The stored retention is verified.** After the object and the manifest
  pass their content checks, the engine reads their retention with a
  `HeadObject` of the exact versions it wrote. It compares what it finds with
  what it asked for: the same mode, a date no earlier, and the legal hold. A
  longer date passes. The comparison is against the date the engine sent,
  never its own clock at check time, so clock skew between the engine and the
  store cannot fail a batch.
- **A reused object is checked more loosely.** A replay of the same bytes
  reuses the object an earlier attempt stored (#626). Its lock was set when
  it was first archived, so it only has to be under the configured mode with
  a date still ahead. The replay's own manifest is new and is checked
  strictly.
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
| `s3:PutObjectLegalHold`, `s3:GetObjectLegalHold` | Only with `legal_hold: true`. |

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
supports the profile. Every other backend refuses the block at startup,
naming the key and itself. Unknown keys inside the block are refused, so a
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
| Bucket check could not be made | `the Object Lock configuration of bucket X could not be read (...); ...` | Grant `s3:GetBucketObjectLockConfiguration`. |
| Startup, a backend that does not apply the lock | `upload.object_lock is set, but the X upload backend does not apply it ...` | Use `s3_native`. |
| Written object or manifest without the lock (`object_lock_check_failed`, `result` = `missing`, `mode_mismatch`, `too_short` or `legal_hold_missing`) | `object X (version V) is not under the requested retention (expected governance until at least D, found ...); the batch was marked failed and will be replayed; check the bucket's Object Lock configuration and the credentials' s3:PutObjectRetention permission` | Fix the store or the credentials. The replay writes the batch again. |
| Reused object without the lock | `object X, reused from an earlier attempt, is not under the requested retention (...); ... a replay of the same bytes reuses this object again, so extend its retention with PutObjectRetention, or delete that version if nothing protects it and the replay will write a locked one` | As it says. Until then this range fails on every replay. |
| Retention could not be read (`result="error"`) | `the retention of object X could not be read (...); ...; check the store's health and the credentials' s3:GetObjectRetention permission` | As it says. |

Messages and logs never carry a signed URL or credentials. A store error that
quotes one is redacted before it reaches the ledger or the log.

Metrics: `vtop_object_lock_checks_total{tenant,result}` counts every check,
and every result other than `ok` is a failed batch. Missing, shorter,
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
fleet, to find objects an older engine wrote unlocked. See
[Scrub and coverage audit](operate/scrub-and-audit.md).

## Storage cost

An object from an attempt that failed after its upload (a manifest upload
that failed, say) is locked like any other. It cannot be removed until its
retention ends, and under `compliance` nobody can remove it. Content-derived
keys (#626) make this rarer, because a replay of the same bytes reuses the
object rather than writing a second one, but a replay with other batch
boundaries still writes a new object. Budget for these objects. The startup
WARN under `compliance` says so.

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
- **Stores other than MinIO** have not been measured. The live tests run
  against the lab's MinIO, RELEASE.2025-09-07. It accepts locked writes for
  both checksum paths and for multipart, returns the lock headers on
  `HeadObject`, refuses to delete a locked version, and still reports an
  ended retention's mode and date. Behaviour on AWS S3 comes from its
  documentation.

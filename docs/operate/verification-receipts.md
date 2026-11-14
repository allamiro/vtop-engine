# Verification receipts

A manifest is written, and its digest pinned, **before** its object is
verified, so it can only say how the object was going to be checked (a
version `0.4` manifest's `verification_plan`). What verification actually
found is recorded in the state-store ledger. A verification receipt puts that
result in the archive too, so an auditor reading only the bucket can tell how
each object was checked (#671).

## Turn it on

```yaml
manifest:
  verification_receipts: true
```

It is off by default. It works with either manifest version, and no reader
needs upgrading first: a reader that does not know about receipts never
fetches them. A consumer that **lists** the archive's prefix does see them,
one `.receipt.json` object per batch beside its `.manifest.json`, so check
such consumers before turning it on. An engine before this release ignores
the key and writes none.

At startup the engine logs one line saying receipts are on, where they go,
and that writing one is not part of the commit. `vtopctl validate
--print-effective` shows `manifest.verification_receipts`.

## What is written

After a batch reaches `VERIFIED`, the engine writes
`<batch_id>.receipt.json` beside `<batch_id>.manifest.json`:

```json
{
  "protocol": "VTOP",
  "receipt_version": "1",
  "batch_id": "vtop-20261114T194849Z-...-1655ba04",
  "manifest": {
    "uri": "s3://telemetry-data/.../vtop-20261114T194849Z-...-1655ba04.manifest.json",
    "sha256": "2fa5f6c9...",
    "version_id": "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"
  },
  "object": {
    "uri": "s3://telemetry-data/.../vtop-...-20d25430....raw",
    "size_bytes": 33,
    "checksum_algorithm": "sha256",
    "checksum": "20d25430...",
    "version_id": "..."
  },
  "verification": { "class": "strong", "method": "read_back" },
  "verified_at": "2026-11-14T19:48:49.975421305+00:00",
  "verified_by": "vtop-engine",
  "origin": "engine",
  "receipt": { "sha256": "939c3965...", "mac": "74f7a1ef..." }
}
```

- **`verification.class`** is the class the ledger recorded with `VERIFIED`:
  `strong`, or `size_only` under `upload.require_strong_verification: false`.
- **`verification.method`** is the check the store actually ran on the
  object: `store_checksum` (a SHA-256 the store computed), `read_back` (the
  engine read the object back and hashed it) or `size_only`. It can differ
  from the manifest's plan: native S3 configured for `store_checksum` reads
  back a multipart object, because S3 keeps no whole-object SHA-256 for one,
  and the receipt says `read_back`. A backend that does not say is written as
  `unspecified`, never guessed.
- **`manifest.sha256`** is the manifest's own self-hash, which the manifest's
  MAC covers, so the receipt is bound to that manifest.
- **The receipt is signed like a version `0.4` manifest**: a self-hash and,
  with a MAC key configured, a keyed MAC over the stored document, with the
  key id in `receipt.mac_key_id` when the key has one. The format is in the
  protocol draft, [§11.3](../VTOP_PROTOCOL_DRAFT.md#113-verification-receipt-extension).

## A receipt never holds a batch back

Writing a receipt is not part of the commit. If it cannot be written (the
store refuses it, it does not answer within the receipt's deadline, which is
`upload.request_timeout_seconds` and never more than 30 s, or under the S3
Object Lock profile the store did not keep the retention asked for), the
batch commits anyway, the
engine counts `vtop_verification_receipts_total{result="failed"}`, and logs,
at most once a minute per source with the number suppressed since:

```text
WARN event="verification_receipt_failed" batch_id=... suppressed=0
the verification receipt for batch ... could not be written: <the store's error,
URLs without their query strings>; the batch is verified and is committed
regardless; batches are unaffected. recovery writes the receipt if the batch is
still VERIFIED at the next start, and once it is committed vtopctl scrub reports
it missing. Nothing to do unless it repeats; then check the store's health and
the engine's permission to write beside the manifests
```

Alert on it as a warning, not a page:

```yaml
- alert: VtopVerificationReceiptsFailing
  expr: increase(vtop_verification_receipts_total{result="failed"}[1h]) > 0
  labels: {severity: warning}
  annotations:
    summary: "Verification receipts are not being written (batches are unaffected)"
```

## Recovery

When an engine starts and finds a batch still `VERIFIED` (it crashed, or its
source commit failed, before committing), recovery re-checks the stored batch
before committing it. If that batch has no receipt, recovery writes one from
what its re-check verified, with `"origin": "recovery"` and the class the
ledger recorded, and counts `vtop_verification_receipts_total{result="repaired"}`.
It asks the store first and writes only when no receipt is there: an existing
receipt is never replaced. The receipt is written before the commit, so a
batch recovery then holds back, and that is later replayed rather than
committed, keeps its receipt; the scrub checks committed batches only and
never reports it. A batch verified `strong` whose re-check could only
confirm size (under `upload.require_strong_verification: false`) is
receipted `class: strong, method: size_only`.

A batch that was committed without a receipt keeps none. The receipt records
the verification the batch was committed on; re-reading the object later
would be a different statement, so the scrub reports it missing rather than
writing one.

## Under S3 Object Lock

With `upload.object_lock` set ([S3 Object Lock](../OBJECT_LOCK_DEPLOYMENT.md)),
a receipt is written through the same path as the manifest, so it carries the
same retention, and the engine reads its retention back the way it reads the
manifest's. A receipt the store did not lock is a failed receipt, not a
failed batch, and is not counted in `vtop_object_lock_checks_total`, whose
failures are failed batches. Because recovery never writes over an existing receipt, no
second locked version appears beside the first.

After that, `vtopctl scrub --check-retention` reads each receipt's retention
again whenever the scrub also checks receipts (#710): the receipt at its
current key, since receipts are not pinned in the ledger. It is counted apart
from the batch's own retention, in
`vtop_scrub_receipt_retention_total{result}` and a `receipt retention:` line,
so a receipt whose lock was removed (by a principal allowed to bypass
governance) or that an engine wrote before the profile was set is reported,
and never mistaken for an unlocked object. A receipt with no retention, or
under another mode, is a finding; one whose retention has ended is not; a
missing receipt is not a retention finding (it is `missing` in
`vtop_scrub_receipts_total`). See
[Scrub and coverage audit](scrub-and-audit.md#scrub).

## Check them

`vtopctl scrub` checks each committed batch's receipt when the config sets
`manifest.verification_receipts`, or with `--require-receipts`:

```bash
vtopctl scrub --config /etc/vtop/config.yaml --require-receipts --metrics-file /var/lib/node_exporter/vtop-scrub.prom
```

It authenticates each receipt with the key it names and binds it to the
ledger row: the batch, the manifest's digest and pinned version, the object,
and the class. The results (`ok`, `missing`, `invalid`, `unauthenticated`,
`mismatched`, `unreadable`) are in
[Scrub and coverage audit](scrub-and-audit.md#scrub) and counted in
`vtop_scrub_receipts_total{result}`. Batches archived before receipts were
enabled have none, so `missing` is a finding only with `--require-receipts`:
pass it once the archive you scrub was written entirely with receipts on.

## What a receipt proves

With a MAC key, a receipt that verifies was written by a holder of the key
and is bound to one batch's manifest. Without a key, its self-hash detects
damage but not a forgery. It does not prove the object is intact today (the
scrub re-verifies that), and `verified_at` and `verified_by` are the engine's
own clock and name. The ledger stays the authority for whether a batch was
committed. See [Security model §8.4](../SECURITY_MODEL.md#84-verification-receipts-671).

## Turn it off

Set `verification_receipts: false` (or remove it). Receipts already written
stay where they are and keep verifying. Run the scrub without
`--require-receipts` afterwards, since new batches will have none.

# Tamper evidence

Every anti-tamper feature of VTOP, with the tampering it is meant to catch and
the test that does the tampering. Each case is a pair: someone changes the
archive, then the defence reports or refuses it. Where a defence is off, or
cannot see the change, the same tampering is shown **not** caught, so the
limits are stated by a test and not only by this page. The
[security model](../SECURITY_MODEL.md) says what each feature is for; this
page says what was measured.

## What the attacker is

The live cases run against a real S3 server, the lab's MinIO
(RELEASE.2025-09-07). The attacker:

- **holds write access to the bucket** (the lab's root credentials, used
  through a bare S3 client and never through the engine) and **not the manifest
  MAC key**;
- in the cases that say so, **also rewrites the ledger** so that it agrees
  with the forgery. That is the attacker a second line of defence has to
  survive, and it is what separates a MAC from a plain digest;
- in the audit cases, **can write the ledger file** (a SQLite ledger the test
  edits with SQL) and nothing else.

A forgery here is a good one: the replacement object, and a manifest that
describes it with its self-hash recomputed the way the engine does it.

## The cases

| Tamper action | Defence | Expected result, and with the defence off | Test |
|---|---|---|---|
| Replace an object with other bytes and write a manifest describing them, digest recomputed, no key | `manifest.mac` (needs `manifest.write_version: "0.4"`) | The scrub reports the batch `unauthenticated` (`MAC verification failed`), also when the ledger was rewritten to agree, and when the MAC was stripped (`MAC is required`). Recovery re-checks a VERIFIED copy of the row, does not commit it and hands the range back for replay (`replay_required`). | `live_mac_on_a_replaced_object_with_a_recomputed_manifest_digest_is_unauthenticated_and_recovery_refuses` |
| The same, with `manifest.mac` **off** | The ledger's recorded digest | Bucket access only: still `unauthenticated`, from the ledger (`hashes to ..., the ledger recorded ...`), not from the manifest. Bucket access **and** a rewritten ledger: **not caught**. The scrub passes and recovery commits the forged batch. | `live_mac_off_a_replaced_object_with_a_recomputed_digest_is_caught_only_by_the_ledger` |
| Replace an object behind an honest manifest (same size, one byte changed) | The object checksum in the manifest and the ledger, in the scrub and in recovery's storage re-check | The scrub reports `mismatched`; recovery does not commit and replays. No key is involved. | `live_a_replaced_object_behind_an_honest_manifest_is_mismatched_and_recovery_refuses` |
| Copy another batch's valid manifest over this batch's manifest | The binding of a manifest to its ledger row (the batch id, URIs and digest) | `unauthenticated` (`is batch X's, not Y`), with the MAC on and with it off; the other batch stays `ok`. Recovery replays. The MAC is not what catches this one. | `live_a_swapped_in_valid_manifest_of_another_batch_is_unauthenticated` |
| Forge a manifest signed under a key id the attacker made up, with the attacker's own key, and rewrite the ledger | The key a manifest names verifies it; a key the reader does not hold is never tried or guessed | `unauthenticated`, naming the id and `which is not in this keyring`, not corrupt. Recovery replays. | `live_a_manifest_signed_under_a_made_up_key_id_is_unauthenticated` |
| Sign with a key that was retired, or with a leaked key | `manifest.mac.retired` (a tombstone) | Manifests signed with a retired key, and a forgery made with it, are `unauthenticated` (`which is retired`), and recovery replays. **Before** the key is retired, a forgery made with the leaked key, with the ledger rewritten, is **not caught**. | `live_a_manifest_signed_with_a_retired_key_is_reported_retired_and_a_leaked_key_is_not_caught_before` |
| Delete a locked version, shorten its retention, overwrite it, delete it without a version, or clear its retention with a bypass | `upload.object_lock` (S3 Object Lock, `s3_native` only), and `scrub --check-retention` | The store **refuses** a delete of a locked version of an object or manifest and a shortening of its retention without the bypass, and VTOP's own check reports every object and manifest `locked`. The store **accepts** an overwrite (a new version) and a plain DELETE (a delete marker): the locked version keeps its bytes and its lock, and the scrub reports the batch `mismatched` or `missing`. A principal allowed to bypass governance can clear a retention: the scrub reports it `missing`. Compliance mode is not exercised (see below). | `live_object_lock_refuses_a_delete_of_a_locked_version_and_the_scrub_reports_what_it_allows` |
| Overwrite a manifest or an object in a versioned bucket | `upload.require_object_versioning`: the ledger pins the manifest's version, and the scrub compares the current key with it | A new version is created and the original is still stored. The scrub reports the manifest (`the current key no longer holds version V`) and the object (`mismatched`). With versioning **off** the overwrite leaves one version, the forgery: the original bytes are gone. | `live_a_versioned_overwrite_is_a_new_version_and_the_scrub_reports_the_replacement` |
| Suspend the bucket's versioning | The versioning check at engine start and at the first upload | A new engine refuses the bucket (`does not have versioning enabled`); a running one commits nothing and writes nothing. With the setting **off** the bucket is accepted and written to. | `live_suspended_versioning_makes_the_engine_refuse_the_bucket` |
| Alter a receipt without the key (hash recomputed, or not), swap in another batch's genuine receipt, or delete it | `manifest.verification_receipts`, signed like a 0.4 manifest | `invalid` for an altered receipt (the MAC, or the self-hash), `mismatched` for a swapped one, `missing` for a deleted one, which is a finding only under `--require-receipts`. The batches themselves still scrub `ok`: a receipt finding is about the evidence. | `live_receipts_with_the_mac_on_an_altered_swapped_or_deleted_receipt_is_reported` |
| Alter a receipt with the MAC **off**, hash recomputed | The ledger's record of the batch | A field the ledger holds (the verification class) is `mismatched`. A field it does not hold (`verified_by`, `verification.method`) is **not caught**: the receipt is `ok`. | `live_receipts_with_the_mac_off_only_what_the_ledger_holds_is_checked` |
| Delete an object, or replace it with one of another size, or of the same size | `audit-coverage --check-storage` (a HEAD of every committed object and manifest) | The deleted object (`not readable`) and the resized one (`bytes stored, the ledger recorded`) are reported. The same-size replacement is **not caught**: a HEAD cannot prove content. The scrub, which reads the bytes, reports all three. | `live_check_storage_sees_a_deleted_or_resized_object_not_a_same_size_replacement` |
| Edit or delete an audit row through the ledger | The append-only triggers on `audit_events` | The ledger refuses the `UPDATE` and the `DELETE`; the trail is unchanged. | `an_audit_row_cannot_be_updated_or_deleted_through_the_ledger` |
| Drop the triggers and edit an audit record | The hash chain, `vtopctl audit verify` | `BROKEN: row N ... the row was edited`; exit status non-zero. | `an_edited_audit_record_breaks_the_hash_chain` |
| Delete an audit record from the middle | The hash chain | `BROKEN` at the row after it (`deleted, inserted or reordered here`). | `a_deleted_audit_record_breaks_the_hash_chain` |
| Swap two audit rows | The hash chain | `BROKEN` (`deleted, inserted or reordered here`). | `a_reordered_audit_record_breaks_the_hash_chain` |
| Truncate the end of the audit trail | The row count and last link `audit verify` prints, **kept elsewhere** | **Not caught by the chain**: the shortened chain verifies. Against the anchor an operator kept, the row count, the last sequence number and the last link all differ. The verdict says so: a chain cannot show rows removed from its end. | `a_truncated_audit_tail_passes_the_chain_and_shows_against_the_kept_anchor` |
| Edit an audit record and recompute every later link, with the chain **unkeyed** | None | **Not caught**: the chain proves the rows agree with each other, not who wrote them. | `an_unkeyed_chain_recomputed_after_an_edit_is_not_caught` |
| The same, with a manifest MAC key configured (a **keyed** chain), without the key | The keyed chain | `BROKEN` (`an unkeyed link after a keyed one`). | `a_keyed_chain_rewritten_without_the_key_is_caught` |
| The same, with the key | None | **Not caught**: a chain recomputed with the key verifies. | `a_keyed_chain_rewritten_with_a_leaked_key_is_not_caught` |
| Delete a committed batch's ledger row from the middle of a file's history | `audit-coverage` | A coverage hole, reported as lost data. | `a_committed_ledger_row_deleted_from_the_middle_is_a_coverage_hole` |
| Delete the last committed row | `audit-coverage` | **Not caught**: nothing below the last committed range is missing, and the source resumes from the earlier committed point, so the bytes are archived again. | `a_committed_ledger_row_deleted_from_the_end_is_not_a_coverage_hole` |

## Reading the table

- **MAC off is not defenceless against a bucket-only attacker.** The scrub and
  recovery bind each manifest to the digest the ledger recorded, so a manifest
  rewritten by someone who cannot also change the ledger is still reported.
  What the MAC adds is protection against an attacker who can also change the
  ledger, and against any reader that has no ledger to compare with (an
  auditor holding only the bucket).
- **Object Lock protects versions, not keys.** S3 refuses to delete or shorten
  a locked version. It does not refuse a new PUT or a DELETE marker at the same
  key. Those leave the locked version intact underneath, and VTOP's scrub
  compares the current key with what the ledger pinned. Governance mode can be
  bypassed by a principal holding `s3:BypassGovernanceRetention`; keep that on a
  break-glass identity ([Object Lock deployment](../OBJECT_LOCK_DEPLOYMENT.md#identities-and-permissions)).
- **An audit chain cannot show a removed tail.** Keep the row count and last
  link `vtopctl audit verify` prints somewhere the ledger's writer cannot
  reach, and compare them at the next verification.

## Not protected

These are outside what VTOP's tamper evidence promises, and the suite does not
claim otherwise:

- **A compromised engine host.** An attacker running as the engine, or root on
  its host, holds the MAC key, the S3 credentials and the ledger. It can write
  a manifest that authenticates, rewrite the ledger to agree, and write the
  audit trail with a valid key. Nothing here detects a writer that holds
  everything the engine holds.
- **A leaked MAC key.** A forgery signed with the key authenticates until the
  key is retired, and the audit chain keyed from it can be recomputed (the two
  cases marked above). Retiring the key makes its manifests fail by design;
  re-archiving what it signed is a separate decision
  ([Rotate the manifest MAC key](../operate/manifest-key-rotation.md)).
- **Confidentiality.** There is no encryption at rest unless the store is
  configured to provide it. Integrity and secrecy are separate: every case
  above is about changed bytes, not read bytes.
- **Deletion where nothing pins the data.** An unversioned bucket with no
  Object Lock keeps no copy of an overwritten object. The scrub reports the
  replacement; it cannot restore the original.
- **S3 Object Lock in compliance mode.** It is the retention that no principal
  can bypass, and the live suite does not exercise it: an object written under
  it cannot be removed from the lab store until its retention ends. What the
  suite measures is governance mode, and the engine's check that the mode and
  date it asked for were stored.
- **Stores other than the lab's MinIO.** Behaviour on AWS S3 comes from its
  documentation and was not measured here
  ([Object Lock deployment](../OBJECT_LOCK_DEPLOYMENT.md#not-covered)).
- **A tamperer who cannot be told apart from the engine.** The audit actor of a
  local command is the process's uid and login name: as strong as the host's
  logins, and not authentication.

## Running the suite

The live cases are `#[ignore]` tests that need an S3 endpoint with Object Lock
and the lab's buckets, the same as the other live MinIO tests, and run in the
`minio-integration` CI job:

```bash
docker compose build minio && docker compose up -d minio
docker compose run --rm minio-init

VTOP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  cargo test -p vtop-cli --test live_s3_tamper -- --ignored --test-threads=1
```

The cases that need no store, the audit trail and the audit of coverage, are
ordinary tests:

```bash
cargo test -p vtop-cli --test tamper_audit_trail --test tamper_evidence_doc
```

Every object a live case writes is deleted at its end (governance retention
bypassed with the lab's root credentials). `tamper_evidence_doc` fails when a
test is missing from the table above or the table names one that does not
exist.

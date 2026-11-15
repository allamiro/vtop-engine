# Data-safety register

Every data-safety bug found in VTOP so far: when it was introduced, when and
how it was found, and what fixed it. It is the record the data-safety quiet
period is measured from, so it is kept complete by a check, not by memory.

## What counts

VTOP's one promise is that a source's progress is committed only after its
batch is VERIFIED. A **data-safety bug** is one that can reach one of these
five outcomes, the classes in the *Class* column:

| Class | What it means |
|---|---|
| commit without verified data | a source position committed without verified data |
| lost range | a lost or silently skipped range |
| acknowledged record lost | an acknowledged record lost |
| corruption passes verification | corruption that passes verification |
| unarchived bytes deleted | deletion of source bytes that were not archived |

Out: duplicates (VTOP is at-least-once, so a replay that stores a range twice
loses nothing), liveness failures where nothing is lost but the engine or a
range stops or retries, and a bug that only fails a batch in the safe
direction. A bug that the code analysis shows can reach one of the five
outcomes counts even when no loss was observed; its row says "derived, not
reproduced".

Issues are classified by hand after triage and carry the
[`data-safety` label](https://github.com/allamiro/vtop-engine/issues?q=label%3Adata-safety).
The [data-safety issue form](https://github.com/allamiro/vtop-engine/issues/new?template=data_safety.yml)
does not add it, because a suspected report is not yet a classified bug.

## Quiet period

The quiet period is the number of days since the most recent `data-safety`
issue was opened:

```bash
scripts/check-data-safety-register.py --quiet-days
```

Every release records that number in its release pull request. It is a
measurement, not yet a gate: the production-readiness bar (#714, criterion A1)
asks for 90 days or three consecutive releases, whichever is longer, with the
fault, soak and history-checking campaigns running inside the window.

Data-safety bugs were still being found as late as November 2026. Until a
quiet period has passed, keep the source data until a
[scrub](../operate/scrub-and-audit.md) of the archive passes.

## Adding a row

When an issue is labelled `data-safety`, add a row in issue-number order:

- **Introduced**: the commit or PR that brought the bug in and the first
  release that carried it, read from `git log -S` and `git tag --contains`.
  "Never released" when it was fixed before a release shipped it. Where the
  origin cannot be traced, say so; do not guess.
- **Found**: the date the issue was opened (UTC), which the check compares.
- **Fixed**: the PR, the first release that carries it and the merge date;
  `open` while the issue is open.
- **Found by**: review, soak or chaos run, user, CI, or other.

`scripts/check-data-safety-register.py` compares the table below with the
labelled issues. The `data-safety register` workflow runs it on every pull
request that touches this page, weekly, and whenever the label is added or
removed; a red run means a row is missing, stale or unlabelled.

## Register

Releases: v0.1.0 2026-08-05, v0.2.0 08-06, v0.3.0 08-09, v0.3.1 08-10,
v0.4.0 08-28, v0.5.0 09-05, v0.6.0 09-07, v0.7.0 11-10, v0.8.0 11-12, v0.9.0 11-14.

| Issue | Class | Component | Introduced | Found | Fixed | Found by | What happened |
|---|---|---|---|---|---|---|---|
| [#64](https://github.com/allamiro/vtop-engine/issues/64) | corruption passes verification | upload (verification) | the first commit, 9bb5ca5 (the aws CLI backend trusted uploaded metadata), and 474ee39 (the local-fs sidecar); never released | 2026-07-18 | #137 · before v0.1.0 · 2026-07-19 | review | "Strong" verification could pass on an engine-written sidecar or on user metadata instead of the stored bytes. |
| [#65](https://github.com/allamiro/vtop-engine/issues/65) | unarchived bytes deleted | file source | the first commit, 9bb5ca5; never released | 2026-07-18 | #124 · before v0.1.0 · 2026-07-19 | review | The progress marker took the inode from a second `stat` of the path, so after a rotation `delete_after_commit` could delete the replacement file. |
| [#68](https://github.com/allamiro/vtop-engine/issues/68) | corruption passes verification | `vtopctl verify-manifest` | the first commit, 9bb5ca5; never released | 2026-07-18 | #113 · before v0.1.0 · 2026-07-19 | review | `verify-manifest` only sent a HEAD, so it reported success for a corrupted or replaced object. |
| [#261](https://github.com/allamiro/vtop-engine/issues/261) | acknowledged record lost | broker replication | #176 (e05beec) · v0.1.0 | 2026-08-05 | #263 · v0.2.0 · 2026-08-06 | review (while building #259) | A diverged follower acknowledged the new leader's records at offsets where it held different bytes, so a record was acknowledged without a quorum holding it. |
| [#264](https://github.com/allamiro/vtop-engine/issues/264) | acknowledged record lost | broker replication | #176 (e05beec) · v0.1.0 | 2026-08-06 | #266 · v0.2.0 · 2026-08-06 | review | High-water-mark updates to a busy follower were dropped, so it could allow a truncation of acknowledged records. Derived, not reproduced. |
| [#315](https://github.com/allamiro/vtop-engine/issues/315) | acknowledged record lost | replica repair | the sealed-segment transfer of #270 (#278, 6df215e) and `vtopctl node repair` (#303) · v0.3.0 | 2026-08-09 | #320 · v0.3.1 · 2026-08-10 | chaos run (live scenario of #242) | A repaired replica received no epoch history, so the next leader transition truncated everything the repair had written to it, committed records included. Derived: the issue records the replica emptied, not an acknowledgement lost from every copy. |
| [#449](https://github.com/allamiro/vtop-engine/issues/449) | acknowledged record lost | promotion | #235 (23fd315) · v0.1.0 | 2026-09-05 | #461 · v0.6.0 · 2026-09-05 | review (#240 analysis) | A promotion could publish a boundary below one already published, sending watermarks backwards and truncating acknowledged records off the replica still holding them. Derived, not reproduced. |
| [#552](https://github.com/allamiro/vtop-engine/issues/552) | lost range | engine | the first commit, 9bb5ca5 · v0.1.0 | 2026-10-14 | #563 · v0.7.0 · 2026-10-15 | review | A FAILED batch was never re-read, and a later range of the same source could commit past it. Derived, not reproduced. |
| [#553](https://github.com/allamiro/vtop-engine/issues/553) | acknowledged record lost | replication quorum | #235 (23fd315) · v0.1.0 | 2026-10-14 | #576 · v0.7.0 · 2026-10-15 | review | Quorum size came from each node's own config, so during a replica-set change two disjoint majorities could form. Derived, not reproduced. |
| [#558](https://github.com/allamiro/vtop-engine/issues/558) | lost range | file and spool sources | the first commit, 9bb5ca5 · v0.1.0 | 2026-10-14 | #564 · v0.7.0 · 2026-10-15 | review | Rotation or truncation was not detected, so a new file was read from the old file's offset and its start was skipped. |
| [#578](https://github.com/allamiro/vtop-engine/issues/578) | lost range | file and spool sources, coverage audit | the first commit, 9bb5ca5 (markers without content identity) · v0.1.0 | 2026-10-15 | #606 · v0.7.0 · 2026-11-10 | review (while building #573) | A same-inode replacement at least as large as the old read position looked like the old file, so its unread start counted as archived. |
| [#611](https://github.com/allamiro/vtop-engine/issues/611) | acknowledged record lost | promotion | #263 (9702232, reconciliation at the fence) · v0.2.0 | 2026-11-10 | #616 · v0.8.0 · 2026-11-11 | review (of #607) | A replica ahead of the candidate could truncate a quorum-acknowledged record at the fence and then vote for a candidate without it. Derived, not reproduced. |
| [#614](https://github.com/allamiro/vtop-engine/issues/614) | commit without verified data | Kafka source | the first commit, 9bb5ca5 · v0.1.0; reachable only with two engines on one state store, which is unsupported | 2026-11-10 | #617 · v0.8.0 · 2026-11-10 | review | A stale engine's offset commit was not fenced; its commit past a range sent back for replay skipped that range (#617's test `a_stale_commit_past_a_range_sent_back_for_replay_does_not_skip_it`). |
| [#619](https://github.com/allamiro/vtop-engine/issues/619) | unarchived bytes deleted | file source | the first commit, 9bb5ca5 · v0.1.0 | 2026-11-10 | #631 · v0.8.0 · 2026-11-11 and #689 · v0.9.0 · 2026-11-14 (in part); #753 and #713 · v0.10.0 · 2026-11-15 | review | `delete_after_commit` checked the length and then unlinked the path, so lines appended in between, or by a writer still holding the file open, were deleted unarchived. |
| [#620](https://github.com/allamiro/vtop-engine/issues/620) | lost range | file source | the first commit, 9bb5ca5 · v0.1.0; narrowed by #564 (inode and length) and #606 (first 4 KiB) | 2026-11-10 | #632 · v0.8.0 · 2026-11-11 | review | A rewrite that kept the first 4 KiB and regrew past the cursor was read as a continuation, so its rewritten bytes below the cursor were skipped. |
| [#621](https://github.com/allamiro/vtop-engine/issues/621) | lost range | file source | #563 (a157549, a failed range handed back to its source) · v0.7.0 | 2026-11-10 | #647 · v0.8.0 · 2026-11-11 | review | A failed range from a file rotated since it was read was refused by the rewind, and was archived only if the rotated name matched a source pattern. |
| [#622](https://github.com/allamiro/vtop-engine/issues/622) | corruption passes verification | engine, manifest | 94b7da9 (batches of several reads) · v0.1.0 | 2026-11-10 | #630 · v0.8.0 · 2026-11-11 | review (3,955 of 4,056 manifests) | A batch of several reads wrote a manifest whose `source_progress` started at the last read, so the manifest misstated its range and still verified. No record was lost: the ledger held the right range ([note](#labelled-rows-that-record-no-loss)). |
| [#623](https://github.com/allamiro/vtop-engine/issues/623) | lost range | Kafka source | most likely #121 (8877bb3, the Stored-offset retry) · v0.1.0; the issue's leading hypothesis, not confirmed by a reproduction | 2026-11-10 | #644 · v0.8.0 · 2026-11-11 | other (one benchmark run) | A partition stopped being read until the engine restarted; whatever Kafka retention removed meanwhile was never archived. Derived, not reproduced. |
| [#635](https://github.com/allamiro/vtop-engine/issues/635) | acknowledged record lost | replication | #616 (1c6ab48); never released | 2026-11-10 | #643 · v0.8.0 · 2026-11-11 | review (of #616) | A kept disputed suffix was remembered in memory only, so a restart reopened the #261 gap: an acknowledgement over different bytes. Derived, not reproduced. |
| [#639](https://github.com/allamiro/vtop-engine/issues/639) | lost range | engine recovery | #563 (a157549) · v0.7.0 | 2026-11-11 | #645 · v0.8.0 · 2026-11-11 | review | A range whose replay decision could not be recorded was not held back, so a higher VERIFIED range committed past it. |
| [#640](https://github.com/allamiro/vtop-engine/issues/640) | none of the five: liveness | replication (fencing epochs) | #320 (5ec236a, adoption recorded at the replica's tail) · v0.3.1; not confirmed by running the reproduction there | 2026-11-11 | #648 · v0.8.0 · 2026-11-11 | chaos run (deterministic promotion crash sweep) | A follower filed caught-up records under its newly adopted epoch, so after a second failover no quorum was provable and no leader could be elected. No acknowledged record was lost in any run ([note](#labelled-rows-that-record-no-loss)). |
| [#641](https://github.com/allamiro/vtop-engine/issues/641) | acknowledged record lost | promotion | #235 (23fd315) · v0.1.0 | 2026-11-11 | #649 · v0.8.0 · 2026-11-11 | chaos run (deterministic promotion crash sweep) | A deposed leader absent from the election kept its old-epoch records under the new high-water mark and acknowledged the new leader's records there as duplicates. Derived: no run lost an acknowledged record. |
| [#657](https://github.com/allamiro/vtop-engine/issues/657) | none of the five: liveness | metadata tier | #430 (33e7d14, the transition records) · v0.5.0; the release in which a record could first exceed the 1024-byte bound is not traced | 2026-11-11 | #662 · v0.8.0 · 2026-11-11 | review (while building #656) | A snapshot could be written that could not be read back, so a voter could not restore and the tier could lose quorum. No data-plane record was lost ([note](#labelled-rows-that-record-no-loss)). |
| [#782](https://github.com/allamiro/vtop-engine/issues/782) | lost range | engine recovery | #563 (a157549, a range left mid-pipeline is not rewound when its replay decision cannot be recorded, the path reproduced) · v0.7.0; for a stale VERIFIED range, #126 (e2cdcfa) · v0.1.0, derived, not reproduced | 2026-11-15 | open | experiment (a three-range recovery fixture) | A range held during recovery, because its replay decision could not be recorded, was passed after the pass ended: a later range's rewind moved the reader past it, and the running pipeline committed progress beyond it, so it was never archived. |

### Labelled rows that record no loss

Three labelled issues record, in the issue or in the PR that fixed it, that
nothing was lost: #622 (the manifest misstated its range; the ledger and
recovery were right), #640 and #657 (a range or the metadata tier stopped,
which is a liveness failure). They carry the label from the triage that
created this register. If that is revised and the label removed, move the row
to the table below with the reason; the check enforces either place.

## Considered and not labelled

| Issue | Why it is not labelled |
|---|---|
| [#73](https://github.com/allamiro/vtop-engine/issues/73) | A benchmark cleanup could delete a directory the caller passed in. It is not source data the engine archives. |
| [#453](https://github.com/allamiro/vtop-engine/issues/453) | Liveness: an append the replica plane could not frame was refused, so the quorum never arrived and the client timed out. Nothing was acknowledged. |
| [#465](https://github.com/allamiro/vtop-engine/issues/465) | Liveness: a metadata node wedged on a vote write. Nothing was lost. |
| [#482](https://github.com/allamiro/vtop-engine/issues/482) | Multipart upload as first planned would have failed every batch above 8 MiB: the safe direction. |
| [#557](https://github.com/allamiro/vtop-engine/issues/557) | The issue's worst case is a duplicate or a mixed ledger row. The path from a stale writer to a skipped range is #614, which is labelled. |
| [#577](https://github.com/allamiro/vtop-engine/issues/577) | An Unknown-lineage answer was counted by offset. Neither the issue nor its fix (#607) shows a path to a lost record. Revisit if the history checker (#728) finds one. |
| [#626](https://github.com/allamiro/vtop-engine/issues/626) | Duplicates only: a replay writes a second object. Open. |
| [#627](https://github.com/allamiro/vtop-engine/issues/627) | The running engine's rewind after a failed FAILED write costs at most a duplicate (#638's test `a_failed_failed_write_costs_one_duplicate_object_and_no_gap`). The documented rule would have lost the range, and #638 corrected the docs. |

---
name: vtop-commit-rule-review
description: Review or write code on the VTOP engine archive path (sources, batches, upload, verify, state store, replay) against the commit rule "SOURCE_COMMITTED is forbidden until VERIFIED is true". Use when touching vtop-core state_machine/replay/manifest, vtop-adapters, vtop-upload, vtop-state, or the engine runtime in vtop-cli, or when adding a source or upload backend.
---

# Commit-rule review (engine / archive plane)

The invariant: **source progress (Kafka offset, file byte offset, spool offset) never advances until the object and its manifest are durably written and verified.** Normative text: `docs/VTOP_PROTOCOL_DRAFT.md` §12-13. Implementation tour: `docs/ARCHITECTURE.md` §5, §13.

It is enforced in three layers; a change must not weaken any of them:
1. `vtop-core::state_machine` — legal transitions only.
2. `vtop-state` (`StateStore`, SQLite / Postgres) — the durable ledger refuses an illegal transition.
3. The engine pipeline in `vtop-cli` — the order of calls.

## Why this is the right shape (book lens)

This is an at-least-once pipeline made safe by idempotent, verifiable output — the end-to-end argument: the guarantee is established by the *final* check (verify the stored object against the manifest), not by any one hop being reliable. A crash between upload and commit yields a replay, never a gap. Duplicates are tolerable; loss is not. Keep that asymmetry in every decision.

## Checklist

- [ ] **Order.** Is there any path where the source marker can advance before `VERIFIED`? Include error paths, retries, shutdown, and `recover()`.
- [ ] **Crash at every boundary.** For each state transition, what does restart do? `Engine::recover()` currently handles `Verified` (retry source commit) and `SourceCommitted` (nothing); every other non-committed state becomes `REPLAY_REQUIRED` and re-reads from the last committed marker. Keep it that way unless a granular resume is proven safe (`replay.rs` `Retry*` variants are reserved, not live).
- [ ] **Verification strength is honest.** Strong vs backend-limited vs size-only (protocol §17) must be reported as what it is. A weaker check must never be logged or exported as a strong one.
- [ ] **Manifest binding.** Manifest names the exact marker range, checksum, and object location, and is self-hashed. A change to batch contents must change the manifest.
- [ ] **Idempotent replay.** Known gap: `batch_id` embeds a timestamp and random suffix, so replay writes a new key (protocol §15.1 goal not met). Don't claim deterministic naming; don't make it worse; if you close the gap, say so in docs/ARCHITECTURE.md §13.
- [ ] **Partial uploads / multipart.** An object visible in the bucket is not a verified object. Verify reads back (or uses backend checksums) before commit.
- [ ] **New source adapter.** Implements `SourceAdapter` with a `SourceProgressMarker` whose commit is a no-op until told; commit must be durable on its own terms (Kafka offset commit acked, file offset fsynced). See ARCHITECTURE §7.1 for what the commit rule does *not* cover.
- [ ] **New upload backend.** Implements the `UploadBackend` verification contract and passes `crates/vtop-upload/tests/transport_conformance.rs`.
- [ ] **State store change.** Both SQLite and Postgres go through `state_machine`; add the case to `vtop-state` `test_battery.rs` so both backends are held to it.

## Tests that prove it

Existing: `tests/integration_replay.rs`, `integration_manifest_rollback.rs`, `integration_state_recovery.rs`, `scripts/e2e-smoke.sh`, benchmark scenario `05-fault-replay.yaml`. A fix needs a test that fails against the old behaviour (see `vtop-contributing`). Prefer injecting the fault *between* two steps (after upload, before commit) over mocking the whole step.

## Review output format

Report as: invariant touched -> the interleaving that breaks it (concrete steps) -> minimal fix -> the test that would have caught it. If no interleaving breaks it, say what you checked.

---
name: ddia-encoding-evolution
description: Design and review wire formats, on-disk formats, manifests, and config for forward/backward compatibility and rolling upgrades in VTOP. Use when changing vtop-protocol frames, vtop-log codecs/segment formats, vtop-meta command/key encodings, the manifest schema, or config keys, or when planning an upgrade or format version bump.
---

# Encoding and evolution

Book lens: chapter 4 — formats for encoding data, schema evolution, and dataflow through databases, services, and messages. In a system that is upgraded one node at a time, **old and new code, and old and new data, coexist for a long time**. Every format change must survive that.

## Definitions to keep straight

- **Backward compatible:** new code can read data written by old code.
- **Forward compatible:** old code can read data written by new code (ignoring what it does not understand).
- Rolling upgrades need both on the wire; durable data needs backward at minimum, and forward if downgrade is ever supported.

## What VTOP already does (preserve it)

- Hand-coded, bounded, checksummed codecs; no serde in `vtop-meta` wire/storage paths. Explicit field tags and sizes make the format reviewable and fuzzable.
- Version markers in `vtop-log`: `FORMAT_VERSION_V2`, `RECORD_SCHEMA_VERSION_V2`, `CHUNK_TREE_SCHEME_V1`, `COMMIT_SCHEME_*`, `SegmentManifestV2`.
- `scripts/upgrade-compat.sh`: an old release writes a data dir, the new binary must open it, quarantine nothing, and serve every record byte-exactly.
- **Downgrade is neither tested nor supported**; `docs/ROADMAP.md` says so. Do not imply otherwise.
- Config: older releases refuse unknown keys — that is the compatibility contract, so new keys need a release-gating note.

## Rules

1. **Never reuse or renumber a tag/field/discriminant.** Add; do not repurpose. Removed fields stay reserved.
2. **New required fields break old readers.** A field added to a durable or wire format is optional-with-default for at least one release, or gated behind a version bump that old nodes reject cleanly.
3. **Reject unknown versions loudly, accept unknown optional fields quietly.** Failing closed with a clear error beats misreading. Corrupt != newer: distinguish them in the error.
4. **Data outlives code.** A format written today must be readable by the next releases' readers. Add a fixture from the *old* binary, not one built by the code under test — fixtures built with the code under test only prove the format agrees with itself.
5. **Bound every length before allocating.** See the `MAX_*` constants in `vtop-meta`; a new variable-size field needs its own bound and a fuzz case.
6. **Deterministic encoding where bytes are hashed or compared.** Manifests are self-hashed; canonical field order and number encoding are part of the contract. Any change shifts hashes — treat it as a format version bump.
7. **Mixed-version windows are a design input.** For a protocol change, write down what each of (old leader, new follower), (new leader, old follower) does. Metadata commands applied through Raft must be deterministic across versions: all replicas must reach the same state from the same log.
8. **Dataflow through the database means old rows meet new code.** Postgres/SQLite ledger migrations are explicit and separate from runtime connect (`migrate_state_store`); keep migrations additive and the engine identity free of DDL rights.
9. **Services: prefer evolvable request/response shapes** over positional ones; carry capability/version in the handshake rather than inferring from behaviour.

## Checklist for a format change

- [ ] Which version marker changes, and what do old readers do with the new data?
- [ ] Is there an old-release fixture in `upgrade-compat.sh` coverage (add the profile if the format is new)?
- [ ] Fuzz target extended (`fuzz/fuzz_targets/`: `manifest_parse`, `detect_*`; add one for new parsers)?
- [ ] Docs updated: `docs/VTOP_PROTOCOL_DRAFT.md` for protocol, `docs/ROADMAP.md` known limitations for compatibility notes?
- [ ] CHANGELOG / release notes mention it (`scripts/release-notes.sh`, `docs/RELEASE_VERIFICATION.md`)?

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
- Config splits in two, and a new key behaves differently in each. The cluster-plane role configs in `crates/vtop-node/src/config.rs` (`MetaNodeConfig`, `DataNodeConfig`, `LeaseConfig`, ...) carry `deny_unknown_fields`: an older node **refuses to start** on a key it does not know, so a new key there needs a release-gating note. The top-level engine structs in `crates/vtop-core/src/config.rs` (`VtopConfig`, `EngineConfig`, `UploadConfig`, `SourcesConfig`, ...) do **not**: an older engine silently ignores a new direct key (the nested `StateStoreEnvRef`, `StateStoreFileRef` and `EgressTuning` are the exceptions - they reject unknown keys, so a key added inside them is refused by an older engine), so an operator who sets a safety knob (a deadline, a limit) on a fleet that has not been upgraded gets no error and no protection. State that in the release note and, when the knob matters for safety, make the new behaviour's default safe so ignoring the key is harmless.

## Rules

1. **Never reuse or renumber a tag/field/discriminant.** Add; do not repurpose. Removed fields stay reserved.
2. **New required fields break old readers.** A field added to a durable or wire format is optional-with-default for at least one release, or gated behind a version bump that old nodes reject cleanly.
3. **Reject unknown versions loudly, accept unknown optional fields quietly.** Failing closed with a clear error beats misreading. Corrupt != newer: distinguish them in the error.
4. **Data outlives code.** A format written today must be readable by the next releases' readers. Add a fixture from the *old* binary, not one built by the code under test — fixtures built with the code under test only prove the format agrees with itself.
5. **Bound every length before allocating.** See the `MAX_*` constants in `vtop-meta`; a new variable-size field needs its own bound and a fuzz case.
6. **Deterministic encoding where bytes are hashed or compared.** Manifests are self-hashed; canonical field order and number encoding are part of the contract. Any change shifts hashes — treat it as a format version bump.
7. **Mixed-version windows are a design input.** For a protocol change, write down what each of (old leader, new follower), (new leader, old follower) does. Metadata commands applied through Raft must be deterministic across versions: all replicas must reach the same state from the same log.
8. **Dataflow through the database means old rows meet new code.** Ledger migrations differ by backend: PostgreSQL migrations are explicit and separate from runtime connect (`migrate_state_store`), so the engine identity needs no DDL rights; SQLite migrates on `connect` (the embedded single-node store opens and initialises itself). Keep migrations additive either way, and say which backend a migration note is about.
9. **Services: prefer evolvable request/response shapes** over positional ones; carry capability/version in the handshake rather than inferring from behaviour.
10. **Decode-then-re-encode drops what a newer writer added** — for formats that carry unknown fields at all (the JSON manifest). Any component that relays or rewrites such records must keep unknown content; if a hash or MAC covers the re-encoded form, a newer manifest fails verification on an older reader and looks like corruption. Hash stored bytes (or preserve unknown keys), and check the `version` field first so the error says *newer than supported*, not *corrupt*. This does **not** apply to the hand-coded binary codecs (`vtop-meta`, `vtop-protockol`-style frames): they have no skippable-field envelope and deliberately reject trailing bytes and unknown tags, so their evolution path is a version gate, not tolerance.
11. **Pick the compatibility direction per flow, from the readers that can actually meet the data.** Request/response between servers upgraded in order needs backward compat on requests and forward on responses. Stored data needs new readers to read old data (backward) always; it needs old readers to read new data (forward) only where a supported flow puts an old binary in front of it - a Raft entry or replicated segment reaching a voter or follower not yet upgraded during a rolling upgrade, a manifest read by an older consumer. A local durable format that only this node reads again needs no forward compatibility while downgrade is unsupported (see above): version-gate the change and have an old reader refuse it cleanly, never accept it. Report an explicit unsupported-version or unknown-tag decode error as "newer format", and a truncated body, trailing bytes or an invalid field as "malformed" — a valid checksum alone does not identify a future format, so the error type must carry the distinction.
12. **Enum/integer shape matters.** Adding an enum variant is backward-only (old readers have no arm); give closed enums an explicit unknown/skip behaviour or a version gate. Widening an integer lets old readers truncate.
13. **A version field no reader checks is dead.** Negotiate in-band; where the framing allows it (a length-delimited header the receiver can skip), an unknown message kind should get a typed `Unsupported` reply rather than a dropped connection, and the sender should check the peer's negotiated minor before sending a new kind. VTOP's native protocol today validates the kind in the header decoder before dispatch and adds compatible kinds under minor 0, so a typed reply and a minor bump would be a protocol change to design deliberately, not a free rule to apply.
14. **Mixed-version cluster is its own test.** Raft peers with exact-equality wire versions and strict trailing-byte decoding may be unable to append a new command tag on an old voter. Whether quorum survives depends on the *current voter set*: a majority of upgraded voters can still commit while an old voter rejects the entry (and then falls behind or fails to apply), whereas too few upgraded voters stall the group. Pick one stated procedure: (a) a **coordinated** stop-all/upgrade-all of the metadata tier, or (b) a compatibility gate — a persisted cluster `min_version` that refuses to propose commands above it until every voter reports support. Note that `OnDelete` or a partitioned StatefulSet rollout only controls *when* pods are replaced; it does **not** enforce all-at-once and can leave voters on mixed revisions. `upgrade-compat.sh` only proves a single data dir opens.
15. **Large integers in JSON** above 2^53 are parsed inexactly by non-Rust consumers; carry large u64s as strings where such consumers exist.

## Checklist for a format change

- [ ] Which version marker changes, and what do old readers do with the new data?
- [ ] Is there an old-release fixture in `upgrade-compat.sh` coverage (add the profile if the format is new)?
- [ ] Mixed-version behaviour stated for each of: old leader/new follower, new leader/old follower, old reader of new data?
- [ ] Fuzz target extended (`fuzz/fuzz_targets/`: `manifest_parse`, `detect_*`; add one for new parsers)?
- [ ] Docs updated: `docs/VTOP_PROTOCOL_DRAFT.md` for protocol, `docs/ROADMAP.md` known limitations for compatibility notes?
- [ ] CHANGELOG / release notes mention it (`scripts/release-notes.sh`, `docs/RELEASE_VERIFICATION.md`)?

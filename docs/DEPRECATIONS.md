# Deprecations

Everything VTOP has deprecated and not yet removed. **Nothing is removed
before it appears here**, for the time [Versioning and support](SUPPORT.md)
sets: at least one minor release in 0.x, at least two in 1.x. A removal made
for security or data safety is stated in that release's notes instead.

Each entry was checked against the code. "Deprecated in" is the first
release that said so.

## Deprecated

| Item | Deprecated in | Earliest removal | Replacement | How an operator sees it |
|---|---|---|---|---|
| The `CancelRetention` metadata command, and `vtopctl meta cancel-retention` that proposes it | v0.1.0 (#184) | Not scheduled. The command stays on the wire so a peer of any release decodes it. | None. A `RetentionPlanned` segment is terminal: recovery completes the retention, or a repair establishes fresh verified placement. | `vtopctl meta cancel-retention --help` says "Deprecated", and the metadata state machine rejects every proposal of it ("retention cannot be cancelled after deletion is authorized"). |

## On `main`, in no release yet

Deprecations merged for the next release. Each moves to the table above when
that release ships.

| Item | Deprecated on `main` by | Replacement | Notes |
|---|---|---|---|
| The manifest field `verification_status` | #671 (release B), for v0.10.0 | `verification_plan` in a `0.4` manifest, with the ledger or a verification receipt for what verification found ([protocol §11.2](VTOP_PROTOCOL_DRAFT.md#112-field-reference)) | Still written, also in `0.4` manifests, because v0.8.0 readers require it. Its value is always `not_verified`, the state at write time. |

## Announced for a later release

Decided, and not yet deprecated. Each is listed here so the release that
deprecates it has the plan to follow.

| Item | What is decided | Where |
|---|---|---|
| `manifest.write_version: "0.3"` | `0.4` becomes the default writer at v0.12.0, announced in the v0.10.0 and v0.11.0 notes. `"0.3"` stays accepted through 1.x, with a deprecation warning. | [#743, decision 3](https://github.com/allamiro/vtop-engine/issues/743#issuecomment-6884402733) |

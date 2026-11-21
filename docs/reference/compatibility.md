# Compatibility and upgrade policy

This page says what each persisted and wire format of VTOP promises, how a
format may change, which upgrades and downgrades are supported, and how a
surface is deprecated. It is the policy VTOP 1.0 is held to, and it says
where 0.x still falls short of it. The plan behind it, with the reasoning
for each rule, is issue
[#743](https://github.com/allamiro/vtop-engine/issues/743).

> [!IMPORTANT]
> This is the policy 1.0 is held to. VTOP is at 0.x, and where 0.x differs
> today, the rule says so. **Downgrade is not supported in 0.x**, except
> for the formats and pairs of releases the compatibility matrix records a
> result for (see [downgrade](#downgrade)): the downgrade rules below are
> targets, and take effect for a pair of releases only once the matrix
> records a downgrade result for it. The compatibility matrix is added
> under #743; it records the ledger, its digests and the audit trail so far.

## Status words

Every format in the [inventory](#inventory) carries one of three statuses.

| Status | Meaning |
|---|---|
| **stable** | Documented. A change needs a version bump, a reader-first release (below) and a deprecation entry. |
| **evolving** | Expected to change before 1.0, through its version mechanism. Each one is classified when 1.0 is frozen: stable, or left outside the 1.0 guarantees (the cluster-plane formats follow the scope decision in [#740](https://github.com/allamiro/vtop-engine/issues/740)). |
| **experimental** | May change in any minor release with a release-note entry. Outside every guarantee on this page. |

## Four classes of format

| Class | Formats | A reader must accept | The writer writes | Mixed versions | Downgrade |
|---|---|---|---|---|---|
| **A. Archive artifacts** (outlive every release) | manifests, verification receipts, object keys, ledger digests | Every version any release has written, forever. | One version, the default. A new version is opt-in until the default changes (see [reader first](#reader-first)). | Every release reads every archive version up to its own. A reader older than a version's first reader reports a failure, and the matrix records which. | The archive is never rewritten. Going back means setting the write version back; what was written stays. |
| **B. Node-local state** | the state ledger (SQLite and PostgreSQL), progress markers, audit records, segment logs and their sidecars, broker journals, multipart sessions, the scrub cursor, the metadata log, snapshots and their files | What minors N and N-1 wrote (and N-2's, at rest: stop, upgrade, start), and what the bridge release wrote (see [upgrade](#upgrade)). | One version. | One node, or a fleet sharing a ledger: minor N and N-1. | Target: supported until the first one-way step (see [downgrade](#downgrade)). After it, the older binary refuses cleanly and changes nothing. |
| **C. Wire** | the native protocol, replica protocol negotiation, the metadata peer and admin transport, the Kafka gateway's advertised surface | N-1 and N during a rolling upgrade. | The highest version the peer advertised (replica negotiation does this today; the native protocol from 1.0, see [before 1.0](#config-json-output-and-the-wire-before-10)). | N-1 and N. N-2 is not a mixed-version guarantee. | A peer that predates a feature is never sent it (from 1.0 for native message kinds). |
| **D. Operator interface** | `config.yaml`, `validate --print-effective`, `vtopctl` JSON output and exit codes, metric names and labels, the Helm values | From 1.0: every config key, flag and JSON field of N-1. | Not applicable. | Per node: an older node ignores or refuses a new key ([change rules](#change-rules-within-1x)). | Not applicable. |

## What frozen means

A format is frozen at 1.0 when all six of these hold. The format registry
(see [inventory](#inventory)) will record which do for each format, as
#743's later steps add the fixtures, cells and fuzz targets.

1. **The marker is read first.** Every reader checks the version before it
   decodes anything else, and its error says one of three different things:
   *newer than this build*, *malformed*, or *unauthenticated*.
2. **Fixtures from released binaries.** One for each version written since
   the baseline, produced by that release's published binary, with a
   `PROVENANCE` file naming the tag, the asset, the asset's SHA-256 from the
   release's `SHA256SUMS`, the command and the input. A fixture built by the
   code under test only proves that the format agrees with itself.
3. **A cell in the compatibility matrix** for upgrade (the old release
   writes, the current build reads) and for forward (the current build
   writes, the old release reads), each with a recorded expected result:
   `pass`, or `refuse` with the message the older binary gives.
4. **A fuzz target** for each decoder of untrusted bytes, seeded from the
   fixtures.
5. **A closed-enum review.** Every persisted or wire enum either reads an
   unknown value as unknown, or sits behind a version gate whose error is
   classified as *newer*.
6. **A byte lock.** The golden files are hashed in the registry, and a change
   to one without a registry edit and a `format-change` label fails CI.

## Change rules within 1.x

| Change | In a minor release? | Condition |
|---|---|---|
| A new optional field in an unhashed JSON document (an audit record, a multipart session file, the effective-settings JSON) | Yes | Readers ignore unknown fields, and a new value of a closed set reads as unknown. |
| A new field in a **hashed** JSON document (a manifest) | Only under the `0.4` canonical scheme | Manifests `0.1` to `0.3` hash the decoded document, so a reader drops an unknown field and reports the document as tampered. Such a field stays ledger-only until every supported reader uses the `0.4` scheme. |
| A new nullable ledger column | Yes | Named in the column list, never written by N-1, never dropped, no schema-version or marker bump. |
| A new ledger state or source type, or any change N-1 would misread | No; it needs a version bump | A one-way step: announced, operator-gated, with its refusal test. For SQLite that gate does not exist yet: the ledger migrates on every open, so a bump must first be made something only `vtopctl migrate` takes. |
| A new metadata command kind, snapshot category or value tag, or a changed apply behaviour | Yes, behind a feature level | The level gates it, voters are upgraded first, and an older reader classifies the new tag as *newer*. |
| A new native protocol message kind | From 1.0, only with a `PROTOCOL_MINOR` bump and a sender that checks the peer's minor | Before 1.0, kinds are added under minor 0 and an N-1 peer rejects one as unknown, so a new kind is used only once every node runs N. |
| A new config key | Yes | A safe default, so an older engine that ignores the key is harmless; otherwise the release notes say so. A key inside a section that refuses unknown keys (for example the cluster role configs, `manifest.mac`, `upload.object_lock`) makes an older node refuse the file: the notes say that too. |
| Renaming, removing or repurposing anything stable, or a default that changes the bytes written | No | A version bump, a reader-first release and the [deprecation](#deprecation) notice. A removal waits for 2.0. |
| A new metric label value | Yes | From a bounded set, pinned by a test. |
| A metric name or label name | No | Deprecation first. |
| **A patch release** | **No format change of any kind** | No new marker, default, feature level or one-way step. In 0.x a patch that cannot avoid one says so in its notes as a one-way change. |

## Reader first

A new version of a format reaches users in three steps, each in a minor
release, never a patch:

1. Release R ships the **reader** for version V, with its fixtures and
   matrix cells.
2. Release R+1 or later ships an **opt-in writer**.
3. The **default changes** no earlier than two minors after R and one minor
   after the opt-in writer. The notes of the release before announce it, and
   a startup warning names the setting that keeps the old version.

The oldest reader of V is R. The engine cannot see who reads its archive, so
turning the writer on is the operator's acknowledgement that every reader is
on R or later, and the notes say so.

For manifests: v0.8.0 is the `0.4` reader, v0.10.0 adds the opt-in writer
(`manifest.write_version: "0.4"`), and `0.4` becomes the default writer at
**v0.12.0**, announced in the v0.10.0 and v0.11.0 notes.
`write_version: "0.3"` stays accepted through 1.x with a deprecation
warning. Verification receipts stay opt-in for 1.0.

## Upgrade

- **From any release of minor N-1 to any release of minor N** is supported.
  What is tested today is below; cells for the other node-local formats are
  being added under #743.
- **From 0.x to 1.0** goes through a **bridge release**: the last 0.x minor.
  It reads every 0.x ledger it is upgraded from, and 1.0 reads the bridge
  release's state. 1.0 does not have to read a v0.7.0 or v0.8.0 ledger
  directly, so upgrade to the bridge release first.
- **N-2 to N** is the target for state at rest (stop, upgrade, start); its
  tests come with the 1.0 freeze. A rolling fleet mixing N-2 and N is not
  supported.

What is tested today:

- **A data node's directory, in the compatibility matrix** (the `segments`
  cell of [`scripts/compat/expected.yaml`](../../scripts/compat/expected.yaml),
  which runs [`scripts/upgrade-compat.sh`](../../scripts/upgrade-compat.sh)).
  For the newest patch of each minor since v0.2 (the newest minor on a pull
  request, all of them nightly) the release's **published** `vtop-node`,
  checked against its `SHA256SUMS` and Sigstore bundle, writes a directory:
  the single `range.active` before v0.3.0, sealed segments with their indexes,
  commit markers, manifests, producer snapshots and the broker's epoch
  journals from v0.3.0, and segment format v2 with its chunk sidecar from
  v0.5.0 (`segment_format: v2`, which the default writer does not produce).
  The current build must open it, quarantine nothing, serve every record byte
  for byte, and serve them again after a restart. The directories written by
  v0.2.1, v0.3.1, v0.5.0, v0.8.0 and v0.10.1 are also committed as fixtures
  (`crates/vtop-log/tests/fixtures/segments/<tag>/<profile>/`, each tag with
  a `PROVENANCE`), which `released_segment_dirs` (every record reads, every
  sealed segment verifies) and `released_journals` (the producer-epoch and
  fencing-epoch journals read as what the release recorded) open on every
  build. The retention and truncate intents are not among them: a clean run
  never leaves one, so no released binary's intent is captured yet.
- The metadata tier rolls from the newest release to the current build, one
  voter at a time (live-chaos scenario 22).
- The released `vtopctl` of v0.7.0 and later is run over a manifest archive
  the current build wrote: v0.8.0 and later must verify it, and v0.7.0 must
  report the documented self-hash failure
  ([`scripts/manifest-compat.sh`](../../scripts/manifest-compat.sh)).
- **The state ledger and its digests, in the compatibility matrix** (the
  `ledger-sqlite`, `digests-audit` and `ledger-postgres` cells of
  [`scripts/compat/expected.yaml`](../../scripts/compat/expected.yaml)). For
  every release since v0.7.0 the published `vtopctl` writes a SQLite ledger
  (batches committed, handed back for replay and retired, one committed row
  pruned into a digest, and from v0.10.0 the audit trail), and the current
  build must open it, read every row, audit coverage over the ledger and the
  digest, scrub, recover, archive new input behind it, and leave every
  batch the release committed committed, with the schema version at the one
  it writes: opening it is not a one-way step. (A ledger an older release
  created is at mode 0644, which the current build refuses to read until the
  operator runs `chmod 0600` on it
  ([#775](https://github.com/allamiro/vtop-engine/pull/775)); the cell
  checks that refusal changes nothing, then takes the step.) The same for PostgreSQL, from
  v0.9.0, with the release built from its tag because the published
  archives have no PostgreSQL support; the current build runs `vtopctl
  migrate` on what the release migrated, and a second `migrate` changes
  nothing. The released ledgers are also committed as fixtures from each
  release's binary
  (`crates/vtop-cli/tests/fixtures/ledger-sqlite/<tag>/`, each with a
  `PROVENANCE`), which `ledger_sqlite_compat` opens on every build.

## Downgrade

In 0.x, downgrade is not supported, except where the matrix records a
result for a pair of releases. The rules below are the target; each applies
to a pair once the matrix records a downgrade result for it. Today it
records them for these formats, against the **previous release** (the older
releases down to v0.7.0 are run too, and what they do is recorded, not
promised):

| Format | The previous release, on what the current build wrote | Recorded in |
|---|---|---|
| State ledger, SQLite | Runs `status`, `list-batches` and `process-once`, then (`scripts/ledger-compat.sh`) archives, audits coverage over the digests and writes the nullable columns it knows, and the current build carries on over what it wrote. | `ledger-sqlite` forward |
| Prune digest and audit trail | Reads the digest (`audit-coverage`, `scrub`) with the `sealed_at` key it does not know, and verifies and lists the audit chain. | `digests-audit` forward |
| State ledger, PostgreSQL | After the current build ran `vtopctl migrate` and wrote, runs `status`, `list-batches` and `process-once`. Nightly. | `ledger-postgres` forward |

Each is `pass` in `expected.yaml` for the previous release, with one recorded
refusal: **a release before v0.8.0 stops on a ledger that holds a
`SUPERSEDED` row** (a handed-back range the current build retired), saying
`unknown batch state`, on its first read, and changes nothing. A result that
is not `pass` is an edit to that reviewed file: the older binary exits
non-zero with a message, and the ledger is exactly as the current build left
it. Two minors apart is not a supported downgrade.

What an older binary does with newer data, by case:

| Newer data | The N-1 binary | Policy (target) | Tested today |
|---|---|---|---|
| A nullable ledger column | Names its own columns, ignores the new one and writes `NULL` in the rows it inserts. A later release reads `NULL` as unknown. | Supported. No marker bump, and the column is never dropped. Only N-1 shares a PostgreSQL ledger with N. | Yes: `sealed_at` (#626) against the previous release, SQLite on every pull request and PostgreSQL nightly (`ledger-sqlite`, `ledger-postgres`, forward) |
| A Kafka progress marker with a `topic_id` | Ignores the key. It resumes a partition from the newest committed row, as it always did, and its pruning keeps that row. | Supported: an optional field in an unhashed document, read as unknown when absent. | No |
| A ledger row in a state, or of a source type, N-1 does not know | Fails closed. | One-way: needs a marker or version bump, so the refusal comes at startup and not mid-run. | No |
| A `0.4` manifest | v0.8.0 and later verify it; v0.7.0 and earlier report it as tampered. | The documented cutoff. Archives are never rewritten; set `write_version: "0.3"` to stop writing more. | Yes: v0.7.0, and v0.8.0 and later (`manifest-compat`) |
| A metadata log entry or snapshot record from a raised feature level | The voter refuses to start, saying *newer format*, and changes nothing. Today that holds for a new command kind, log entry kind, value tag or key category; other enum values inside records are reported as corrupt until [#763](https://github.com/allamiro/vtop-engine/pull/763). | One-way at the raise, which is operator-gated and permanent. The notes say which level first puts a new kind in the log. | No |
| A segment v2 directory | Every release since v0.5.0 reads v2. | Supported. A later segment version follows [reader first](#reader-first). | No |

## Deprecation

- A stable surface (a config key or value, a CLI command or flag, a JSON
  field, a metric name or label name, a manifest version written) is
  removed only in a major release. Before 1.0, a removal needs one minor of
  notice.
- In 1.x the notice runs for **at least two minor releases** before the
  removal. The support and backport window is tracked in
  [#718](https://github.com/allamiro/vtop-engine/issues/718).
- A notice has four parts, all in the release that announces it: a row in
  the table below, the release notes, a warning where the surface is used
  (at startup for a config key, on stderr for a flag, and in
  `vtopctl validate` for both), and this page.

| Surface | Deprecated in | Earliest removal | Replacement |
|---|---|---|---|
| `manifest.write_version: "0.3"` (planned) | v0.12.0, when `0.4` becomes the default | 2.0 | `"0.4"` |
| The manifest field `verification_status` (on `main`, in no release yet) | v0.10.0 (#671 release B) | Not set. It is still written, also in `0.4` manifests, because v0.8.0 readers require it; its value is always `not_verified`. | `verification_plan` in a `0.4` manifest, and the ledger or a verification receipt for what verification found ([protocol §11.2](../VTOP_PROTOCOL_DRAFT.md#112-field-reference)) |
| The `CancelRetention` metadata command, and `vtopctl meta cancel-retention` that proposes it | v0.1.0 (#184). The subcommand's help says "Deprecated", and the state machine rejects every proposal ("retention cannot be cancelled after deletion is authorized"). | Not scheduled. The command stays on the wire so a peer of any release decodes it. | None. A `RetentionPlanned` segment is terminal: recovery completes the retention, or a repair establishes fresh verified placement. |

## Config, JSON output and the wire, before 1.0

- **Config.** Loading stays lenient through 0.x: the engine's top-level
  `config.yaml` sections ignore a key they do not know, while the cluster
  role configs and some nested sections refuse one: `manifest.mac` and its
  `keys`, `upload.object_lock`, `upload.transports.<name>`, the state-store
  `env`/`file` references, and a stream's `checksum` in `streams.yaml`. A
  `config_version` key and a `vtopctl validate --strict` that refuses
  unknown keys are planned.
  Until then, each release's notes list the new keys an older engine
  silently ignores.
- **JSON output.** `validate --print-effective` carries
  `effective_version: 1`, audit records carry `audit_version: 1` and ledger
  digests carry `vtop_ledger_digest: 1`. The other `--json` reports will
  gain a `report_version` field, an additive change (planned for v0.11.0).
- **Native protocol.** The rule that a new message kind needs a minor bump,
  and a sender that checks the peer's minor, starts at 1.0. Until then, a
  new kind is added under minor 0, an N-1 peer rejects it as unknown, and it
  is used only once every node runs N.
- **Cluster plane.** The metadata, native and replica formats (inventory
  rows 15 to 21) are classified here but not frozen. Whether they are frozen
  at 1.0 follows the 1.0 scope decision
  ([#740](https://github.com/allamiro/vtop-engine/issues/740)).

## Inventory

Every persisted and wire format, its version marker, its class and its
status now and at 1.0. The progress-marker JSON key names `path` and
`end_byte`, and for a Kafka marker `topic`, `partition`, `consumer_group`,
`end_offset` and `topic_id`, are frozen as they are: PostgreSQL indexes read
them.

The Kafka marker's `topic_id` (the broker's UUID for the topic, base64url
without padding) is optional and **ledger only**: a manifest never carries it
(`for_manifest` clears it, as for a file marker's fingerprints), so manifests
are byte for byte what every released reader verifies. A row written before
the field existed has none and reads as unknown, and so does a row written
against a broker that gives topics no id (before Kafka 2.8) or did not answer.
The ledger resumes a Kafka partition from the highest committed end among the
rows of one topic id; rows with no id keep the older rule (the newest row), and
the two kinds are never ranked against each other. An older release ignores the
key, resumes by the older rule and prunes by it; going back to it restores
that rule and the duplicate it allows after a failed source commit.

The table is generated from the format registry,
[`scripts/compat/formats.yaml`](../../scripts/compat/formats.yaml), which also
names the source constants behind each format.
[`scripts/check-format-registry.py`](../../scripts/check-format-registry.py)
runs on every pull request and fails when the sources hold a magic, or a
version, schema, protocol or feature-level constant, that no entry names;
when a listed value changes (a version bump, a new feature level, a new
metadata command kind) without the registry changing with it; or when this
table differs from what the registry generates. Edit the registry and run
`scripts/check-format-registry.py --write`; never edit the table by hand.

<!-- inventory:begin -->
<!-- Generated by scripts/check-format-registry.py --write from scripts/compat/formats.yaml. Edit the registry, not this table. -->
| # | Format | Version marker | Class | Status now | At 1.0 |
|---|---|---|---|---|---|
| 1 | Manifest | `version` `0.1` to `0.4`; writes `0.3`, `0.4` opt-in | A | stable reader, evolving writer | reads `0.1` to `0.4` forever, writes `0.4` |
| 2 | Verification receipt | `receipt_version` `1` | A | evolving | stable |
| 3 | Object key layout | none: the key's shape | A | partition path stable, file-name scheme evolving | stable |
| 4 | Ledger digest (`ledger-digest-*.jsonl`) | `vtop_ledger_digest` `1` | A | evolving | stable, read forever |
| 5 | State ledger, SQLite | `PRAGMA user_version` `1` | B | evolving | stable |
| 6 | State ledger, PostgreSQL | schema marker `3` | B | evolving | stable |
| 7 | Progress markers (ledger, digests, manifests) | none: optional fields (Kafka `topic_id`: ledger only, never in a manifest) | B | evolving; key names frozen | stable |
| 8 | `audit_events` table and hash chain | `audit_version` column | B | evolving | stable |
| 9 | Audit record (JSONL) | `audit_version` `1` | B | evolving | stable |
| 10 | Segment log v1 and v2, index, chunk sidecar, commit marker | `VTOPSEG1`, `VTOPREC1`, `VTOPIDX1`; `VTOPSEG2`, `VTOPREC2`; `VTOPCHK1`; `VTOPCMT1` | B | v1 stable, v2 evolving | v1 stable; v2's status and the release that makes it the default writer are decided in their own issue |
| 11 | Retention and truncate intents, producer snapshot | `VTOPRIN1`, `VTOPTIN1`, `VTOPPRD1` | B | evolving | classified at the freeze |
| 12 | Broker journals: fencing epochs, lineage, epoch, kept dispute, committed floor | `VTOPFNC1`, `VTOPFLN1`, `VTOPEPC1`, `VTOPKDS1`, `VTOPFLR1` | B | evolving | classified at the freeze |
| 13 | Multipart session (`*.multipart.json`) | `schema_version` `1` | B | evolving | stable |
| 14 | Scrub cursor | `vtop_scrub_cursor` `1` | B | evolving | classified at the freeze |
| 15 | Metadata log chunk and entry frames | `VTOPMLG1`, `VTOPMLE1` | B | evolving | follows #740 |
| 16 | Metadata command set | command kinds, feature levels 3 to 8 | B | evolving, level-gated | follows #740 |
| 17 | Metadata snapshot and state encoding | `VTOPMSN1` v1 and v2, payload version `1` | B | evolving, level-gated | follows #740 |
| 18 | Metadata hard state, applied, purged, membership log id | `VTOPMHS1`, `VTOPMAP1`, `VTOPMPG1`, `VTOPMML1` | B | evolving | follows #740 |
| 19 | Native protocol | `VTPW`, major `1`, minor `0` | C | evolving | follows #740 |
| 20 | Replica protocol negotiation | replica protocol `1` and `2` | C | evolving | follows #740 |
| 21 | Metadata peer and admin transport | `VTPM`, version `1` | C | evolving | follows #740 |
| 22 | Kafka gateway: advertised APIs and version ranges | none | C | experimental | experimental |
| 23 | `config.yaml` | none (`config_version` planned) | D | evolving | classified at the freeze |
| 24 | `vtopctl validate --print-effective` | `effective_version` `1` | D | JSON stable, text for people | JSON stable |
| 25 | `vtopctl` JSON output and exit codes | none yet (`report_version` planned) | D | evolving | classified at the freeze |
| 26 | Metric names and labels | none | D | evolving | a stable subset |
| 27 | Helm values | the chart `version` | D | evolving | classified at the freeze |
<!-- inventory:end -->

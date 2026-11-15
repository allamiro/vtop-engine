# Versioning and support

Which VTOP releases receive fixes, what a release may change, and what 1.x
will add to that. What each persisted and wire format promises, the rules for
changing one, upgrade and downgrade, and the deprecation table are in the
[compatibility policy](reference/compatibility.md); this page summarises the
parts an operator choosing a release needs, and links there for the rest.

"Support" here means which releases receive fixes. The project offers no
service-level agreement and no response-time promise.

## Where the project stands

VTOP is a **prototype / reference implementation**. Every release is signed,
attested and published as a full GitHub release; only a version with a
pre-release suffix, such as `1.0.0-rc.1`, would be published as a
pre-release. A 0.x release makes no promise across minor versions beyond the
rules in the next section.

## 0.x: the rules in force now

### Which releases get fixes

- **Supported:** the latest 0.x minor only. A fix lands on `main` and ships
  in the next release.
- **Backported:** data-safety and security fixes, to the latest minor's
  patch line, when `main` holds changes that should not ship in a patch.
  Nothing is backported to an earlier minor: upgrade to the latest one.

### What a minor may break

A 0.x minor may change config keys, CLI output, metrics and formats, but
never silently: the release notes in the [roadmap](ROADMAP.md) name every
such change and every one-way step. The
[compatibility policy](reference/compatibility.md) bounds it:

- **Reader first.** A release that can read a new format ships before the
  release that writes it by default
  ([reader first](reference/compatibility.md#reader-first)). For manifests,
  v0.8.0 reads `0.4`, v0.10.0 adds an opt-in writer, and `0.4` becomes the
  default no earlier than v0.12.0.
- **One-way steps are taken by an operator action**, such as raising a
  feature level, running `vtopctl migrate` or setting a write version, not by
  a binary on first open. The SQLite ledger has no such gate yet: it migrates
  on every open
  ([change rules](reference/compatibility.md#change-rules-within-1x)).
  Each release's one-way changes are listed in its roadmap section.
- **Mixed versions: N-1 and N.** A fleet may mix adjacent minors during a
  rolling upgrade; two minors apart is not supported. Before 1.0 a new
  native message kind is used only once every node runs the new release
  ([before 1.0](reference/compatibility.md#config-json-output-and-the-wire-before-10)).
- **Downgrade is not supported and not tested.** A newer release may write
  data an older one cannot read. Going back means a fresh directory, or a
  verified backup taken before the upgrade ([Upgrades](operate/upgrades.md)).
- **To 1.0 through the bridge release**, the last 0.x minor: upgrade to it
  first ([upgrade](reference/compatibility.md#upgrade)).

### Deprecation

A removal needs one minor of notice before 1.0. The notice is a row in the
[deprecation table](reference/compatibility.md#deprecation), a line in the
release notes, and a warning where the surface is used. A removal made for
security or data safety is stated in that release's notes instead.

## 1.x: the commitments made at 1.0.0-rc.1

> [!NOTE]
> This section takes effect at **1.0.0-rc.1**, not before. Until then the
> 0.x rules above apply.

- **A patch** changes no format of any kind: no new marker, default, feature
  level or one-way step. It fixes behaviour.
- **A minor** adds: an optional field, a config key with a safe default, a
  metric label value. It renames, removes or repurposes nothing stable
  ([change rules](reference/compatibility.md#change-rules-within-1x)).
- **Only a major** removes something stable, after a deprecation notice of
  **at least two minor releases**.
- **Archive artifacts** (manifests, verification receipts, object keys,
  ledger digests) written by any release stay readable.
- **Experimental surfaces are outside the promise**, as the
  [inventory](reference/compatibility.md#inventory) marks them.
- **Downgrade** follows the
  [downgrade rules](reference/compatibility.md#downgrade), which apply to a
  pair of releases only once the compatibility matrix records a result for
  it. Until then, downgrade is not supported.

## The 1.x support window: to be decided by the maintainer

> [!WARNING]
> How long a 1.x release line receives fixes is **to be decided by the
> maintainer** ([#718](https://github.com/allamiro/vtop-engine/issues/718)).
> The table below is a proposed default, not a commitment.

| Release line | Proposed: receives | Proposed: for how long |
|---|---|---|
| The latest 1.x minor | every fix | until the next minor ships |
| The previous 1.x minor | security and data-safety fixes | 6 months after the next minor ships |
| The last minor of the previous major | security and data-safety fixes | 12 months after the next major ships |

Backports are fixes only: no features and no format changes. Nothing else on
this page depends on the numbers in this table.

## Reporting a problem

- **A security vulnerability:** report it privately through a
  [security advisory](https://github.com/allamiro/vtop-engine/security/advisories/new),
  never as a public issue.
- **Suspected data loss or unexpected duplicates:** open a
  [data-safety report](https://github.com/allamiro/vtop-engine/issues/new?template=data_safety.yml).
  Keep the evidence first: copy the state ledger before pruning, deleting or
  resetting anything. Confirmed data-safety bugs are listed in the
  [data-safety register](reference/data-safety-register.md).

## What this page does not cover

- Benchmarks and the measured numbers, which describe the conditions they
  were measured under, not a promise.
- The Docker Compose lab and its observability stack, which are for
  development and evaluation.
- Experimental surfaces, and behaviour the documentation calls undefined.

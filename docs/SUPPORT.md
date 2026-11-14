# Versioning and support

What a VTOP release promises: which releases get fixes, what a release may
change, how a removal is announced, and what 1.x will add to that. The rules
for formats come from the compatibility policy in
[#743](https://github.com/allamiro/vtop-engine/issues/743) and its
[decisions](https://github.com/allamiro/vtop-engine/issues/743#issuecomment-6884402733).
The tests that enforce several of them are still being built there; where a
rule is not yet tested, this page says so.

"Support" here means which releases receive fixes. The project offers no
service-level agreement and no response-time promise.

## Where the project stands

VTOP is a **prototype / reference implementation**. Every release is signed,
attested and published as a full GitHub release; only a version with a
pre-release suffix, such as `1.0.0-rc.1`, is published as a pre-release. A
0.x release makes no promise across minor versions beyond the rules in the
next section.

## 0.x: the rules in force now

### Which releases get fixes

- **Supported:** the latest 0.x minor only. A fix lands on `main` and ships
  in the next release.
- **Backported:** data-safety and security fixes, to the latest minor's
  patch line, when `main` holds changes that should not ship in a patch.
  Nothing is backported to an earlier minor: upgrade to the latest one.

### What a minor may break, and the rules that bound it

A 0.x minor may change config keys, CLI output, metrics and formats, but
never silently: the release notes in the [roadmap](ROADMAP.md) name every
such change, and every one-way step. These rules bound it:

- **Reader first.** A release that can read a new format ships before the
  release that writes it by default. The manifest is the example: v0.8.0
  reads `0.4`; the `0.4` writer, on `main` for v0.10.0, writes it only when
  `manifest.write_version: "0.4"` is set; and `0.4` becomes the default
  writer no earlier than v0.12.0.
- **No automatic one-way steps.** A step after which an older release can no
  longer use the data is always taken by an operator action (raising a
  feature level, running `vtopctl migrate`, setting a write version), never
  by a binary on first start. An exception needs a written sign-off in that
  release's notes. This was decided in #743; earlier releases did not all
  follow it, and each release's one-way changes are listed in its roadmap
  section.
- **Wire changes: N-1 and N.** Nodes of adjacent minors run together during
  a rolling upgrade; a mixed fleet two minors apart is not supported. The
  live-chaos suite tests this for the metadata tier (scenario 22 replaces
  voters of the previous release one at a time); the other planes are not
  yet tested this way.
- **Downgrade is not supported and not tested.** A newer release may write
  data an older one cannot read. Going back means a fresh directory, or a
  verified backup taken before the upgrade
  ([Upgrades](operate/upgrades.md)).
- **The bridge release.** The last 0.x minor, not yet named, will read every
  0.x ledger it is upgraded from, and 1.0 will read the bridge release's
  state. To reach 1.0 from 0.x, upgrade to the bridge release first.

### Deprecation in 0.x

A config key, CLI command or flag, JSON field or metric that is renamed or
removed is listed in [DEPRECATIONS.md](DEPRECATIONS.md) and named in the
release notes at least one minor before it goes, where a one-minor warning is
possible. A removal made for security or data safety is stated in that
release's notes instead.

## 1.x: the commitments made at 1.0.0-rc.1

> [!NOTE]
> This section takes effect at **1.0.0-rc.1**, not before. Until then the
> 0.x rules above apply.

### Semantic versioning

- **A patch** changes no format, config key, CLI flag, JSON field or metric
  name. It fixes behaviour only.
- **A minor** is additive: a new optional key, flag, field, metric or label
  value, each with a default that leaves an older setup unchanged.
- **Only a major** removes or breaks something stable.
- **Archive artifacts** (manifests, verification receipts, ledger digests,
  object keys) written by any release stay readable until 2.0.
- **Wire:** N-1 and N during a rolling upgrade.
- **Experimental surfaces are outside the promise.** The format inventory in
  #743 classes the Kafka gateway's wire surface as experimental; it may
  change in any minor, with a release-note entry.

### No automatic one-way steps

In a minor, a one-way step is always gated by an operator action. An
exception needs a written sign-off in that release's notes.

### Deprecation in 1.x

- A deprecation notice runs for **at least two minor releases** before the
  item is removed.
- The notice is an entry in [DEPRECATIONS.md](DEPRECATIONS.md), a line in the
  release notes, and a runtime warning where the engine reads the item (a
  config key, for example).
- A stable item is removed only in a major release, after the two minors
  have passed.
- Already decided: `manifest.write_version: "0.3"` stays accepted through
  1.x, with a deprecation warning once `0.4` is the default writer.

### Downgrade in 1.x

Not promised. #743 plans tested downgrade between adjacent minors, up to the
first one-way step; until those tests exist, downgrade is not supported.

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
  resetting anything.

## What this page does not cover

- Benchmarks and the measured numbers, which describe the conditions they
  were measured under, not a promise.
- The Docker Compose lab and its observability stack, which are for
  development and evaluation.
- Experimental surfaces, and behaviour the documentation calls undefined.

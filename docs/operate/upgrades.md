# Upgrades

VTOP is at 0.x: a minor version may break compatibility. Two rules hold for
every upgrade.

- **Upgrades are tested.** CI checks that a data directory written by the
  newest patch of each released minor series (v0.2 onwards) opens under the
  current build, serves its records byte for byte, and still serves them after
  a restart ([`scripts/upgrade-compat.sh`](../../scripts/upgrade-compat.sh)).
- **Downgrades are not supported.** A newer release may write data an older
  one cannot read, and nothing tests that path. Going back means a fresh
  directory, or a verified backup taken before the upgrade.

The source for this page is the release notes in the [roadmap](../ROADMAP.md);
read the section for the release you are moving to.

## Upgrading from v0.7.0 to v0.8.0

### 1. Upgrade every node first

Replace the binaries or image on every archive engine, every metadata voter
and every data node before you raise the feature level or set any of the new
keys below.

On Kubernetes, install chart `0.5.1` from the v0.8.0 checkout (its
`appVersion`, and so its default image tag, is `0.8.0`) with `helm upgrade`,
or set `image.tag: "0.8.0"`.

### 2. Then raise the cluster feature level

Cluster plane only. v0.7.0 supports feature levels up to 5 and v0.8.0 up to 7.
Raising is permanent: there is no lowering.

```bash
# What every registered node reports it runs.
vtopctl meta node-versions --config admin.yaml

# Raise once every metadata voter runs v0.8.0.
vtopctl meta enable-features --level 7 --config admin.yaml
```

Level 7 lets data nodes report their release to metadata. From then on, a
raise above 7 (in a later release) is refused while any counted node has not
reported that level, and the refusal names each one. `node-versions --level N`
answers whether level `N` could be raised now, and exits non-zero if not.

### 3. Then use the new settings

Engines older than v0.8.0 silently ignore these keys, so set them only once
every engine runs v0.8.0:

| Key | What it does |
|---|---|
| `upload.request_min_bytes_per_second` | A request body's deadline grows with its size. |
| `upload.max_egress_bytes_per_second` | An operator ceiling on upload bandwidth. |
| A stream's `checksum` block | Per-stream checksum algorithm. Keep the global `checksum.algorithm` strong during the rollout. |

### One-way changes

After these, a v0.7.0 binary can no longer use the data:

- Once the engine writes a `SUPERSEDED` row, the state store carries schema
  marker 3 and v0.7.0 refuses it.
- An older binary refuses to start while a kept dispute is pending.
- Fencing journal and sidecar rewrites stage through `<file>.rewrite` and a
  rename.

### Manifests

v0.8.0 still writes manifest version `0.3`. Its readers check the version
first, read `0.1` to `0.4`, and report a newer manifest as newer rather than
as tampered: recovery leaves such a batch VERIFIED and pending, and
`vtopctl scrub` reports `unsupported_version`. Readers before v0.8.0 cannot
read `0.4`, so upgrade every engine and auditor before a later release turns
`0.4` on. The details are under
[v0.10.0 and beyond](../ROADMAP.md#v0100-and-beyond) in the roadmap.

## Before any upgrade

- Read the release's section in the [roadmap](../ROADMAP.md), including its
  known limitations.
- [Verify the release](../RELEASE_VERIFICATION.md) you are installing.
- Back up the state ledger (the SQLite file and its `.ledger-digests`
  directory, or the PostgreSQL database), since you cannot downgrade.
- On PostgreSQL, the engine never changes the schema itself. Run
  `vtopctl migrate` from the new release with the migration identity before
  the new engines start, as in the
  [deployment sequence](../POSTGRES_DEPLOYMENT.md#deployment-sequence).
- Stop each engine with SIGTERM and let its shutdown flush finish.

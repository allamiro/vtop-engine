---
name: vtop-release
description: How a vtop-engine release is cut, as it was done for v0.8.0 (#676) — the version-bump PR (Cargo.toml, Cargo.lock, Helm chart, ROADMAP upgrade notes, CITATION.cff), the squash merge pinned to the head SHA, the annotated tag on the merge commit that makes release.yml publish, and the checks on the published assets (SHA256SUMS, Sigstore bundles, the macOS otool check). Also the bug-only release rule, how to write one-way-change notes, and why a published tag is never moved. Use when preparing, reviewing or publishing a release, or when asked what is in the next one.
---

# Cutting a vtop-engine release

Ground truth: `.github/workflows/release.yml` (a push of a `v*` tag publishes), `docs/RELEASE_VERIFICATION.md` (what users check), `scripts/release-notes.sh` (the changelog it generates), and the history: v0.7.0 is #605, v0.8.0 is #676 (`git show 6669b3b`). Process rules (branch, PR, no AI attribution) are in `vtop-contributing`. The one-way-change reasoning is `ddia-encoding-evolution`.

## Rules that do not bend

1. **A release is a bump PR and a tag, nothing else.** No feature rides in the bump PR.
2. **Squash-merge the bump PR, then tag the merge commit.** The tag must point at the commit whose `Cargo.toml` says the version: `release.yml` fails the run if the tag and the workspace version differ (the "Derive version" step).
3. **Never re-tag a published release.** The signatures name the tag (`.../release.yml@refs/tags/vX.Y.Z`), the image and archives were built from it, and users verified against it. A bad release is fixed forward with the next patch version. Do not delete, move or re-push a `v*` tag, and do not replace or edit a release's assets.
   - The one precedent for moving a tag is v0.7.0, and nothing had been published: its first run (on the bump commit `af867f7`) was cancelled when GitHub retired the macOS runner the workflow named (#608), and the tag was cut again on `6c8f386` (#609) after two `workflow_dispatch` dry-runs. The first GitHub release for v0.7.0 came from the second run. That is a reason to dry-run first (below), not a licence to move tags.
4. **Downgrade is not supported and not tested** (`docs/ROADMAP.md`, "Versioning"). Say so in every upgrade note; never imply a rollback path.
5. **Bug-only release rule.** A milestone can be declared bug-only: v0.9.0 is ("only issues labelled bug, and the PRs that fix them. Features and measurements go to v0.10.0"). While such a milestone is open: merge only a PR that fixes a bug in it; hold every feature PR (the ones for the next milestone say "do not merge until v0.9.0 is tagged"); an issue that is a feature in disguise moves to the next milestone instead of holding the tag. After the tag, features merge. Check `gh api "repos/allamiro/vtop-engine/issues?milestone=N&state=open"` for N empty and every PR in it merged before cutting the bump.

## 0. Before the bump

- `git fetch origin --tags`; list what the release carries: `git log --first-parent --oneline vPREV..origin/main` and `./scripts/release-notes.sh <tag> <previous-tag>` once a tag exists (it groups by the squash subject's `area:` prefix and uses the API to tell PRs from issues).
- The milestone is empty of open issues and PRs, or each remaining one is moved on deliberately.
- `main` is green at the commit you will bump from (`gh api repos/allamiro/vtop-engine/commits/<sha>/check-runs`). The v0.8.0 bump rested on "the `upgrade compat` and `live chaos` jobs, both green on main at 2ad1d68".
- **Did `.github/workflows/release.yml`, `docker/` or `.cargo/config.toml` change since the last tag?** Then dry-run the pipeline before tagging: Actions, Release, Run workflow, with `tag` set to the previous tag (and the branch if the fix is not on `main`). A dispatch builds and tests everything but publishes nothing (the image-manifest and release jobs are skipped for `workflow_dispatch`). This is how #608 and #609 were checked.

## 1. The bump PR

Branch from `origin/main` (`release/vX.Y.Z-bump` was used for 0.8.0, `release/0.7.0` for 0.7.0). Title `release: bump the workspace to X.Y.Z`. Four files, plus the citation file:

| File | Change |
|---|---|
| `Cargo.toml` | `[workspace.package] version`, and the `version =` on the ten `vtop-*` path dependencies (eleven edits). Nothing else. |
| `Cargo.lock` | `cargo update --workspace`. The diff must be exactly the **twelve** `vtop-*` entries and no third-party package; if anything else moved, revert it. Then `cargo check --workspace --locked`. |
| `helm/vtop/Chart.yaml` | `appVersion: "X.Y.Z"` (it must track the workspace version; GHCR tags follow it). The chart's own `version` follows its contract, not the app: **patch** bump when no template or values changed since the last tag, **minor** when they did (a values-contract change). Check with `git diff vPREV..origin/main --stat -- helm/vtop/templates helm/vtop/values.yaml helm/vtop/ci`. Add the matching comment line above `version:` (0.5.1 for 0.8.0: "no template or values change — the bump carries appVersion 0.8.0"). Nothing in CI ties `appVersion` to `Cargo.toml`, so review it by eye. |
| `docs/ROADMAP.md` | A section for the release (below), and the forward section renamed to the next release. |
| `CITATION.cff` | `version` and `date-released` (the day the tag is pushed, as `YYYY-MM-DD`; v0.8.0 is `2026-11-12`). If the tag goes out on another day, fix the date in a follow-up PR; the tag does not move. |

### The ROADMAP section

`## vX.Y.Z — <what the release is about>`, then groups of bullets with the issue or PR numbers (the 0.8.0 groups were Archive engine, Upload, Manifests, Cluster plane, Measured), then **Upgrading from vPREV**:

- **Order.** For v0.8.0: upgrade every engine and data node first, then raise the feature level (`vtopctl meta node-versions` shows what each node reports, `vtopctl meta enable-features` raises it; a level above 7 waits for every node's report, #634).
- **One-way changes**, each with the reason an older binary refuses: a schema marker bump (#567: marker 3, v0.7.0 refuses), a refusal to start while a state exists (#635 kept disputes), a rewrite older code cannot read (#640 staged rewrites). Find them by reading every merged PR's "Compatibility and upgrade impact" section (the PR template has one) and grepping the range for `marker`, `one-way`, `older binary`, `refuses`.
- **New keys older engines silently ignore.** Most `config.yaml` sections have no `deny_unknown_fields`, so an older engine ignores a new key without a word (the cluster-plane role configs refuse them instead). List each, say whether ignoring it is harmless, and tell operators to upgrade every engine before relying on it.
- **Known open**: issues that did not make it, by number, so the notes do not promise them.
- **Downgrade is not supported.**

Also retire a stale "Next release" block at the top of the file if what it describes has shipped.

### The PR body

Mirror #676: the four files and what changed in each, the chart version and why, that CI runs the upgrade checks, and "After merge, tag `vX.Y.Z` on the merge commit; `release.yml` triggers on `v*` tags and publishes". Link no AI tool and add no trailer (`vtop-contributing`).

## 2. CI green, then the squash merge pinned to the head

CI must be green **on the exact head you are merging**, and the head must contain current `main` (`vtop-contributing`, "The merge bar"). Then merge with REST, pinning the head, with the commit title and body written explicitly (the squash body for #676 is a short summary of the four files, not the PR text):

```bash
HEAD=$(gh api repos/allamiro/vtop-engine/pulls/N --jq .head.sha)
gh api -X PUT repos/allamiro/vtop-engine/pulls/N/merge \
  -f merge_method=squash -f sha="$HEAD" \
  -f commit_title="release: bump the workspace to X.Y.Z (#N)" \
  -f commit_message="<what the four files carry, in prose>"
```

`sha` makes GitHub refuse the merge if the branch moved after you checked it (HTTP 409), which is the point. Squash is the only method used for this repository's history.

## 3. The annotated tag

```bash
git fetch origin main --tags
git checkout --detach origin/main            # must be the squash commit, not a later one
git log -1 --format='%H %s'                  # "release: bump the workspace to X.Y.Z (#N)"
grep -m1 '^version' Cargo.toml               # version = "X.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

If another PR merged after the bump, tag the bump's own merge commit explicitly (`git tag -a vX.Y.Z -m "vX.Y.Z" <merge sha>`); the tag decides what ships. Tags are annotated (`git cat-file -t vX.Y.Z` prints `tag`), as v0.1.0 to v0.8.0 are.

The push starts `release.yml`: `verify` (fmt, clippy, tests, the tag-versus-`Cargo.toml` check), `binaries` on four native runners (linux x86_64 and aarch64, macOS arm64 on `macos-15` and x86_64 on `macos-15-intel`), the container image for two platforms and its manifest, the SBOM, then `release`, which signs the archives and publishes. A plain `X.Y.Z` publishes as a full release (`prerelease=false`, as 0.8.0 did); only a hyphenated version such as `0.9.0-rc.1` is a pre-release. The run is not cancelled by a newer run (`cancel-in-progress: false`); if a job fails, fix forward and re-run the failed jobs of that run only while nothing is published.

## 4. Verify the published assets

`gh release view vX.Y.Z --json assets,isPrerelease` then check every item. v0.8.0 had ten assets: four `vtop-engine-X.Y.Z-<target>.tar.gz` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, `aarch64-apple-darwin`; a `.sigstore.json` bundle for each; `SHA256SUMS`; `vtop-engine-sbom.spdx.json`.

```bash
mkdir "$(mktemp -d)" && cd "$_"              # a fresh directory
gh release download vX.Y.Z
sha256sum --check --ignore-missing SHA256SUMS
for a in vtop-engine-X.Y.Z-*.tar.gz; do
  cosign verify-blob "$a" --bundle "$a.sigstore.json" \
    --certificate-identity "https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/vX.Y.Z" \
    --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
done
cosign verify ghcr.io/allamiro/vtop-engine:X.Y.Z \
  --certificate-identity "https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/vX.Y.Z" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

`SHA256SUMS` catches a corrupted or swapped download; the bundle says it came from this workflow at this tag. They answer different questions (`docs/RELEASE_VERIFICATION.md`), so check both.

**macOS binaries (from #679, in the first release after it).** The `binaries` job's step "Check macOS binaries load only system libraries" must have passed in the run log. Check the archives yourself too: extract each `*-apple-darwin` archive and run, for `vtop-node` and `vtopctl`:

```bash
otool -L vtopctl | tail -n +2 | awk '{print $1}' | grep -vE '^(/usr/lib/|/System/)'   # no output
otool -l vtopctl | grep -A3 -E 'LC_BUILD_VERSION|LC_VERSION_MIN_MACOSX' | grep -E 'minos|version'   # minos 11.0
```

A library outside `/usr/lib` and `/System` means a Homebrew dependency leaked back in; `minos` above 11.0 means `MACOSX_DEPLOYMENT_TARGET` was overridden. Either is a release defect: fix forward with a patch release.

Also: the image's `latest` tag moved (only for a non-pre-release), `helm/vtop/Chart.yaml` `appVersion` matches the image tag, and the release body's changelog lists the PRs you expect.

## 5. After the tag

- Close the milestone (`gh api -X PATCH repos/allamiro/vtop-engine/milestones/N -f state=closed`) once it is empty. Release notes are generated; the ROADMAP section is the narrative.
- If `date-released` in `CITATION.cff` is wrong, fix it by a PR.
- Features held for the next milestone may now merge.
- A defect found after publishing: open an issue in a new bug-only milestone, and release the next patch. Do not touch the published tag or assets.

## Checklist

- [ ] Milestone empty or deliberately moved; the bug-only rule respected.
- [ ] Bump PR touches only Cargo.toml (11 edits), Cargo.lock (12 `vtop-*` entries), Chart.yaml, ROADMAP.md, CITATION.cff.
- [ ] Chart version bump matches whether templates or values changed.
- [ ] ROADMAP: upgrade order, one-way changes with reasons, silently ignored keys, known open, no downgrade.
- [ ] CI green on the exact head; head contains current `main`; merged by REST with `sha` pinned.
- [ ] Tag is annotated, on the merge commit, and matches `Cargo.toml`.
- [ ] `release.yml` green; ten assets; `SHA256SUMS`, every Sigstore bundle and the image signature verified; macOS `otool` clean and `minos 11.0`.
- [ ] No tag moved, no asset replaced.

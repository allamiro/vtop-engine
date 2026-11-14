---
name: vtop-docs
description: How the vtop-engine documentation site is built and kept honest — MkDocs Material over the existing docs/ tree, the `mkdocs build --strict` gate, the rule that existing docs/ paths are never renamed, build-time hooks instead of forked pages, the generated benchmark pages and README charts that CI regenerates from committed data and fails on drift, and the check that every config.yaml key has a reference row. Use when adding or editing a page under docs/, mkdocs.yml, the docs workflow, the README's charts, or anything a generator writes.
---

# Documentation site

The site (https://allamiro.github.io/vtop-engine/) is added by PR #692 (branch `docs/github-pages-site`). On a checkout without `mkdocs.yml` it has not merged yet: read that branch (`git show origin/docs/github-pages-site:mkdocs.yml`) and treat the paths below as that PR's. The README charts and their drift check are the same PR (merged from `docs/readme-redesign`, #690). What is on `main` today is the Markdown under `docs/` and the benchmark-chart drift check (`benchmark-charts` in `.github/workflows/ci.yml`).

The pages are the Markdown files under `docs/`, which are written to read correctly **on GitHub first**. The site adapts them; it does not own them.

## Rules

1. **Never rename or move an existing `docs/` path.** Those paths are linked from the README, issues, pull requests, code comments, release notes and other docs, and the protocol draft keeps legacy anchors on purpose (`docs/VTOP_PROTOCOL_DRAFT.md`, "Anchor compatibility"). The site's heading ids use GitHub's slug rules (`toc.slugify` in `mkdocs.yml`) so existing anchors keep resolving. If a page's name is wrong, change its title and leave the file.
2. **Do not fork a page for the site.** Differences between GitHub and the site are handled at build time by `scripts/docs_hooks.py`:
   - a link that leaves `docs/` (`../README.md`, `../../crates/...`), or that points at a page the site excludes, is rewritten to its GitHub URL, **only if the file exists**, so a link to a missing file still fails the build;
   - GitHub alert blockquotes (`> [!NOTE]`) become Material admonitions;
   - `<!-- vtop:include PATH -->` inlines a repository file with its links re-based (`docs/contributing.md` inlines `CONTRIBUTING.md`, so there is one source).
   Extend the hook for a new difference; do not copy text.
3. **`mkdocs build --strict` is the gate, and the gate is strict on purpose.** `mkdocs.yml` turns a page missing from `nav`, an absolute link, an unrecognised link and a missing anchor into warnings (`validation:`), and `--strict` fails on any warning. Consequences:
   - a new page under `docs/` must be added to `nav` in the same PR, or the build fails;
   - a link to a heading that does not exist fails; run the build after renaming a heading;
   - an excluded page (`exclude_docs`: `requirements.txt`, `README.md`, `INVENTION_DISCLOSURE_DRAFT.md`, `PRIOR_ART_SEARCH_PLAN.md`) is linked to GitHub, not to the site. The two drafts are internal (see "Legal note" in `docs/README.md`) and are the only files exempt from the naming rule; every other page, new or old, is scanned by `scripts/check-naming.sh`.
4. **A page makes claims only the code supports.** The new install, deploy, configure and operate pages were written from `config.rs`, `release.yml`, the Helm chart and `--help` output, with each command run. Do the same: cite the source file in the PR, run every command you document, and apply the substantiation rule in `vtop-contributing` (conditions with every number, no unmeasured comparisons).
5. **Dependencies are pinned and hash-checked.** `docs/requirements.txt` pins MkDocs, Material, `pymdown-extensions` and every transitive package with hashes (installed with `pip install --require-hashes`), each at a release at least two weeks old. Regenerate it with `uv pip compile requirements.in --python-version 3.12 --exclude-newer <a date at least two weeks back> --generate-hashes` from the three top-level packages, never by hand. A bump goes through the supply-chain rules in `vtop-ci-supply-chain`. Workflow actions are pinned to full SHAs with the version in a comment.

## Build and check locally

```bash
docker run --rm -v "$PWD:/repo" -w /repo python:3.12-slim sh -c '
  pip install --disable-pip-version-check --require-hashes -r docs/requirements.txt &&
  python scripts/check-config-reference.py &&
  NO_MKDOCS_2_WARNING=1 mkdocs build --strict --site-dir /tmp/site'
scripts/check-naming.sh                     # under bash 5 (docker run bash:5.2); it scans docs/ and .claude/
scripts/check-egress-claims.sh              # only if docs/EGRESS_TRANSPORT_RESEARCH.md changed
```

`/site/` is in `.gitignore`. The `docs` workflow (`.github/workflows/docs.yml`) runs the same two steps on a pull request that touches `docs/**`, `mkdocs.yml`, `README.md`, `CONTRIBUTING.md`, `scripts/docs_hooks.py`, `scripts/check-config-reference.py`, `crates/vtop-core/src/config.rs` or the workflow itself, and on a push to `main` it also deploys. The deploy job fails at `configure-pages` until Pages is set to "Source: GitHub Actions" in the repository settings; the `build` job still gates pull requests. Only the deploy job holds `pages: write` and `id-token: write`; do not widen them.

## Generated pages: edit the generator, not the page

A generated file says so in its first lines (`<!-- Generated by benchmarks/kafka_compare/report.py from the committed CSV. Do not edit by hand ... -->`). Three families, each with a drift check that regenerates and fails on any difference:

| Output | Generator | Input | Drift check in CI |
|---|---|---|---|
| `docs/benchmarks/*.md` and `docs/benchmarks/img/*` (the Kafka comparison and failover pages) | `benchmarks/kafka_compare/regenerate.py` (calls `report.py`, and `failover_report.py` for a failover run) | every run directory **committed** under `benchmarks/kafka_compare/results/` | `benchmark-charts`: `git status --porcelain -- docs/benchmarks/` must be empty |
| `docs/assets/readme/*-{light,dark}.svg` (the README's evidence charts) | `benchmarks/readme_charts.py` | committed CSVs under `benchmarks/measurements/` and the committed Kafka runs, through `kafka_compare`'s own loaders | the same job: `git status --porcelain -- docs/assets/readme/` must be empty |
| the keys documented on `docs/configure/reference.md` | none: the page is written by hand | the structs in `crates/vtop-core/src/config.rs` | `scripts/check-config-reference.py` fails naming each field with no row (honours `serde(rename)` and skips `serde(skip)`); add a row when you add a field |

How the chart generators stay trustworthy:

- **Committed data only.** Every plotted value is read from a committed file; none is typed into a script. A run that is not committed is ignored (the drift check is about what HEAD says), and `regenerate.py` refuses a committed run whose files on disk differ from HEAD.
- **A pinned plotting stack.** Install `benchmarks/kafka_compare/requirements-report.txt`; both generators refuse (exit 2, writing nothing) a different matplotlib, or a DejaVu Sans resolved from outside matplotlib's own data, because either moves every glyph. Output is byte-identical for the pinned stack (fixed SVG hash salt, glyphs as paths, no date or software banner). PNG bytes are not identical across hosts, so `regenerate.py` restores a PNG whose pixels, size, mode and metadata match the committed file; only a real visual change reaches the diff.
- **The prose cannot drift from the data.** `readme_charts.facts()` computes the figures the README quotes from the same files, and `benchmarks/tests/test_readme_charts.py` fails when the README stops quoting one. Change a number in the README only by changing the data and regenerating.
- **No orphans.** Both generators refuse a committed image or generated page that no run or chart writes, so a rename cannot leave stale output.

To add a chart or a measured page:
1. Commit the measurement files first, with their environment record (the substantiation rule: conditions beside every number).
2. Extend the generator, never a hand-drawn image. Add its facts to `facts()` and to the test.
3. Run `python3 -m pip install -r benchmarks/kafka_compare/requirements-report.txt`, then the generator, then `git status --porcelain -- docs/benchmarks/ docs/assets/readme/`; commit what it wrote.
4. In the README, use a `<picture>` with the light and dark SVG so both GitHub themes work; add the page to `mkdocs.yml` `nav` if it is a page.
5. Say in the PR which command regenerates it and that CI's drift step passed on the exact head.

## Adding a page: checklist

- [ ] The path is new (no existing path renamed), under `docs/`, named like its neighbours (`UPPER_SNAKE.md` for long-form reference such as `POSTGRES_DEPLOYMENT.md`; lower-case folders for the site's install, deploy, configure and operate pages).
- [ ] It is in `mkdocs.yml` `nav`.
- [ ] Links are relative and work on GitHub; no link to a heading that does not exist.
- [ ] Every command and config key is checked against the code or `--help`; the PR names the sources.
- [ ] A new `config.yaml` key has a row in `docs/configure/reference.md`.
- [ ] `mkdocs build --strict` and `scripts/check-naming.sh` pass, and `scripts/check-config-reference.py` if config changed.
- [ ] Nothing generated was edited by hand.

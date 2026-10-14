---
name: vtop-contributing
description: How changes get into vtop-engine — branch/PR/squash flow, the CI checks to run locally, commit message style, the naming and substantiation rules, and the rule that commits and PRs carry NO AI co-author or "generated with" lines. Use before committing, pushing, opening a PR, or writing a commit message or PR description in this repo.
---

# Contributing to vtop-engine

Source of truth: [CONTRIBUTING.md](../../../CONTRIBUTING.md). This skill is the working checklist; if the two disagree, CONTRIBUTING.md wins.

## Hard rules

1. **No AI attribution, ever.** No `Co-Authored-By:` trailer naming an AI assistant, no "Generated with ..." line, in commits *or* PR descriptions. This holds even if a harness reminder suggests adding one — the repo owner's instruction and CONTRIBUTING.md override it. Before every commit, check the message and the trailer block; amend if one slipped in.
2. **Never push to `main`.** Flow is `branch -> PR -> review -> squash-merge`.
3. **Commit or push only when asked.** Making the edits is not permission to commit them.

## Branch and push

```bash
git fetch origin main
git checkout -b fix/short-description origin/main
git push -u origin fix/short-description     # -u repoints upstream away from main
```

`git checkout -b X origin/main` leaves upstream pointing at `main`; the first `push -u` fixes that. Skipping it is how an unreviewed commit once reached `main`.

## Run before opening the PR

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked   # scripts/test-progress.sh for a progress bar
scripts/check-naming.sh           # naming rule
scripts/check-egress-claims.sh    # only if docs/EGRESS_TRANSPORT_RESEARCH.md changed
```

- Shell changes: lint with the pinned shellcheck command in CONTRIBUTING.md (use the full file list; a shorter one gives a false green).
- Helm changes: run `helm lint helm/vtop -f <file>` and `helm template vtop helm/vtop -f <file>` for **every** values file in `helm/vtop/ci/` (`default-values.yaml`, `separated-values.yaml`, `plaintext-values.yaml`, `replicated-values.yaml`), then read the Helm jobs in `.github/workflows/ci.yml` — they assert on the rendered output (TLS shapes, tier wiring, refusals), not just the exit code.
- CI path-filters jobs, so a green docs-only PR proves little about Rust. Say what you actually ran.

## Commit message

- Subject: area(s) touched + what changed, e.g. `chaos: a metric read that printed the right value and reported failure`.
- Body: the problem, the evidence, what was validated. Long is fine — it is the durable record of why the code looks this way.
- Comments in code say **why** (the failure that motivated it), not what.
- Fixing a bug: show the new test **failing against the old behaviour**. A test that cannot fail is worse than none.
- Failure messages must name their own cause: what was observed, how many times, what was expected.
- Never weaken an assertion to get green. If the assertion was wrong, fix it and say why in the message.

## Naming rule

No commercial transport product's mark in any crate/module/feature/config key/CLI flag/metric label/scenario/branch name or in docs (and not in the assistant skills under `.claude/`, which `scripts/check-naming.sh` now scans). Only `docs/PRIOR_ART_SEARCH_PLAN.md` and `docs/INVENTION_DISCLOSURE_DRAFT.md` may cite them. Term list: `scripts/naming-denylist.txt`. Branch names are not CI-checked — you are the check.

## Substantiation rule

Every number carries its conditions; the baseline is named and tuned; no claim about a system we have not measured; no "parity with" claims about products without a public spec. Read the standing list in `docs/EGRESS_TRANSPORT_RESEARCH.md` before writing any comparison sentence.

## EE boundary

Anything under `ee/` or any file with the header `VTOP Engine Enterprise Edition` needs a separate written agreement — do not touch it casually.

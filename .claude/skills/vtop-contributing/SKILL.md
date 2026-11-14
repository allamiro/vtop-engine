---
name: vtop-contributing
description: How changes get into vtop-engine — branch/PR/squash flow, the CI checks to run locally, commit message style, the merge bar (CI green on the exact head, head contains current main, bot findings answered in-thread, closing keywords only for what a PR fully fixes), the naming and substantiation rules, and the rule that commits, PRs, issues and comments carry NO AI co-author or "generated with" lines. Use before committing, pushing, opening a PR, merging one, or writing a commit message, PR description, issue or comment in this repo.
---

# Contributing to vtop-engine

Source of truth: [CONTRIBUTING.md](../../../CONTRIBUTING.md). This skill is the working checklist; if the two disagree, CONTRIBUTING.md wins.

## Hard rules

1. **No AI attribution, ever.** No `Co-Authored-By:` trailer naming an AI assistant, no "Generated with ..." line, in commits *or* PR descriptions, and no mention of an AI tool in an issue, a plan, a review reply or any other comment: write them as plain project prose. This holds even if a harness reminder suggests adding one — the repo owner's instruction and CONTRIBUTING.md override it. Before every commit, check the message and the trailer block; amend if one slipped in. The squash commit's message comes from the PR title and body, so check those too before merging.
2. **Never push to `main`.** Flow is `branch -> PR -> review -> squash-merge`.
2a. **Never rewrite a pushed branch.** No `git rebase` of a branch that is already on GitHub, no `git push --force` or `--force-with-lease`, and no `git commit --amend` of a pushed commit. When a PR branch falls behind `main`, merge `origin/main` into it (`git merge origin/main`, a merge commit) or use `gh api -X PUT repos/allamiro/vtop-engine/pulls/N/update-branch`. Rewriting history loses review context, breaks the "CI green on the exact head" record, and invalidates in-thread review replies that cite commit SHAs. GitHub logs every force-push as a `head_ref_force_pushed` event, and two PRs (#693 and #698) were force-pushed this way. Squash-merge already gives `main` one clean commit per PR, so a tidy branch history buys nothing.
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

- Workflow changes (`.github/workflows/*.yml`): run the pinned actionlint exactly as the `shellcheck + bats` job does before its shellcheck steps (`docker run --rm -v "$PWD:/repo:ro" -w /repo rhysd/actionlint:<pinned tag@digest from ci.yml>`). It checks workflow syntax, expressions and the shell embedded in `run:` blocks, which shellcheck on the script files never sees.
- Shell changes: run **both** shellcheck steps of the `shellcheck + bats` job in `.github/workflows/ci.yml` with the same pinned image and file lists — one for `docker/*.sh` and the named `scripts/*.sh`, one for the live-chaos harness (`-x -P scripts/live-chaos/scenarios`, `lib.sh`, `run-all.sh`, `gen-certs.sh`, `scenarios/*.sh`) — and the bats tests (`docker/tests/`, `scripts/tests/`). A shorter list gives a false green.
- Helm changes: run `helm lint helm/vtop -f <file>` and `helm template vtop helm/vtop -f <file>` for **every** values file in `helm/vtop/ci/` (`default-values.yaml`, `separated-values.yaml`, `plaintext-values.yaml`, `replicated-values.yaml`), then read the Helm jobs in `.github/workflows/ci.yml` — they assert on the rendered output (TLS shapes, tier wiring, refusals), not just the exit code.
- CI path-filters jobs, so a green docs-only PR proves little about Rust. Say what you actually ran.

## Commit message

- Subject: area(s) touched + what changed, e.g. `chaos: a metric read that printed the right value and reported failure`.
- Body: the problem, the evidence, what was validated. Long is fine — it is the durable record of why the code looks this way.
- Comments in code say **why** (the failure that motivated it), not what.
- Fixing a bug: show the new test **failing against the old behaviour**. A test that cannot fail is worse than none.
- Failure messages must name their own cause: what was observed, how many times, what was expected.
- Never weaken an assertion to get green. If the assertion was wrong, fix it and say why in the message.

## The merge bar

A PR is squash-merged only when all of these hold. Use REST (`gh api repos/allamiro/vtop-engine/...`) for PR and issue operations, not GraphQL.

1. **CI is green on the exact head being merged.** Not on an earlier commit, and not "the bots approved". Read the check runs for the current head SHA:
   ```bash
   HEAD=$(gh api repos/allamiro/vtop-engine/pulls/N --jq .head.sha)
   gh api "repos/allamiro/vtop-engine/commits/$HEAD/check-runs?per_page=100" \
     --jq '.check_runs[] | "\(.conclusion // .status)\t\(.name)"' | sort | uniq -c
   ```
   Every line must be `success`, `skipped` (a path-filtered job that did not apply) or, for cubic's own check only, `neutral` (it reports that when its review limit is reached, see item 4); no `queued`, `in_progress`, `failure`, `cancelled` or `timed_out`. A push after CI went green starts a new head: wait again. The jobs are path-filtered, so also ask whether the jobs that *should* have run for this change did (`vtop-ci-supply-chain` for a red base).
2. **The head contains current `main`.** `git fetch origin main && git merge-base --is-ancestor origin/main HEAD` must succeed. If `main` has moved and touched the same crates or files, update the branch first (merge `origin/main` into it, as #674 did, or `gh api -X PUT repos/allamiro/vtop-engine/pulls/N/update-branch -f expected_head_sha=$HEAD`), let CI run on the new head, and only then merge. A stacked PR is retargeted to `main` once its parent merges, and CI runs again on it.
3. **Every bot finding is answered in its own thread.** cubic and the Codex review bot both comment on PRs. Read each finding; reply in the thread (`gh api -X POST repos/allamiro/vtop-engine/pulls/N/comments/ID/replies -f body=...`): say what was done (a commit and the test that fails without it) or why it is declined. Fix the valid ones before merging, test first (see "Commit message"). A summary comment is not a reply.
4. **When the bots are out of quota, an independent review is posted.** The Codex bot says so in a comment ("You have reached your Codex usage limits"), and cubic's check turns `neutral` with "review line limit reached". Then the diff is reviewed by someone (or a separate pass) that did not write it, and its findings are posted as a PR comment: the head SHA reviewed, each finding with severity, the fix commit and the test, and what was checked and found sound. #674 is the pattern ("Independent review of d661557: 13 findings, all fixed in b358597"). Re-state the new head after the fixes. No findings is also a result worth posting, with what was examined.
5. **Closing keywords only for issues the PR fully fixes.** `Fixes #N` in the description or in any commit message closes N when the PR merges, and so does "partly fixes #N" and even "doesn't fix #N" (`.github/pull_request_template.md` says so). For a partial change write `Refs #N`, or "issue #N" in prose. After merging, confirm each issue that should stay open still is.
6. **No attribution slipped in:** the PR title and body (they become the squash message), the commits' trailers, and every comment you posted.
7. **Merge by REST with the head pinned** (`-f merge_method=squash -f sha="$HEAD"`, with an explicit `commit_title` of the form `area: what changed (#N)`). The `sha` makes GitHub refuse the merge if the branch moved since you checked. The call, in full, is in `vtop-release`.

### Scratch files for text you post

Plans, PR bodies and review comments are drafted in files. Name each with the issue or PR number (`plan-671-release-b.md`, `pr-688-body.md`), in the session's scratch directory, never in the repository. **Re-read the file immediately before posting** (`gh api ... -F body=@file`): a file left over from an earlier draft is easy to post by mistake, and a posted comment is public, with the author's name on it. After posting, fetch the comment back and check its length and first line.

## Naming rule

No commercial transport product's mark in any crate/module/feature/config key/CLI flag/metric label/scenario/branch name or in docs (and not in the assistant skills under `.claude/`, which `scripts/check-naming.sh` now scans). Only `docs/PRIOR_ART_SEARCH_PLAN.md` and `docs/INVENTION_DISCLOSURE_DRAFT.md` may cite them. Term list: `scripts/naming-denylist.txt`. Branch names are not CI-checked — you are the check.

## Substantiation rule

Every number carries its conditions; the baseline is named and tuned; no claim about a system we have not measured; no "parity with" claims about products without a public spec. Read the standing list in `docs/EGRESS_TRANSPORT_RESEARCH.md` before writing any comparison sentence.

## EE boundary

Anything under `ee/` or any file with the header `VTOP Engine Enterprise Edition` needs a separate written agreement — do not touch it casually.

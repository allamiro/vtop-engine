---
name: vtop-ci-supply-chain
description: Diagnose and fix red CI and dependency/supply-chain failures in vtop-engine — telling a red base from a bad bump, reproducing failures in containers, cargo-deny/cargo-audit advisory handling and the audit.toml/deny.toml mirror, MSRV vs stable, dependabot PR triage, pinned actions, and lab image pins. Use when dependabot PRs are failing, when CI is red on main, when adding an advisory ignore, or when bumping a dependency.
---

# CI and supply-chain triage

Learned the hard way: four dependabot PRs (#546-#549) all failed the same checks — including a bump that only changed workflow action pins — because **`main` was already red**. Fixing each PR would have been wasted work.

## First question: is the base red?

1. `gh run list --workflow ci.yml --branch main --limit 5`, then compare the *failing jobs and their errors* with the PR's. A red `main` does not by itself stop other PRs going green — a PR can pass, or fail for its own reasons — but the **same job failing with the same error on `main`'s latest run** is an inherited failure: fix `main` first, then `@dependabot rebase` the rest.
   - **The supply-chain job can rot with no commit, so `main`'s last *run* can be stale-green.** `cargo audit` and `cargo deny` fetch the RustSec DB live at job time, and `ci.yml` runs only on `push` to `main` and on `pull_request` — there is no scheduled run. So a newly published advisory changes nothing visible at publication: `main`'s last recorded run stays green, and the **next** run that reaches the supply-chain jobs (a push, a PR, or a rerun) fails (in #590, `main`'s last run was green on Oct 20, yet a PR opened on Nov 9 failed both jobs on a diff that could not touch the lockfile). Trust a *fresh* run, not an old checkmark: read the newest PR's supply-chain run, or re-run CI on `main`. By the same token, an **older PR's green supply-chain mark is stale** — it reflects the DB the day it last ran and will flip red on `@dependabot rebase`. Only a PR whose changed paths all fall outside the `rust` filter is genuinely unaffected, because `audit` and `deny` are gated on `needs.changes.outputs.rust`. That filter includes `docker/**`, `.cargo/**`, `deny.toml`, `Cargo.*` and `.github/workflows/ci.yml`, so **Dockerfile and `ci.yml` Actions-pin bumps DO run supply-chain**; docs-only, `docker-compose.yml`, `observability/**` and `benchmarks/**` changes do not. Check the filter in `ci.yml` rather than guessing from the file type.
2. A PR that fails jobs its diff cannot touch (an Actions-pin bump failing `clippy`) is the signature of an inherited failure; confirm it by matching the error text against `main`'s run before acting.
3. Find the failing job and step first, so you read the right part of the log: `gh run view <run> --json jobs --jq '.jobs[]|select(.conclusion=="failure")|"\(.databaseId) \(.name)", (.steps[]|select(.conclusion=="failure")|"  step: "+.name)'` (the leading number is the job id for the next call), then fetch that job's log with `gh api repos/<owner>/<repo>/actions/jobs/<id>/logs | sed 's/\x1b\[[0-9;]*m//g'`. The endpoint returns the **whole** job log, so search it for the failing step's name or the first `error` rather than reading it top to bottom (`gh run view --log-failed` returned nothing for these runs).
4. Several independent causes can hide behind identical red marks: clippy, MSRV, deny and e2e failed here for four unrelated reasons.

## Reproduce before fixing (no local toolchain needed)

Use throwaway containers with a named cargo volume, never installing a toolchain on the host and never writing root-owned files into the tree (mount read-only, or `chown` after):
- MSRV: `docker run --rm -v "$PWD":/w:ro -v vtop-ci-cargo:/cache -w /w -e CARGO_HOME=/cache/home -e CARGO_TARGET_DIR=/cache/target-msrv rust:1.94.1 ...` (CI's MSRV is the `rust-version` in `Cargo.toml`).
- Stable: same with `rust:latest`, plus the packages CI installs for rdkafka: `apt-get install -y --no-install-recommends cmake clang libssl-dev libsasl2-dev libcurl4-openssl-dev zlib1g-dev pkg-config` (the list in `.github/workflows/ci.yml`; a shorter list can fail before the workspace compiles). **`docker pull rust:latest` first**: a cached `latest` lags GitHub's stable, and a newer stable adds deprecations that `clippy -D warnings` turns into errors. In #531 a stale local image passed clippy while CI failed on `AtomicUsize::fetch_update` ("renamed to `try_update`"). Neither name is usable: `fetch_update` is deprecated on stable and `try_update` is newer than the MSRV, so the repo's pattern is a `compare_exchange_weak` loop (see `MockUploadBackend::take_throttle` in `crates/vtop-upload/src/mock.rs`).
- Supply chain: download the `cargo-deny` **and** `cargo-audit` prebuilt binaries inside the container (no compile) and run both — CI runs them as two separate jobs and they fail for different reasons. Resolve each release URL from the GitHub API so you are not guessing a version:
  - `DENY_URL=$(curl -s https://api.github.com/repos/EmbarkStudios/cargo-deny/releases/latest | grep -o 'https://[^"]*x86_64-unknown-linux-musl.tar.gz' | head -1)`
  - `AUDIT_URL=$(curl -s https://api.github.com/repos/rustsec/rustsec/releases | grep -o 'https://[^"]*cargo-audit-x86_64-unknown-linux-musl-v[0-9.]*.tgz' | head -1)` (the `rustsec/rustsec` repo tags many crates; filter for the `cargo-audit` asset). `rust:*` images lack `curl` — `apt-get install -y curl` first.
  - `cargo deny check` must end `advisories ok, bans ok, licenses ok, sources ok`; `cargo audit` must exit 0 with no `error: N vulnerabilities found`.
- A fix is verified only when all of: `cargo audit` (exit 0), `cargo deny check` (all four sections ok), `cargo clippy --workspace --all-targets --locked -- -D warnings` (stable), `cargo check --workspace --all-targets --locked` (MSRV), and the touched crates' tests. Add `cargo fmt --check` whenever the change touches `.rs` files (a lockfile/TOML-only advisory fix does not, so fmt is moot there). Run the two heavy compiles (stable, MSRV) in the background with **separate** `CARGO_TARGET_DIR`s so they do not thrash one cache.

## Advisories: fix, don't ignore, unless there is nothing to fix

- **Decide fix-vs-ignore from the advisory's patched range against what the graph can actually reach, not a guess.** For each advisory: (a) get the authoritative patched requirement — `WebFetch https://rustsec.org/advisories/<ID>.html` (e.g. "patched `>=1.1.31`"); (b) see where the crate *can* move under its parents' pins — `cargo update -p <crate>` with no `--precise` lands on the newest version the existing requirements allow; (c) if that version crosses the patched threshold, it is a **fix**; if a parent caps it below the threshold, it is an **ignore**. Confirm reachability with `cargo tree -i <crate>` (who pulls it) before writing the justification.
- A vulnerability with a reachable patched release -> **update the lockfile** for just that crate: `cargo update -p <crate>` (pass `-p` for each; do not run a blanket `cargo update`). Verify nothing else moved with `git diff Cargo.lock`.
- **Keep the lockfile diff to crates that actually cross their patched threshold; revert incidental bumps.** `cargo update -p` can move a crate forward without reaching the fix (in #590 `libsqlite3-sys` 0.30→0.37, but the fix is 0.39, capped by `sqlx-sqlite 0.9`). A bump that clears no advisory buys no security and only adds churn and build risk — reset `Cargo.lock` and re-apply only the bumps that cross their thresholds (`cp` the pristine lockfile back, then `cargo update -p A -p B ...` for the real fixes).
- An *unmaintained* or *unsound* notice, or a vulnerability whose fix a parent pin blocks -> an ignore, with a comment recording: what the advisory is, why it is not exploitable here (how it is reached), and the **condition for removing the ignore** (name the upstream bump needed — "when `sqlx` depends on `libsqlite3-sys >=0.39`", "when the AWS/RustCrypto stack adopts `crypto-bigint >=0.7.6`"). Examples: `unsafe-libyaml` via `serde_yaml` (#550); `crypto-bigint`/`libsqlite3-sys`/`pin-utils` (#590).
- **Unmaintained advisories fail `cargo deny` but only *warn* `cargo audit`.** CI runs plain `cargo audit` (no `-D warnings`), so an `unmaintained`/`informational` notice prints as an "allowed warning" and does not fail that job — but `cargo deny`'s `[advisories] unmaintained` check raises `error[unmaintained]` and *does* fail. So such an advisory must be ignored in `deny.toml`; mirror it into `audit.toml` too for parity (as `unsafe-libyaml` RUSTSEC-2024-0449 and `pin-utils` RUSTSEC-2026-0441 both are). The failing job tells you which: match the `error[...]` lines in the `cargo deny` log against the `error: N vulnerabilities found` count in the `cargo audit` log — a crate in the first but not the second is unmaintained-only.
- `.cargo/audit.toml` is the single source of truth; `deny.toml` mirrors it. Change **both**, in the same commit. Known intentional exceptions are audit-only: `RUSTSEC-2023-0071` (rsa) and `RUSTSEC-2026-0235` (rkyv) are ignored in `audit.toml` but not in `deny.toml`, because cargo-deny's feature-aware graph never sees those crates (`.cargo/audit.toml` carries each exception's justification; `deny.toml` has a note explaining why rsa is absent). Anything else must be in both.
- **Every ignore you add or remove is also an edit to `docs/SECURITY_MODEL.md`**: its "Currently tracked" table is the list a reader consults when re-evaluating exceptions on a dependency bump, with a row per advisory carrying the rationale and the condition for removal. An ignore that is only in the TOML files is invisible to that review (review finding on #550).
- `advisory-not-detected` warnings mean an ignore has gone stale (the vulnerable dependency left the graph). They do not fail the job, but clean them up in a separate change once confirmed against `Cargo.lock` — do not remove them as a side effect of an unrelated fix.
- A `yanked` warning is advisory; usually `cargo update -p <crate>` resolves it.

## Dependency bumps

- Read the bump as a change to the code that uses it: a type's fields can move (syn 3), a function can be deprecated (std atomics on a new stable), a transitive MSRV can rise (the aws-smithy crates set this repo's floor). Build tests, not just the lib.
- Raising `rust-version` is a deliberate, documented decision (see the comment above it in `Cargo.toml`), not a way to make a bump compile.
- Group bumps (`cargo-minor-patch`) fail as a group; bisect by crate before blaming the group.

## Workflow and image pins

- GitHub Actions are pinned to full commit SHAs with the version in a trailing comment. A bump PR must keep that form; never replace a SHA with a mutable tag.
- Lab/benchmark container images are pinned by tag in `docker-compose.yml` and `benchmarks/docker-compose.benchmark.yml`. A registry can delete pinned tags: the `e2e smoke` job failed before the engine started because the pinned MinIO tags (and `latest`) no longer exist on quay.io. Check with `docker manifest inspect <image>` before assuming the code is at fault. Choosing a replacement S3-compatible backend is a design decision — surface it, don't swap silently.
- CI path filters skip heavy jobs for docs-only PRs; a green docs PR proves nothing about Rust.

## Case study & roadmap: the November 2026 advisory wave (#590)

**Intent.** Triage 13 open PRs; close what was ready, fix what was broken. The real work turned out to be one repo-wide blocker, not per-PR breakage.

**What was actually wrong.** The live RustSec DB had published six advisories since `main`'s last green run (Oct 20), so the `supply-chain` jobs were red on `main` and on every PR whose path filter ran them. The visible symptom was #588 — a pure GitHub-Actions-SHA bump — failing `cargo audit`/`cargo deny` on a diff that cannot touch `Cargo.lock`: the inherited-failure signature. Older PRs' green supply-chain marks were stale. The docker-compose image bumps were genuinely green because the `rust` path filter skips supply-chain for them; the `docker/Dockerfile` base-image bump (#541) was not in that group — `docker/**` is in the `rust` filter, so it ran supply-chain and its green was as stale as the Rust PRs' (its diff could not cause an advisory failure, which is what made it safe to merge, not a skipped job).

**How each advisory was resolved** (method: patched range vs. reachable version):

| Advisory | Crate | Patched | Reachable? | Action |
|---|---|---|---|---|
| RUSTSEC-2026-0385 | ctutils | ≥0.4.4 | 0.4.2→0.4.8 | fix (bump) |
| RUSTSEC-2026-0418 | libz-sys | ≥1.1.31 | 1.1.29→1.1.32 | fix (bump) |
| RUSTSEC-2026-0382 | rustls-webpki | ≥0.103.16 | 0.103.15→0.103.16 | fix (bump) |
| RUSTSEC-2026-0381 | webpki-roots | ≥1.0.10 | 1.0.8→1.0.11 | fix (bump) |
| RUSTSEC-2026-0387 | crypto-bigint | ≥0.7.6 (major) | pinned 0.5.x by aws-sigv4 / elliptic-curve | ignore |
| RUSTSEC-2026-0459 | libsqlite3-sys | ≥0.39.0 | capped <0.38 by sqlx-sqlite 0.9 | ignore |
| RUSTSEC-2026-0441 | pin-utils | none (unmaintained) | — | ignore (deny-fails / audit-warns) |

The three ignores went into **both** `.cargo/audit.toml` and `deny.toml` and a row each in `docs/SECURITY_MODEL.md` §13. Verified in containers: `cargo audit` exit 0, `cargo deny check` all sections ok, MSRV `cargo check` and stable `clippy -D warnings` both exit 0. The docker-compose image bumps skip supply-chain and were independently safe to merge; the Dockerfile and Actions/`ci.yml` bumps (#541, #588, #584) run it, so their marks had to be fresh — #588 and #584 needed `@dependabot rebase` **after** `main` was green (a rebase onto still-red `main` is wasted). If a grouped dependabot PR has not picked up `@dependabot rebase` after a few minutes (its head commit is unchanged), `@dependabot recreate` is the next thing to try — #588 was green after it, though which of the two commands did it was not confirmed.

**Roadmap — remove these ignores when their upstream cap lifts** (re-check on every AWS SDK / sqlx / RustCrypto bump; the condition lives beside each entry in `audit.toml`):
- RUSTSEC-2026-0387 (crypto-bigint): remove when the AWS signing / RustCrypto stack adopts `crypto-bigint >=0.7.6` (`elliptic-curve >=0.14`).
- RUSTSEC-2026-0459 (libsqlite3-sys): remove when `sqlx` ships a release depending on `libsqlite3-sys >=0.39` (SQLite 3.54).
- RUSTSEC-2026-0441 (pin-utils): remove when the AWS SDK (`aws-smithy-http` / `aws-smithy-runtime`) drops `pin-utils`.
- **Outstanding stale cleanup** (separate PR, deliberately not bundled with #590): `cargo deny` now prints `advisory-not-detected` for RUSTSEC-2026-0098/-0099/-0104 (legacy `rustls-webpki 0.101.x`) and -0253 (`lru`) — the vulnerable crates have left the graph. Confirm against `Cargo.lock`, then drop those ignores from `audit.toml`, `deny.toml`, and the `SECURITY_MODEL.md` table together.

## PR hygiene

Fix on a branch from `origin/main`, open a PR, never push to `main`. Commit messages explain the evidence and what was validated, with no AI attribution lines (see `vtop-contributing`). Say plainly what you did **not** fix. Merging is the maintainer's action — surface what is ready (and whether its green marks are fresh or stale) rather than assuming you can merge. Resolve a feature branch's conflicts by bringing `origin/main` in and keeping *both* sides of an **additive** conflict (two new dict keys, two new table rows — as in #527's `scenario.py` / `README.md`); a conflict that is a genuine contradiction, not two independent additions, is the author's call.

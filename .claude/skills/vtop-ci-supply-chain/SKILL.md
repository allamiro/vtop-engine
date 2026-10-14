---
name: vtop-ci-supply-chain
description: Diagnose and fix red CI and dependency/supply-chain failures in vtop-engine — telling a red base from a bad bump, reproducing failures in containers, cargo-deny/cargo-audit advisory handling and the audit.toml/deny.toml mirror, MSRV vs stable, dependabot PR triage, pinned actions, and lab image pins. Use when dependabot PRs are failing, when CI is red on main, when adding an advisory ignore, or when bumping a dependency.
---

# CI and supply-chain triage

Learned the hard way: four dependabot PRs (#546-#549) all failed the same checks — including a bump that only changed workflow action pins — because **`main` was already red**. Fixing each PR would have been wasted work.

## First question: is the base red?

1. `gh run list --workflow ci.yml --branch main --limit 5` — if `main`'s recent runs failed, nothing queued behind it can go green. Fix `main` first, then `@dependabot rebase` the rest.
2. A PR that fails jobs its diff cannot touch (an Actions-pin bump failing `clippy`) is the signature of a red base.
3. Pull the failing step, not the whole log: `gh run view <run> --json jobs --jq '.jobs[]|select(.conclusion=="failure")|.name, (.steps[]|select(.conclusion=="failure")|.name)'`, then `gh api repos/<owner>/<repo>/actions/jobs/<id>/logs | sed 's/\x1b\[[0-9;]*m//g'` (the `--log-failed` flag returned nothing for these runs).
4. Several independent causes can hide behind identical red marks: clippy, MSRV, deny and e2e failed here for four unrelated reasons.

## Reproduce before fixing (no local toolchain needed)

Use throwaway containers with a named cargo volume, never installing a toolchain on the host and never writing root-owned files into the tree (mount read-only, or `chown` after):
- MSRV: `docker run --rm -v "$PWD":/w:ro -v vtop-ci-cargo:/cache -w /w -e CARGO_HOME=/cache/home -e CARGO_TARGET_DIR=/cache/target-msrv rust:1.94.1 ...` (CI's MSRV is the `rust-version` in `Cargo.toml`).
- Stable: same with `rust:latest`, plus `apt-get install -y cmake libsasl2-dev libssl-dev pkg-config` for rdkafka.
- Supply chain: download the `cargo-deny` release binary inside the container and run `cargo deny check` (all four sections: advisories, bans, licenses, sources).
- A fix is verified only when all of: `cargo fmt --check`, `clippy --workspace --all-targets -D warnings` (stable), `cargo check --workspace --all-targets --locked` (MSRV), the touched crates' tests, and `cargo deny check`.

## Advisories: fix, don't ignore, unless there is nothing to fix

- A vulnerability with a patched release -> **update the lockfile** for just that crate: `cargo update -p <crate>` (pass `-p` for each; do not run a blanket `cargo update`). Verify nothing else moved with `git diff Cargo.lock`.
- An *unmaintained* or *unsound* notice with no fixed release -> an ignore, with a comment recording: what the advisory is, why it is not exploitable here (how it is reached), and the **condition for removing the ignore**. Example in tree: `unsafe-libyaml` via `serde_yaml`.
- `.cargo/audit.toml` is the single source of truth; `deny.toml` mirrors it. Change **both**, in the same commit. Known intentional exception: `RUSTSEC-2023-0071` (rsa) is ignored only in `audit.toml` because cargo-deny's feature-aware graph never sees the crate.
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

## PR hygiene

Fix on a branch from `origin/main`, open a PR, never push to `main`. Commit messages explain the evidence and what was validated, with no AI attribution lines (see `vtop-contributing`). Say plainly what you did **not** fix.

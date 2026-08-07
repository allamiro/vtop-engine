# Project conventions for AI assistants

## Commit messages — hard rule

**Never add attribution trailers to commits.** Do NOT include any of:

- `Co-Authored-By:` lines (no Claude, no AI tools)
- `🤖 Generated with ...` lines
- any "made/assisted by AI" footer

Write plain commit messages: a concise subject line, then an optional body
explaining *why*. Reference issues with `Closes #N` when applicable. That's it.

A local `commit-msg` git hook (`.git/hooks/commit-msg`) strips these trailers as
a safety net, but do not rely on it — just don't write them.

## Git workflow

- Branch for changes; open a PR against `main`. Do not commit straight to `main`.
- Do not force-push shared branches (especially `main`) without explicit
  confirmation from the maintainer.
- CI (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -D warnings`,
  `cargo test --workspace`) must pass before merge.

## Build / test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

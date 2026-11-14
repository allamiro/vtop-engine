<!--
Write this in your own words. Delete a section only if it can't apply.
The title becomes the squash commit's subject: `area: what changed`.
-->

## Summary

Fixes #
<!--
"Fixes #N" closes issue N when this merges; use it only if the PR fully
resolves N. "Refs #N" links an issue that should stay open.
GitHub reads closing keywords (fix, fixes, close, closes, resolve, resolves
and their past tenses) anywhere in this description and in the commit
messages, which end up in the squash commit. Even "doesn't fix #N" or
"partly fixes #N" closes N. In prose, write "issue #N" or "refs #N".
-->

## What changed and why

<!-- The problem, then the change. What a reviewer should look at first. -->

## Evidence

<!--
- The test that fails before this change and passes after it: its name, and
  how you saw it fail on the old code. If no such test is possible, say why
  and what stands in for it.
- The checks you ran, with their results (the summary lines, not "all
  green"). CI filters jobs by path, so say what you ran yourself.
-->

## Compatibility and upgrade impact

<!--
Durable formats (manifest, segment log, ledger schema, Raft log), wire
(vtop-protocol frames, what Kafka clients see), config keys (added, renamed,
new default; does an older node ignore, refuse or misread the key?), and a
mixed-version cluster mid-upgrade. "None" is fine, with the reason.
-->

## Docs

<!-- What you updated (README, docs/, helm/vtop/README.md, --help text), or why nothing needed it. -->

## Checklist

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] `cargo test --workspace --locked`
- [ ] Builds on the MSRV (`rust-version` in `Cargo.toml`), if dependencies or language features changed
- [ ] `scripts/check-naming.sh`, plus the shell, workflow or Helm checks in CONTRIBUTING.md if those files changed
- [ ] No assertion, timeout or check weakened to get a pass; a corrected assertion is explained in the commit message
- [ ] No AI co-author trailer or "generated with" line in any commit
- [ ] Use of an LLM is disclosed below, per CONTRIBUTING.md
- [ ] Security impact considered (docs/SECURITY_MODEL.md); an undisclosed vulnerability goes through a private advisory, not this PR

LLM use: <!-- "none", or what it helped with -->

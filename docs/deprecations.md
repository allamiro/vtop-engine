# Deprecations: commands, flags and metrics

Dashboards, alerts and scripts are written against the names below, so a
name that disappears breaks them without an error anywhere. This page lists
every `vtopctl` or `vtop-node` command, flag or accepted value, and every
metric or label name, that is deprecated or has been removed. The policy is
the one in the [compatibility policy](reference/compatibility.md#deprecation):
before 1.0 a removal needs one minor release of notice, and in 1.x the notice
runs for at least two minor releases and the removal waits for a major.

## How the gate works

The whole surface is committed under `docs/surface/`, one item per line:

| File | What it lists |
|---|---|
| `vtopctl.txt` | Every `vtopctl` command, and each argument with its spellings, accepted values and default |
| `vtop-node.txt` | The same for `vtop-node` |
| `metrics-engine.txt` | Every metric family `vtopctl run` serves on `/metrics`: `name type labels` |
| `metrics-node.txt` | Every metric family a `vtop-node` process can serve, whichever roles it runs |

Each file is generated from the code, by a test in the crate that builds the
binary, so it cannot list a name the code does not have. Two checks run on
every pull request:

1. **Drift.** `cargo test` fails when a snapshot no longer matches the code,
   lists the lines on each side, and says how to regenerate:
   `scripts/update-surface-snapshots.sh`. A new command, flag, value, metric
   or label needs nothing more than the regenerated file.
2. **Removals.** `scripts/check-surface-deprecations.py` compares the
   snapshots with the pull request's base. Every item that is gone, renamed
   or narrowed needs a row in the table below, which may be added in the
   same pull request. A label added to a family, a changed default or a
   reworded help text needs no row.

What counts as narrowed, and which row covers it:

| Change | Kind of row | Name |
|---|---|---|
| A command removed or renamed, or one of its aliases dropped | `command` | `vtopctl old-name` (covers its flags and subcommands too) |
| A flag removed or renamed, a short or alias dropped, a flag that took no value now takes one (or the reverse), a flag made required, or a positional moved | `flag` | `vtopctl scrub --old-flag` |
| A new required argument on a command that already existed | `flag` (or the `command`), named after the new argument | `vtopctl scrub --tenant` |
| An option that took any value now accepts only a listed set | `flag` | `vtopctl scrub --mode` |
| A value no longer accepted | `value` (or the `flag`) | `vtopctl process-once --source syslog` |
| A metric removed, renamed, or changed type | `metric` | `vtop_old_total` (covers its labels too) |
| A label removed or renamed | `label` (or the `metric`) | `vtop_old_total{reason}` |

A row also needs a line in the release notes' upgrade section. Where the old
name can keep working for the notice period, keep it: a hidden alias for a
flag, or the old metric exported beside the new one.

## Entries

`Deprecated in` and `Removed in` are releases (`v0.11.0`); `Removed in` is
`not scheduled` while the item still works. A row stays after the removal, as
the record of when the name went away.

| Kind | Name | Deprecated in | Removed in | Replacement | Notes |
|---|---|---|---|---|---|
| command | `vtopctl meta cancel-retention` | v0.1.0 | not scheduled | None | The metadata state machine rejects every `CancelRetention` proposal; the command stays so a peer of any release decodes it ([compatibility policy](reference/compatibility.md#deprecation)). |

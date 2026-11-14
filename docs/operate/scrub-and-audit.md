# Scrub and coverage audit

Verification gates each commit, but nothing reads an object again after it is
committed. Two scheduled commands close that gap. Both are read-only and can
run beside a live engine. They check the guarantees described on
[Security](../concepts/security.md#integrity).

| Command | Question it answers | Reads |
|---|---|---|
| `vtopctl audit-coverage` | Is every source range archived, with no holes? | The ledger and the prune digests. With `--check-storage`, one HEAD per committed object and manifest. |
| `vtopctl scrub` | Is every committed object still intact and authentic? | Each committed batch's manifest and object, from storage. |

Both exit non-zero when they find a problem, so a cron job or a Kubernetes Job
fails visibly. Neither repairs anything: the remedy for a finding is a restore
from a replica or backup, or recording the loss.

## Coverage audit

```bash
vtopctl audit-coverage --config /etc/vtop/config.yaml
vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage \
  --metrics-file /var/lib/node_exporter/textfile/vtop_coverage.prom
```

| Flag | Meaning |
|---|---|
| `--config` | The engine's config. Required. |
| `--digest-dir` | Where prune digests are read from. Default: `engine.ledger_digest_dir`, else `<database file>.ledger-digests` beside a SQLite file, else `<engine.work_dir>/ledger-digests`. |
| `--check-storage` | Also check that every committed object and manifest is present, and every object at its recorded size. |
| `--metrics-file` | Also write the result as a Prometheus text file. |

For each source unit (a Kafka topic-partition under one consumer group, or a
file or spool file at one inode), committed ranges must tile. A range that
failed and was never archived again, or a gap in a file's bytes, is lost data.
Ranges above the last commit are pending, not holes, and overlaps are counted
as duplicates. What it can and cannot judge, including file rotation and
replaced files, is in
[Production HA §6.6](../PRODUCTION_HA.md#66-ledger-retention--pruning-implemented--incremental-delete-128).

## Scrub

```bash
vtopctl scrub --config /etc/vtop/config.yaml --json
```

For every committed batch, in the ledger or in a prune digest, the scrub
downloads the manifest at its pinned version, checks its self-hash and MAC and
that it binds to the ledger row, verifies the object's stored content against
the manifest, and checks that the manifest's current key still holds the
pinned version.

| Flag | Default | Meaning |
|---|---|---|
| `--config` | Required | The engine's config. |
| `--digest-dir` | As `audit-coverage` | Where prune digests are read from. |
| `--concurrency` | `4` | Batches checked at once, 1 to 256. |
| `--max-objects-per-sec` | - | Start at most this many batches per second. Fractions allowed, down to one a day. |
| `--max-bytes-per-sec` | - | Read at most this many object bytes per second, by recorded size. |
| `--limit` | - | Check at most this many batches this run. |
| `--cursor-file` | - | Resume after the batch this file names, and record progress in it. With `--limit`, successive runs walk the archive a slice at a time. |
| `--sample` | - | Check this many batches chosen at random. Not with `--limit` or `--cursor-file`. |
| `--seed` | random | The sample's seed; printed either way. Needs `--sample`. |
| `--metrics-file` | - | Write the results as a Prometheus text file. Totals carry forward from the file it replaces. |

Each batch gets one result:

| Result | Meaning | Finding |
|---|---|---|
| `ok` | Content matches the authenticated manifest, which matches the ledger. | No |
| `limited` | Present at its size, no digest compared (size-only verification). | No |
| `missing` | The object, the pinned manifest version or the manifest's current key is gone. | Yes |
| `mismatched` | Something is stored, and it is not what was committed. | Yes |
| `unauthenticated` | The manifest does not parse, fails its self-hash or MAC, or does not bind to the ledger row. | Yes |
| `unreadable` | The store answered with an error other than absence. Nothing was proven. | Yes |
| `unsupported_version` | The manifest is from a newer release than this binary reads. Upgrade the scrub's binary and run it again. | Yes |

## Schedule them

There is no periodic mode inside the engine. Run both on a schedule, under an
identity that can read the state store and the bucket and nothing more. Size
`--limit` so the hourly runs cover the archive within your pass window: for
example, 1,000,000 committed batches once a week is about 6,000 per hourly run.

crontab ends a command at the newline, so put the options in a script:

```bash
#!/bin/sh
# /usr/local/bin/vtop-scrub: a slice of the archive per run; a full pass about once a week.
exec vtopctl scrub --config /etc/vtop/config.yaml \
  --digest-dir /var/lib/vtop/ledger-digests \
  --cursor-file /var/lib/vtop/scrub.cursor --limit 6000 \
  --concurrency 4 --max-bytes-per-sec 20000000 \
  --metrics-file /var/lib/node_exporter/textfile/vtop_scrub.prom --json \
  > /var/log/vtop/scrub.json
```

```text
0 * * * * /usr/local/bin/vtop-scrub
```

Pass `--digest-dir` wherever the ledger is pruned: without the digests, the
scrub and the audit see only the rows still in the ledger.

From the container image, run `vtopctl` directly (`--entrypoint vtopctl`, or
`command: [vtopctl]` in Kubernetes). The image's entrypoint adds its own
`--config` and requires writable data directories, which a read-only job does
not need. A Kubernetes CronJob needs a persistent volume for the cursor and
the metrics file, and `concurrencyPolicy: Forbid`; the full example is in
[Production HA §7.5](../PRODUCTION_HA.md#75-re-verifying-committed-objects-vtopctl-scrub-implemented-628).

## Alert on the results

The scrub's metrics are not on the engine's `/metrics`; they are in the file
`--metrics-file` writes, for a node exporter's textfile collector or a
Pushgateway. Alert on `vtop_scrub_last_finding_timestamp_seconds` rather than
on the latest run's count: with a rotating cursor, the next slice reads zero
while earlier damage is still there. The metrics and an example rule set are
in the [observability README](../../observability/README.md#archive-scrub).

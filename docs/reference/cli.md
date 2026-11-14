# The `vtopctl` CLI

The CLI binary is `vtopctl`. From a source checkout, `cargo run -p vtop-cli --`
runs it.

```bash
# Check a config without contacting anything. Exits non-zero only for an
# invalid config; a risky opt-out is printed as a WARNING and still passes.
cargo run -p vtop-cli -- validate \
  --config examples/config.yaml

cargo run -p vtop-cli -- run \
  --config examples/config.yaml

cargo run -p vtop-cli -- discover \
  --config examples/config.yaml

cargo run -p vtop-cli -- process-once \
  --source kafka \
  --config examples/config.yaml

cargo run -p vtop-cli -- process-once \
  --source file \
  --config examples/config.yaml

cargo run -p vtop-cli -- replay \
  --batch-id <batch_id> \
  --config examples/config.yaml

cargo run -p vtop-cli -- status \
  --config examples/config.yaml

cargo run -p vtop-cli -- list-batches \
  --config examples/config.yaml \
  --json

cargo run -p vtop-cli -- verify-manifest \
  --manifest s3://telemetry-data/.../batch.manifest.json \
  --config examples/config.yaml

# PostgreSQL only: run once with the privileged migration secret before the
# engine starts with its DML-only runtime secret.
cargo run -p vtop-cli --features postgres -- migrate \
  --config examples/config.yaml
```

Common CLI behavior:

| Option | Purpose |
|---|---|
| `--json` | machine-readable output |
| `--log-level` | runtime log level |
| non-zero exit | command failure |
| secret-safe output | commands should not print credentials |

## Stopping `vtopctl run`

On Unix, SIGINT (Ctrl-C) and SIGTERM (`docker stop`, a Kubernetes pod
termination, `systemctl stop`) are both orderly stops; on Windows the orderly
stop is Ctrl-C (SIGTERM is not handled there). The engine finishes the source
pass in progress (it never cuts a batch off mid-way, and skips the rest of
that cycle), force-flushes every buffered batch through upload, verification
and source commit, logs `shutdown flush complete`, and exits 0. If the flush
cannot archive what the engine had buffered (a batch fails verification, or a
source pass fails), it still tries every source type, then logs
`shutdown flush incomplete` and exits 1. A source that cannot be read during
the flush does not count: its unread data was never taken from the source,
and the next start reads it, like data that arrives after the flush.

Send the signal once and wait: a SECOND signal while that flush runs abandons
it and exits 1. That is the way out of a flush stuck on an unreachable object
store, and it is no worse than SIGKILL — source progress is only committed
after a batch verifies, so the next start recovers or re-reads whatever the
flush did not finish. Only a signal sent after the flush begins counts as the
second: any signals already queued when the engine observes the first, of
either kind, count as that one stop. A signal that arrives during startup
recovery is honoured before any processing starts. Give the engine's
termination grace period enough time for one source pass plus one flush.

## PostgreSQL migrations

PostgreSQL schema changes are never run by normal engine startup. Use a
separate migration identity for `vtopctl migrate`, then give the runtime role
only schema `USAGE` and `SELECT, INSERT, UPDATE` on `batches`. See
[PostgreSQL deployment](../POSTGRES_DEPLOYMENT.md).

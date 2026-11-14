# Troubleshooting

Start with the message. Each entry below quotes the text the engine, the
entrypoint or the chart prints. Build and library-loading errors are on
[Build from source](../install/from-source.md#troubleshooting).

## The engine will not start

| Message | Cause and fix |
|---|---|
| ``missing field `batching` `` (or another section) | `engine`, `batching`, `compression`, `sources` and `upload` must all be present; `batching: {}` and `compression: {}` take every default. See the [reference](../configure/reference.md#top-level). |
| `another VTOP engine already holds ... multi-instance operation is UNSUPPORTED` | A second engine on this host uses the same state store or work directory. Run one engine per ledger. Read-only commands (`status`, `audit-coverage`, `scrub`) take no lock. |
| `endpoint_url ... is plaintext http:// while verify_tls is true` | `upload.verify_tls` is on by default and refuses plaintext. Use an `https://` endpoint. Only for a lab, set `verify_tls: false` (or `VTOP_S3_VERIFY_TLS=false`). |
| `postgres:// state store requires a build with --features postgres` | The release binaries and image have no PostgreSQL support. Build `vtopctl` with `--features postgres`. |
| `PostgreSQL engine.state_store URLs must be loaded from a secret reference` | Move the URL out of the config: `state_store: { env: VTOP_STATE_STORE }` or `{ file: /run/secrets/vtop-state-store }`. |
| `kafka.enable_auto_commit MUST be false` | Remove the key or set it to `false`. The engine commits offsets itself, after verification. |
| `kafka.bootstrap_servers must not be empty when kafka is enabled` | List at least one broker. |
| `upload.command_binary must be an explicit absolute path for the ... backend` | The `awscli`, `s3cmd` and `minio` backends never search PATH. Give the client's absolute path. |
| `localfs backend requires upload.local_path` | Set `upload.local_path`. |
| `stream "...": checksum.algorithm is none but upload.require_strong_verification is true` | A stream cannot lower the global verification floor. Use `sha256` or `blake3` for that stream. |
| `vtop-engine: /data/work is not writable by uid 10001; fix the volume ownership` | Container image only. The data volume is owned by root; see [Container image](../install/container.md#run-the-engine). |

## The engine starts, but not as configured

- **A setting seems to have no effect.** Most config sections ignore keys they
  do not know, so a misspelled key falls back to its default. Run
  `vtopctl validate --config config.yaml --print-effective` and compare.
- **A warning about size-only verification at startup.** It begins
  `upload.require_strong_verification is false`. The engine is accepting
  same-size corruption. Set `upload.require_strong_verification: true` unless
  this is a lab.
- **A `streams.yaml` entry is ignored.** It must sit in the same directory as
  the `config.yaml` you pass. If two entries share a `source_name`, only the
  first applies, and `validate` warns about the other.

## Data is not moving, or is missing

| Signal | Meaning |
|---|---|
| `vtop_verification_failures_total` rising | Stored copies fail verification, so nothing commits and the data stays in the source. Check the object store and the batch's WARN lines. |
| `vtop_source_read_errors_total` rising | A source cannot be read. |
| `vtop_retention_lost_records_total` rising | Kafka retention deleted records before the engine read them. They cannot be archived. Raise the topic's retention above the engine's worst-case lag. |
| `vtop_source_replay_refused_total` rising | A failed range could not be re-read from its source, for example a file rotated away and removed. The path is in the ERROR line beside it. |
| `vtop_upload_throttled_total` rising | The object store is throttling. Consider `batching.adaptive_width.enabled`. |
| `vtopctl audit-coverage` exits non-zero | A hole in the archive. See [Scrub and coverage audit](scrub-and-audit.md). |

`vtopctl status` and `vtopctl list-batches --json` show where each batch is in
its lifecycle. `vtopctl replay --batch-id <id>` runs recovery for one batch.

## Stopping

- **`shutdown flush incomplete`, exit 1.** The engine could not archive
  everything it had buffered. Nothing is lost: source progress was not
  committed for those batches, so the next start reads them again.
- **The stop hangs.** The flush is waiting on an unreachable object store. A
  second SIGINT or SIGTERM abandons it and exits 1, safely. Give the service
  manager a longer stop timeout next time. See
  [Stopping `vtopctl run`](../reference/cli.md#stopping-vtopctl-run).

## Cluster plane on Kubernetes

| Symptom | Cause and fix |
|---|---|
| `helm template` fails with `... is required` | An identity or Secret name has no value. The chart never defaults them; the message names the value. |
| Pods restart once or twice on a fresh install | A node exits when a peer's name does not resolve yet. It settles within about a minute. Do not loosen the probes. |
| Every pod stays not ready under `data.topology: replicated` | Expected until `vtopctl meta init` has run and the topic, range and node registrations exist. Bootstrap first. |
| `vtopctl meta` fails during the TLS handshake | Port-forward a specific pod, not the Service, and set `server_name` in `admin.yaml` to that pod's headless FQDN; the default `localhost` never matches. |
| A pod reports `503 not ready: <reason>` on `/readyz` | Read the reason. A fenced lease holder going not ready is correct: it must refuse writes. |

See [Kubernetes with Helm](../deploy/kubernetes.md) and the
[chart README](../../helm/vtop/README.md).

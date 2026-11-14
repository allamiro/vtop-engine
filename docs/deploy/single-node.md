# Single engine with SQLite

The simplest production shape: one `vtopctl run` process on one host, with its
ledger in a local SQLite file. This is Tier 0 in
[Production HA §9](../PRODUCTION_HA.md#9-deployment-topologies). There is no
failover; a stopped engine resumes from its ledger when it starts again.

## What it needs

| Item | Purpose |
|---|---|
| `vtopctl` | From the [release binaries](../install/binaries.md) or the [container image](../install/container.md). |
| `config.yaml` | Sources, upload target, batching. See the [reference](../configure/reference.md). |
| `streams.yaml` (optional) | Per-stream tenant, format and checksum, in the same directory as `config.yaml`. See [Streams](../configure/streams.md). |
| A state directory | Holds the SQLite ledger. Create the directory; the database file is created on first start. Durable: back it up. |
| A work directory | `engine.work_dir`, staging for batches in flight, created if missing. Scratch. |
| Object storage credentials | From the environment, never the config file. |

One engine per ledger. `run`, `process-once` and `replay` refuse to start
while another engine on the same host holds the same state store or work
directory, with an error that names "another VTOP engine".

## 1. Write the config

Start from [`examples/config.yaml`](../../examples/config.yaml). The keys a
single node always sets:

```yaml
engine:
  name: vtop-engine
  state_store: "sqlite:///var/lib/vtop/state/vtop-state.db"
  work_dir: /var/lib/vtop/work

sources:
  file:
    enabled: true
    paths:
      - /var/log/app/*.log

upload:
  backend: s3_native
  bucket: telemetry-data
  endpoint_url: https://s3.example.com
  region: us-east-1
```

`upload.verify_tls` defaults to `true`, which requires an `https://` endpoint.
`upload.require_strong_verification` also defaults to `true`; leave it on.

## 2. Validate it

`validate` reads `config.yaml` and the `streams.yaml` beside it and contacts
nothing:

```bash
vtopctl validate --config /etc/vtop/config.yaml
vtopctl validate --config /etc/vtop/config.yaml --print-effective
```

It exits non-zero only for an invalid config. A risky setting is printed as a
warning and still passes. `--print-effective` shows what each stream will run
with and where each value came from; see
[Streams](../configure/streams.md#check-what-each-stream-runs-with).

## 3. Provide credentials and start

The native S3 backend uses the AWS SDK's standard credential chain, so the
usual variables work:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export VTOP_METRICS_ADDR=127.0.0.1:9090   # optional: /metrics, /healthz, /readyz

vtopctl run --config /etc/vtop/config.yaml
```

Grant the credentials only what the backend needs; the per-backend list is in
the [security model §5](../SECURITY_MODEL.md#5-object-storage-permissions-and-least-privilege).

## 4. Run it under a service manager

SIGINT and SIGTERM are orderly stops: the engine finishes the source pass in
progress, flushes every buffered batch through verification and commit, and
exits 0. A second signal during that flush abandons it. Give the service
manager a stop timeout long enough for one source pass plus one flush; the
details are in [Stopping `vtopctl run`](../reference/cli.md#stopping-vtopctl-run).

An example systemd unit (adjust paths, user and timeout to your host):

```ini
[Unit]
Description=VTOP Engine
After=network-online.target
Wants=network-online.target

[Service]
User=vtop
ExecStart=/usr/local/bin/vtopctl run --config /etc/vtop/config.yaml
EnvironmentFile=/etc/vtop/vtop.env
KillSignal=SIGTERM
TimeoutStopSec=300
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Keep `/etc/vtop/vtop.env` readable only by the service user: it holds the
object storage credentials.

## 5. Check it is working

```bash
vtopctl status --config /etc/vtop/config.yaml
vtopctl list-batches --config /etc/vtop/config.yaml --json
curl -s http://127.0.0.1:9090/readyz
```

Then set up [metrics and alerts](../operate/monitoring.md) and a scheduled
[scrub and coverage audit](../operate/scrub-and-audit.md).

## What to back up

| Path | Why |
|---|---|
| The SQLite database file | The ledger: every batch's state, and the cursor recovery resumes from. |
| `<database file>.ledger-digests/` | Written by ledger pruning. The coverage audit and the scrub read it to account for pruned rows. Unless `engine.ledger_digest_dir` names another directory. |

`engine.work_dir` is scratch: staging files are removed after each batch, and
leftovers from a crash are cleaned on restart.

## Growing out of it

Move to a [PostgreSQL ledger](../POSTGRES_DEPLOYMENT.md) when the ledger must
survive the loss of the engine's host. That needs a `vtopctl` built with
`--features postgres`. Several engines sharing one ledger is not supported yet.

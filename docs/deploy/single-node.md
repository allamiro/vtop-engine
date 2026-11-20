# Single engine with SQLite

One `vtopctl run` process on one host, with its ledger in a local SQLite
file. This is Tier 0 in
[Production HA §9](../PRODUCTION_HA.md#9-deployment-topologies). There is no
failover: while the engine is down, data waits at the source, and a restart
resumes from the ledger. If you have not archived anything yet, do
[Your first real archive](../getting-started/first-archive.md) first.

## Prerequisites

| Item | Notes |
|---|---|
| `vtopctl` | From the [release binaries](../install/binaries.md) or the [container image](../install/container.md). |
| A bucket and credentials | Grant only what the backend needs: [security model §5](../SECURITY_MODEL.md#5-object-storage-permissions-and-least-privilege). |
| A state directory | Holds the SQLite ledger. Create the directory; the database file is created on first start. Durable: back it up. |
| A work directory | `engine.work_dir`, staging for batches in flight, created if missing. Scratch. |

One engine per ledger. `run`, `process-once` and `replay` refuse to start
while another engine on the same host holds the same state store or work
directory.

## Steps

### 1. Write the config

Start from [`examples/config.yaml`](../../examples/config.yaml) or the
minimal config in
[Your first real archive](../getting-started/first-archive.md#1-write-a-minimal-config).
The keys every deployment sets:

```yaml
engine:
  name: vtop-engine
  state_store: "sqlite:///var/lib/vtop/state/vtop-state.db"
  work_dir: /var/lib/vtop/work
batching: {}
compression: {}
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

Leave `upload.verify_tls` and `upload.require_strong_verification` at their
default, `true`. Add per-source settings in [`streams.yaml`](../configure/streams.md).

### 2. Validate it

```bash
vtopctl validate --config /etc/vtop/config.yaml
vtopctl validate --config /etc/vtop/config.yaml --print-effective
```

`validate` contacts nothing and exits non-zero only for an invalid config.
[`--print-effective`](../configure/print-effective.md) shows what each stream
will run with.

### 3. Run it as a service

SIGINT and SIGTERM are orderly stops: the engine finishes the current source
pass, flushes every buffered batch through verification and commit, and exits
0. A second signal during that flush abandons it. Give the service manager a
stop timeout long enough for one source pass plus one flush
([Stopping `vtopctl run`](../reference/cli.md#stopping-vtopctl-run)).

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

`/etc/vtop/vtop.env` holds the object storage credentials
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) and, to expose
metrics, `VTOP_METRICS_ADDR=127.0.0.1:9090`. Keep it readable only by the
service user.

In a container, the same config runs with the
[image](../install/container.md#run-the-engine).

## Verify it works

```bash
vtopctl status --config /etc/vtop/config.yaml
vtopctl list-batches --config /etc/vtop/config.yaml --json
vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
curl -s http://127.0.0.1:9090/readyz
```

Batches should reach `source_committed`, the audit should print
`coverage audit PASSED`, and `/readyz` should answer `200`.

## What to monitor

- `vtop_verification_failures_total`, `vtop_source_read_errors_total`,
  `vtop_retention_lost_records_total` and
  `vtop_verification_mode{mode="size_only"}`: see
  [Metrics and alerts](../operate/monitoring.md).
- A scheduled `vtopctl scrub` and `audit-coverage`:
  [Scrub and coverage audit](../operate/scrub-and-audit.md).
- The ledger's size; set `engine.ledger_retention_days` to prune committed
  rows ([reference](../configure/reference.md#engine)).

Back up the SQLite file and its `<database file>.ledger-digests/` directory:
[Backup and recovery](../operate/backup-and-recovery.md).

## Upgrade notes

Stop the engine with SIGTERM, back up the ledger, replace the binary, start
it. Downgrades are not supported. Read [Upgrades](../operate/upgrades.md) for
the release you are moving to.

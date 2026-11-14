# Config basics

The archive engine reads two files and the environment. What the engine,
the ledger and the object store each do is on
[Architecture](../concepts/architecture.md).

| Input | What it holds | Reference |
|---|---|---|
| `config.yaml` | Engine, sources, batching, upload target. Passed with `--config`. | [`config.yaml` reference](reference.md) |
| `streams.yaml` | Optional per-source settings: tenant, format, checksum, storage name. Read from the same directory as `config.yaml`. | [Streams](streams.md) |
| Environment | Secrets and deployment overrides. Never written to the config files. | [Below](#environment-variables) |

Start from [`examples/config.yaml`](../../examples/config.yaml) and
[`examples/streams.yaml`](../../examples/streams.yaml), which are annotated.

## Check a config before you start

```bash
vtopctl validate --config config.yaml
```

`validate` parses both files and applies every check the engine applies at
startup, without opening the state store, the object store or any source. It
exits non-zero only for an invalid config. A setting that is valid but risky,
such as `upload.require_strong_verification: false`, is printed as a warning
and still passes.

Add `--print-effective` to see what each stream runs with and where each value
came from; see [Effective settings](print-effective.md).

## Secrets

The config holds the **names** of secrets, never their values:

| Secret | Config key |
|---|---|
| PostgreSQL URL | `engine.state_store: { env: VAR }` or `{ file: /path }` |
| Kafka SASL password | `sources.kafka.sasl_password_env` |
| Manifest MAC key | `manifest_mac_key_env` |
| Object storage credentials | None: the AWS SDK's standard credential chain (for example `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) |

An inline `postgres://` URL in `engine.state_store` is refused. The full rules
are in the [security model §3](../SECURITY_MODEL.md#3-credential-handling).

## Environment variables

| Variable | Effect |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Credentials and region for the native S3 backend. |
| `VTOP_S3_ENDPOINT_URL` | Overrides `upload.endpoint_url`. |
| `VTOP_S3_FORCE_PATH_STYLE` | Overrides `upload.force_path_style` (`true`/`1`). |
| `VTOP_S3_VERIFY_TLS` | Overrides `upload.verify_tls` (`false`/`0` turns it off). |
| `VTOP_S3_TRANSPORT` | Overrides `upload.transport`. |
| `VTOP_METRICS_ADDR` | Serve `/metrics`, `/healthz` and `/readyz` on this address, for example `127.0.0.1:9090`. Nothing listens unless it is set. |
| `VTOP_METRICS_TLS_CERT`, `VTOP_METRICS_TLS_KEY` | Serve that endpoint over TLS 1.3. Both or neither: half a pair disables the endpoint. |
| `VTOP_METRICS_TLS_CLIENT_CA` | With the pair above, require scrapers to present a certificate under this CA. |
| `RUST_LOG` | Log filter. Wins over `--log-level` and `engine.log_level`. |
| `VTOP_LOG_FORMAT` | `json` for JSON log lines (as does `--json`). |
| `VTOP_AUDIT_LOG` | Append audit records (#694) to this JSONL file, as well as the log stream. A set but unwritable path stops the command or node at startup, naming it. See [Auditing](../operate/auditing.md). |
| `VTOP_CONFIG` | Container image only: the config path the entrypoint passes to `vtopctl`. Default `/app/examples/config.yaml`. |

The variable named by `engine.state_store.env` (conventionally
`VTOP_STATE_STORE`) and by `sasl_password_env` are read too.

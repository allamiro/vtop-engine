# Your first real archive

Archive one directory of log files into one S3-compatible bucket, then prove
the archive is complete and intact. You need `vtopctl`
([binaries](../install/binaries.md) or the
[container image](../install/container.md)) and a bucket you can write to.

## 1. Write a minimal config

Save this as `/etc/vtop/config.yaml`, with your endpoint and bucket:

```yaml
engine:
  name: first-archive
  state_store: "sqlite:///var/lib/vtop/state/vtop-state.db"
  work_dir: /var/lib/vtop/work

batching: {}       # defaults: seal at 10,000 records, 100 MiB or 60 s
compression: {}    # default: gzip level 6

sources:
  file:
    enabled: true
    paths:
      - /var/log/myapp/*.log

upload:
  backend: s3_native
  bucket: telemetry-archive
  endpoint_url: https://s3.example.com
  create_bucket: true   # only if the credentials may create buckets
```

Everything not listed takes its default; the
[reference](../configure/reference.md) lists them all. Create the state
directory (`mkdir -p /var/lib/vtop/state`); the database file and the work
directory are created on first start.

!!! tip "Trying it against a local MinIO"

    Use `endpoint_url: http://127.0.0.1:9000`, add `force_path_style: true`,
    and add `verify_tls: false`, because a plaintext endpoint is refused
    otherwise. Never turn `verify_tls` off for a real store.

## 2. Check it

```bash
vtopctl validate --config /etc/vtop/config.yaml
vtopctl discover --config /etc/vtop/config.yaml
```

`validate` contacts nothing and should print `config is valid (0 warning(s))`.
`discover` lists each file it will read. For a source with no `streams.yaml`
mapping its format column says `raw`; the real format is detected per batch
when it is archived.

## 3. Archive one pass

The bucket credentials come from the environment:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1

vtopctl process-once --source file --config /etc/vtop/config.yaml
```

`process-once` runs one cycle and archives what it read, so you can watch each
step in the log: `object_uploaded`, `manifest_uploaded`,
`verification_passed`, then `source_committed`.

## 4. Prove it worked

```bash
vtopctl status --config /etc/vtop/config.yaml
vtopctl audit-coverage --config /etc/vtop/config.yaml --check-storage
vtopctl scrub --config /etc/vtop/config.yaml
```

| Command | Expect |
|---|---|
| `status` | Every batch in `source_committed`. |
| `audit-coverage --check-storage` | `coverage audit PASSED`, and every committed object and manifest present. |
| `scrub` | `scrub PASSED`: each stored object matches its manifest. |

To check one batch by hand, pass its manifest URI (it ends in
`.manifest.json`, and is in the `manifest_uploaded` log line) to
`vtopctl verify-manifest --manifest <uri> --config ...`. It ends with
`verdict : PASSED`.

## 5. Keep it running

Replace `process-once` with `vtopctl run --config /etc/vtop/config.yaml` to
archive continuously. To run it as a service, with metrics and backups, follow
[Single engine with SQLite](../deploy/single-node.md).

These steps were run end to end against MinIO with the v0.8.0 image (with the
local-MinIO changes from the tip above): one batch of five JSON lines was
detected as `jsonl`, archived, verified and committed, and `scrub`,
`audit-coverage --check-storage` and `verify-manifest` all passed.

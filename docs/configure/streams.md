# Streams (`streams.yaml`)

`streams.yaml` maps a discovered source to the settings its batches are
written with. It is optional and is read from the same directory as
`config.yaml`. A source with no entry still runs: its format is detected from
the data and everything else comes from `config.yaml`.

```yaml
streams:
  - source_name: cef_events        # a Kafka topic
    source_type: kafka
    format: cef
    tenant: default
    checksum:
      algorithm: sha256
    s3_source_name: security
    retention_class: standard

  - source_name: /data/input/auth.cef.log   # a file
    source_type: file
    format: cef
    checksum:
      algorithm: blake3
    s3_source_name: auth
```

This is an excerpt of [`examples/streams.yaml`](../../examples/streams.yaml).

## Keys

| Key | Default | Meaning |
|---|---|---|
| `source_name` | **Required** | The discovered name: a Kafka topic, a file path or a spool path. `vtopctl discover` lists them. |
| `source_type` | **Required** | `kafka`, `file` or `syslog_spool`. |
| `format` | **Required** in an entry | `cef`, `leef`, `json`, `jsonl`, `syslog` or `raw`. Recorded in the manifest and used for `{format}`. A source with no entry has its format detected per batch. |
| `tenant` | `default` | Tenant for `{tenant}` in the object key. |
| `s3_source_name` | the source name | Name used for `{source}` in the object key, for example `app_events` stored as `app`. |
| `retention_class` | - | Free-form label, available as `{retention_class}` in the key template. |
| `checksum.algorithm` | the global `checksum.algorithm` | `sha256`, `blake3` or `none`, for this stream only. |

If two entries share a `source_name`, the first one applies. `validate` warns
that the later entry is never used.

## Per-stream checksum

A stream's `checksum` block overrides the global `checksum.algorithm` for new
batches of that stream. Leave it out, or set it to `null`, to inherit. The
block takes exactly one key, `algorithm`, and refuses any other, so a typo is
an error rather than a silent `sha256`.

- **The verification floor stays global.** While
  `upload.require_strong_verification` is `true`, a stream that sets
  `algorithm: none` is refused by name at startup and by `validate`.
- **Old batches keep their algorithm.** Recovery and `vtopctl scrub` check each
  batch with the algorithm recorded in its manifest, not the current setting.
- **Rolling upgrades.** Engines older than v0.8.0 ignore the `checksum` key
  and use the global algorithm. Upgrade every engine before relying on it, and
  keep the global algorithm strong meanwhile. See [Upgrades](../operate/upgrades.md).

The algorithm decides how the stored copy is verified. With the native S3
backend, `sha256` uses the store's own checksum when it kept one and reads the
object back otherwise; `blake3` always reads it back; `none` checks size only.

## Check what each stream runs with

```bash
vtopctl validate --config config.yaml --print-effective
```

It contacts no service and resolves no secret. For each stream, and for the
`default` block that unmapped sources get, it prints every effective setting
with its origin: `stream`, `global` or `built-in default`. Against the example
files, one stream reads:

```text
stream /data/input/auth.cef.log:
  batching.max_age: 60 (global)
  batching.max_bytes: 104857600 (global)
  batching.max_concurrent_batches: 8 (global)
  batching.max_records: 10000 (global)
  checksum.algorithm: blake3 (stream)
  format: cef (stream)
  manifest_mac: disabled (built-in default)
  path_template: tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/ (global)
  resolved_upload_width: 8 (global)
  retention_class: standard (stream)
  s3_source_name: auth (stream)
  source_name: /data/input/auth.cef.log (stream)
  source_type: file (stream)
  tenant: default (stream)
  upload.backend: s3_native (global)
  upload.prefix: "" (global)
  upload.require_object_versioning: false (built-in default)
  upload.require_strong_verification: true (global)
  verification_method: read_back (stream)
```

`verification_method` is how the stored copy will be checked:
`store_checksum`, `read_back` or `size_only`. Add `--json` for the same data
as JSON, with `effective_version: 1`, `status` and `warnings` beside it.

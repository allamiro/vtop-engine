# Effective settings (`--print-effective`)

Before you start an engine, ask it what each stream will run with. It is the
quickest way to catch a stream that did not get the checksum you meant, or a
misspelled key: most sections ignore a key they do not know, so a listed
setting you meant to change still shows `(built-in default)`.

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

## Reading it

| Field | Meaning |
|---|---|
| `checksum.algorithm` | The algorithm new batches of this stream use; see [per-stream checksum](streams.md#per-stream-checksum). |
| `verification_method` | How the stored copy is checked. |
| `resolved_upload_width` | Batches uploaded and verified at once, after every ceiling is applied. |
| `path_template` | The object key layout; see [`partitioning`](reference.md#partitioning). |
| `upload.require_strong_verification` | The global verification floor no stream can lower. |
| `manifest_mac` | `enabled` or `disabled`, never the key. |

The output is an allowlist: it never includes a secret, a secret reference's
value, or a setting outside the list.

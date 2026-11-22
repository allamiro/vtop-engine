# S3 compatibility: servers and their expectations

VTOP's live S3 tests run unchanged against every S3 server it is tested
with (#722). The server a run is against is named by `VTOP_TEST_S3_SERVER`
(default `minio`, the lab's source-built MinIO). Its file here,
`servers/<id>.yaml`, says what that server is expected to do for each
capability. The run is compared with it, case by case, by the test kit in
`crates/vtop-s3-testkit`.

## Capabilities

| Id | What it means |
|---|---|
| `put_get_head` | PUT, GET, HEAD and DELETE of a plain object, including a PUT carrying `x-amz-checksum-sha256` (every SHA-256 write VTOP makes) |
| `path_style` | path-style addressing (every lab server) |
| `virtual_host` | virtual-host addressing (AWS) |
| `checksum_sha256_enforced` | a PUT carrying a wrong `x-amz-checksum-sha256` is rejected |
| `checksum_sha256_returned` | `HeadObject` with checksum mode returns the SHA-256, so verification is `store_checksum` |
| `checksum_crc32_default` | the SDK's default CRC32 on PUT is accepted |
| `multipart` | CreateMultipartUpload, UploadPart, CompleteMultipartUpload |
| `multipart_abort` | AbortMultipartUpload |
| `multipart_list_uploads` | ListMultipartUploads (the tests use it to prove an abort) |
| `multipart_resume` | a multipart upload resumed from a persisted session |
| `versioning` | version ids on PUT, HEAD and GET by `versionId` |
| `object_lock` | the Object Lock configuration read, retention and legal hold written and read back |
| `object_lock_enforced` | a locked version cannot be deleted |
| `conditional_put` | `If-None-Match: *` on PUT; informational, VTOP does not use it |

Every case adds the addressing it ran with (`path_style` or `virtual_host`)
to the capabilities it names. A run whose addressing the server's file marks
`not_applicable` or `unsupported` is refused before any case runs: every
case would be skipped, and the run would pass having tested nothing.

## States

Every capability must be listed; the kit refuses a file that leaves one out.

| State | What runs | Expected outcome |
|---|---|---|
| `pass` | the positive body | it passes |
| `unsupported` with `refusal: startup` or `refusal: first_batch` | the refusal body | VTOP refuses at that moment, in three parts (what is wrong, what the engine did, what to do) |
| `xfail` with `issue: N` | the positive body | it fails; a pass is an unexpected pass |
| `unknown` | the positive body | recorded, never judged; refused on a gating server by any case that needs it |
| `not_applicable` | nothing | recorded as such, never as a pass |

A case that needs several capabilities is decided by the most constraining
state among them, in the order `not_applicable`, `unsupported`, `xfail`,
`unknown`, `pass`. A capability the server lacks is never skipped silently:
if VTOP has a refusal for it, the case asserts that refusal; if it has none,
the capability is `not_applicable` and the results say so.

The long form also takes a `note`, shown in the report as a note:

```yaml
versioning: { state: unsupported, refusal: startup, note: "GetBucketVersioning is a stub" }
```

The file's other fields: `id` (the file's name), `name` (shown in the report),
`gating`, and `sdk_default_endpoint: true` for a server whose endpoint the
SDK finds itself (AWS), so `VTOP_TEST_S3_ENDPOINT` may be unset.

`gating: true` marks a server whose leg is required. A new server starts
`gating: false` until every capability its cases need is settled and it has
matched its file on ten consecutive runs (decision D11).

## Results

With `VTOP_TEST_S3_RESULTS` set, every case appends one JSON line: the
server, `VTOP_TEST_S3_SERVER_VERSION`, `VTOP_TEST_S3_IMAGE_DIGEST`, the VTOP
commit (`VTOP_TEST_COMMIT`, else `GITHUB_SHA`), the case, its capabilities,
the expectation and the capability that decided it (`deciding_capability`,
none when every capability is `pass`), the outcome (`pass`, `fail`,
`refused`, `not_applicable`, `unexpected_pass`, `unexpected_fail`), whether
that was expected, the verification method the case saw (`store_checksum` or
`read_back`), how long it took, and for a failed body or an unexpected
outcome the first line of why (`detail`).

An unexpected outcome fails the test with a line such as:

```text
UNEXPECTED FAIL verification.sha256_by_store_checksum (checksum_sha256_returned, expected pass) on minio: edit tests/s3-compat/servers/minio.yaml or fix the cause: ...
```

Either the server changed (edit its file, in a reviewed PR) or VTOP did
(fix the cause). Every case in a test runs and writes its line first; the
test fails once at its end, listing each unexpected case.

## Settings

| Variable | Default | |
|---|---|---|
| `VTOP_TEST_S3_SERVER` | `minio` | the server, and so its file |
| `VTOP_TEST_S3_ENDPOINT` | - | required, unless the server's file sets `sdk_default_endpoint: true` |
| `VTOP_TEST_S3_REGION` | `us-east-1` | |
| `VTOP_TEST_S3_PATH_STYLE` | `true` | |
| `VTOP_TEST_S3_VERIFY_TLS` | `false` | the lab is plaintext |
| `VTOP_TEST_S3_BUCKET` | `telemetry-raw` | a bucket without Object Lock |
| `VTOP_TEST_S3_LOCKED_BUCKET` | `telemetry-locked` | a bucket created with Object Lock |
| `VTOP_TEST_S3_SERVERS_DIR` | this directory's `servers/` | for test binaries run from another checkout |

## Running against the lab's MinIO

```bash
docker compose build minio && docker compose up -d minio && docker compose run --rm minio-init
VTOP_TEST_S3_SERVER=minio VTOP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  VTOP_TEST_S3_RESULTS=/tmp/results-minio.jsonl \
  cargo test -p vtop-upload --test live_s3_verification -- --ignored --test-threads=1
```

## Adding a server

1. Add `servers/<id>.yaml` with `gating: false` and every capability
   `unknown`, except those it documents as missing, which are `unsupported`
   with the moment VTOP refuses them. A case with no refusal body fails on
   an `unsupported` capability ("no refusal path"), which is how a missing
   refusal test shows; add the refusal body to the case in the same PR.
2. Run the live tests against it with `VTOP_TEST_S3_SERVER=<id>` and
   `VTOP_TEST_S3_RESULTS` set, and settle each `unknown` from the results.
3. Commit the settled file in the same PR as the server's CI leg.

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
| `object_lock_retention_clear` | a governance retention can be cleared with the bypass header (an empty `PutObjectRetention`); VTOP never does, the live tests do to tamper with a receipt and see the scrub report it |
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
`refused`, `not_applicable`, `not_run`, `unexpected_pass`, `unexpected_fail`), whether
that was expected, the verification method the case saw (`store_checksum` or
`read_back`), how long it took, and for a failed body or an unexpected
outcome the first line of why (`detail`).

`not_run` is a case the test harness cannot run against the server, with
the reason in `detail` (the multipart tests' fault-injecting forwarder needs
a plain-HTTP endpoint, so on AWS they are not run); the report shows its
capabilities as not tested.

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

## Running a server on a laptop

Every server has `docker/s3-servers/<id>/compose.yml` (a `server` and an
`init` that creates the buckets with the pinned AWS CLI) and `server.env`
(how the tests reach it). `scripts/s3-lab.sh` drives them the way the CI leg
does:

```bash
scripts/s3-lab.sh build minio      # the lab's MinIO is built from source
scripts/s3-lab.sh up minio
eval "$(scripts/s3-lab.sh env minio | sed 's/^/export /')"
VTOP_TEST_S3_RESULTS=/tmp/results-minio.jsonl \
  cargo test -p vtop-upload --test live_s3_verification -- --ignored --test-threads=1
scripts/s3-lab.sh summary minio /tmp/results-minio.jsonl
scripts/s3-lab.sh down minio
```

## In CI

`.github/workflows/s3-compat-matrix.yml` builds the live test binaries once
and runs them in one leg per server. It runs:

- from `ci.yml`, with the lab's MinIO alone, on a pull request (and a push to
  `main`) that touches the crates the live tests build or the MinIO build;
- every server, nightly (03:47 UTC) and on `workflow_dispatch` (an optional
  `servers` list such as `["garage"]` narrows it);
- every server, on a pull request that changes the workflow, a server's
  compose file, the lab script or a capability file, so a change to a
  server's setup is proven on its own head before it merges.

A leg whose server is `gating: false` cannot fail the run; its summary still
lists every case and every unexpected outcome, and its results are uploaded
as `s3-compat-results-<id>`. The workflow's `LEGS` table states each server's
`gating` too (it decides `continue-on-error` before any step runs), and the
leg fails if the two disagree. A weekly job
(`.github/workflows/s3-pins.yml`) checks that every pulled server image is
pinned by digest and that the digest still resolves.

## Probes and the second checksum pass

`crates/vtop-upload/tests/live_s3_probes.rs` asks each server three things
VTOP's guarantees lean on or may: whether a PUT with a wrong
`x-amz-checksum-sha256` is refused and nothing stored
(`checksum_sha256_enforced`; `store_checksum` verification trusts the
SHA-256 a store reports, so a store that echoed an unchecked header would
pass corrupt data), and whether `If-None-Match: *` on PUT is honoured
(`conditional_put`, informational), and whether the SDK's default CRC32
trailer on a PUT with no checksum of its own is accepted
(`checksum_crc32_default`, informational: Garage and Zenko CloudServer refuse
it, and since #788 no VTOP write sends it).

Each leg also reruns the verification and Object Lock write tests with
`AWS_REQUEST_CHECKSUM_CALCULATION=when_required` (D13), into
`when_required-<id>.jsonl` beside the leg's results. In that pass a PUT
carries no default CRC32, so `checksum_crc32_default` is dropped from each
case's capabilities, and every line records its `sdk_checksum` mode.

## Licences

The servers run as unmodified test containers in CI and on a laptop. VTOP
does not link them, ship them or distribute their images: each leg pulls
(or, for the lab's MinIO, builds) the image from its own source. That is
why AGPL-3.0 servers, the MinIO line and Garage, are acceptable as test
services here; the decision is recorded in #722. A server whose licence
would forbid even that is not added.

## Adding a server

1. Add `servers/<id>.yaml` with `gating: false` and every capability
   `unknown`, except those it documents as missing, which are `unsupported`
   with the moment VTOP refuses them. A case with no refusal body fails on
   an `unsupported` capability ("no refusal path"), which is how a missing
   refusal test shows; add the refusal body to the case in the same PR.
2. Add `docker/s3-servers/<id>/compose.yml` with the two services `server`
   (its image pinned by digest, the tag in a comment, ports on `127.0.0.1`,
   `no-new-privileges`, `cap_drop: [ALL]`, a memory limit) and `init` (the
   pinned AWS CLI running `docker/s3-servers/init/bootstrap.sh`; set
   `CREATE_LOCKED: "false"` if the server has no Object Lock), and
   `server.env` with `SERVER_VERSION`, `VTOP_TEST_S3_ENDPOINT` and the lab
   credentials.
3. Run the live tests against it (`scripts/s3-lab.sh up <id>`, then the
   tests with `VTOP_TEST_S3_RESULTS` set), and settle each `unknown` from
   the results.
4. Add the leg to the `s3-compat` matrix in `.github/workflows/ci.yml` with
   the file's `gating`, and commit the settled file in the same PR.

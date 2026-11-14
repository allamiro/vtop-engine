# `config.yaml` reference

Every key the archive engine reads from `config.yaml`, its default and what it
does. The source of truth is
[`crates/vtop-core/src/config.rs`](../../crates/vtop-core/src/config.rs); the
annotated example is [`examples/config.yaml`](../../examples/config.yaml). The
per-stream file is on [Streams](streams.md), and the cluster plane's
`vtop-node` configs are separate (see [Cluster plane](../deploy/cluster-plane.md)).

**Required** means the engine refuses a config without it. A dash in the
Default column means the key is unset unless you set it.

!!! warning "Unknown keys are ignored"

    Most sections accept a key they do not know and ignore it, so a misspelled
    key falls back to its default without an error. The exceptions, which
    refuse unknown keys, are `upload.transports.<name>`, a stream's `checksum`
    block, and the `{ env: ... }` / `{ file: ... }` forms of
    `engine.state_store`. Check what took effect with
    `vtopctl validate --print-effective`.

## Top level

| Key | Default | Meaning |
|---|---|---|
| `engine` | **Required** | Engine identity, ledger and working directory. |
| `batching` | **Required** (may be `{}`) | When a batch is sealed, and how many are verified at once. |
| `compression` | **Required** (may be `{}`) | Codec for sealed batches. |
| `checksum` | `algorithm: sha256` | Global object checksum. |
| `manifest_mac_key_env` | - | Name of an environment variable holding the manifest MAC key. |
| `manifest` | `{}` | How manifests are written; see [`manifest`](#manifest). |
| `sources` | **Required** (may be `{}`) | Kafka, file and syslog spool sources. |
| `upload` | **Required** | Object storage target and upload limits. |
| `partitioning` | See [partitioning](#partitioning) | Object key layout. |

## `engine`

| Key | Default | Meaning |
|---|---|---|
| `name` | **Required** | Engine name. |
| `tenant` | `default` | Tenant used for sources no stream maps. |
| `state_store` | **Required** | The ledger. A SQLite URL inline (`"sqlite:///data/state/vtop-state.db"`), or for PostgreSQL a secret reference: `{ env: VTOP_STATE_STORE }` or `{ file: /run/secrets/vtop-state-store }`. An inline `postgres://` URL is refused. PostgreSQL also needs a build with `--features postgres`. |
| `work_dir` | **Required** | Staging directory for batches in flight. Created if missing. Scratch. |
| `work_retention_seconds` | `86400` | Staging files left by a crash and older than this are removed. Successful batches remove theirs at once. |
| `work_max_bytes` | `10737418240` (10 GiB) | Ceiling on VTOP staging files in `work_dir`. Startup removes the oldest until the directory is under it. |
| `log_level` | `INFO` | Log level. `--log-level` overrides it, and `RUST_LOG` overrides both. |
| `ledger_retention_days` | - (no pruning) | Prune `SOURCE_COMMITTED` rows older than this many days, on idle cycles. The newest committed row per source path is always kept. Refused on PostgreSQL; schedule `vtopctl prune-ledger` there instead. |
| `ledger_prune_batch` | `500` | Most rows deleted per prune pass. Must be at least 1. |
| `state_store_timeout_seconds` | `30` | Limit for one state-store request. Whole-ledger scans (startup recovery, pruning) get ten times it. Must be > 0. |
| `ledger_digest_dir` | - | Where prune digests are written and read. Unset: `<database file>.ledger-digests` beside a SQLite file, otherwise `<work_dir>/ledger-digests`, which is scratch; name a durable directory for PostgreSQL. |

## `batching`

| Key | Default | Meaning |
|---|---|---|
| `max_records` | `10000` | Seal a batch at this many records. |
| `max_bytes` | `104857600` (100 MiB) | Seal a batch at this many bytes. Also the per-batch ceiling: an oversized record is rejected without advancing source progress. |
| `max_batch_age_seconds` | `60` | Seal a batch this old. |
| `source_poll_wait_ms` | `250` | Longest a read pass waits for data. Paid once per pass on the Kafka plane; file and spool reads do not wait. |
| `idle_poll_interval_ms` | `2000` | Pause between cycles, only after a cycle that read nothing. |
| `max_concurrent_batches` | `8` | Batches in the verify phase (compress, checksum, upload, manifest, verify) at once. Source commits stay serial. Zero is refused. Not a memory ceiling. |
| `adaptive_width.enabled` | `false` | Let the upload width follow the store's throttles: `max_concurrent_batches` becomes the ceiling, a throttled cycle halves the width and a clean one adds one. |
| `adaptive_width.min_width` | `1` | The width the controller never goes below. |

## `compression`

| Key | Default | Meaning |
|---|---|---|
| `type` | `gzip` | `gzip`, `zstd` or `none`. Recorded in each manifest. |
| `level` | `6` | Codec level. |

The default stays `gzip` so a config that omits it keeps its output format;
[`examples/config.yaml`](../../examples/config.yaml) records why it sets
`zstd` level 3 explicitly.

## `checksum`

| Key | Default | Meaning |
|---|---|---|
| `algorithm` | `sha256` | `sha256`, `blake3` or `none`. A stream can override it in `streams.yaml`. `none` means size-only verification. |

With `upload.backend: s3_native`, `sha256` is verified from the store's own
checksum when the store kept one (a multipart object may not have one, and is
then read back), and `blake3` by reading the stored bytes back. `validate
--print-effective` prints the method per stream as `verification_method`.

## `manifest_mac_key_env`

The **name** of an environment variable whose value is a 32-byte key in 64 hex
characters. When set, every manifest carries a keyed BLAKE3 MAC, and a missing
or invalid MAC fails verification and recovery. A configured but missing or
malformed key is a startup error. Unsigned manifests written before the
cutover are rejected, so verify any backlog first. Key rotation is not
implemented. See [Security model §8](../SECURITY_MODEL.md#8-manifest-authentication).

## `manifest`

| Key | Default | Meaning |
|---|---|---|
| `write_version` | `"0.3"` | The manifest version new batches get: `"0.3"` or `"0.4"`, quoted. Anything else, an unquoted number included, fails config load. `"0.4"` adds `verification_plan` (the configured verification method and `require_strong`) and is hashed over the stored document. |

Set `"0.4"` only once every engine, `vtopctl scrub` and auditor that reads the
archive runs v0.8.0 or later: v0.7.0 and earlier report `0.4` manifests as
tampered, and recovery on such an engine replays their batches. An engine
before this release ignores the section and keeps writing `0.3`, and so does
a misspelled key inside it, so check the startup line or
`validate --print-effective`. Setting `"0.3"` again goes back; `0.4`
manifests already written stay readable. See the protocol draft,
[§11.2](../VTOP_PROTOCOL_DRAFT.md#112-field-reference).

## `sources`

Each source is optional. A source block that is present but has
`enabled: false` is not read.

### `sources.kafka`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Read Kafka. |
| `bootstrap_servers` | **Required** in the block | Broker list. Must not be empty when enabled. |
| `consumer_group` | `vtop-engine` | Kafka `group.id`. |
| `topic_include_regex` | `.*` | Topics to read. |
| `topic_exclude_regex` | `^__.*` | Topics to skip. |
| `auto_offset_reset` | `earliest` | Kafka `auto.offset.reset` for a partition with no committed offset. |
| `enable_auto_commit` | `false` | Must be `false`; `true` is refused. Offsets are committed only after verification. |
| `security_protocol` | - | Kafka `security.protocol`. |
| `sasl_mechanism` | - | Kafka `sasl.mechanism`. GSSAPI needs a build with `--features kafka-gssapi`. |
| `sasl_username` | - | Kafka `sasl.username`. |
| `sasl_password_env` | - | Name of the environment variable holding the SASL password. |
| `ssl_ca_location` | - | Kafka `ssl.ca.location`. |

### `sources.file`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Read files. |
| `paths` | **Required** in the block | Glob patterns of files to read. |
| `whole_file` | `false` | Read each file as one record instead of line by line. Needed for binary or already-compressed files. |
| `delete_after_commit` | `false` | Delete a file once a committed batch covers it to its end and it has stayed unwritten for `delete_quiet_seconds`. On Linux the engine first takes a write lease, so a file any process still holds open is kept. Where no lease can be taken (macOS, NFS or FUSE, another user's file, a container without `CAP_LEASE`) the file is kept, unless `delete_without_open_check` is set. For finished files only, never a live log. |
| `delete_quiet_seconds` | `3600` | How long a fully archived file must stay unwritten before it is deleted. |
| `delete_without_open_check` | `false` | Where the engine can't check that no process holds a file open, delete it on `delete_quiet_seconds` alone. Risky: a writer still holding the file can lose lines if it stays silent past the quiet period and then writes again, or writes between the final check and the unlink. Where the check is possible it is always made. The engine warns at startup and in `vtopctl validate` when this is set. |

### `sources.syslog_spool`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Read syslog spool files written by rsyslog or syslog-ng. |
| `paths` | **Required** in the block | Glob patterns of spool files. |

## `upload`

| Key | Default | Meaning |
|---|---|---|
| `backend` | `s3_native` | `s3_native`, `localfs`, `awscli`, `s3cmd` or `minio` (the last three run an external client). `mock`, `mock_fail` and `mock_limited` are for tests. |
| `bucket` | **Required** | Target bucket. May use [placeholders](#partitioning), for example `telemetry-{format}`. |
| `prefix` | `""` | Key prefix. |
| `endpoint_url` | - | S3-compatible endpoint. `VTOP_S3_ENDPOINT_URL` overrides it. |
| `region` | `us-east-1` | Region. |
| `force_path_style` | `false` | Path-style addressing, as MinIO needs. `VTOP_S3_FORCE_PATH_STYLE` overrides it. |
| `verify_tls` | `true` | When true, a plaintext `http://` endpoint is refused. `false` is the lab opt-out that allows plaintext; it does not turn off certificate checks on `https://`. `VTOP_S3_VERIFY_TLS` overrides it. |
| `transport` | `tcp_tls` | Egress transport. `tcp_tls` is the only one registered. `VTOP_S3_TRANSPORT` overrides it. |
| `transports` | `{}` | Per-transport tuning, keyed by transport name. See [below](#uploadtransports). |
| `request_timeout_seconds` | `300` | `s3_native`: limit for one attempt of one request. Must be > 0. |
| `request_min_bytes_per_second` | `4096` | `s3_native`: a request with a body is also allowed its size at this rate, per attempt, so a slow link stretches the deadline. Must be > 0. |
| `max_egress_bytes_per_second` | - | `s3_native` only: a ceiling on this process's total upload body bytes per second. At least 1024. Other backends refuse it. |
| `max_concurrency_ceiling` | - | An operator maximum above the concurrency settings: `batching.max_concurrent_batches`, `batching.adaptive_width.min_width` and every `transports.*.max_concurrency` must be at or below it. |
| `create_bucket` | `false` | `s3_native` only: create the bucket if it does not exist. Needs the CreateBucket permission. |
| `require_strong_verification` | `true` | Commit only when verification is derived from stored content or a store-computed digest. `false` accepts same-size corruption; the engine warns at startup and in `validate`. |
| `require_object_versioning` | `false` | Hardened profile: the bucket must have versioning enabled and every manifest upload must return a version, which recovery then reads. |
| `local_path` | - | `localfs` only, and required there: objects are written under `<local_path>/<bucket>/<key>`. |
| `command_binary` | - | `awscli`, `s3cmd`, `minio`: absolute path of the client. Required for those backends; PATH is never searched. |
| `profile` | - | Command backends: named profile or alias (`minio` defaults to `local`). |
| `command_timeout_seconds` | `300` | Command backends: limit for each external command. |
| `command_max_output_bytes` | `1048576` | Command backends: most stdout or stderr captured. |
| `command_env_allowlist` | `[]` | Command backends: exact environment variable names passed to the otherwise empty child environment. |
| `multipart_part_size_bytes` | `8388608` (8 MiB) | Multipart part size. S3 needs at least 5 MiB for non-final parts. Must be > 0. |
| `multipart_threshold_bytes` | `8388608` (8 MiB) | Use multipart for objects at least this large, on backends that support it. Must be > 0. |
| `multipart_max_parallelism` | `4` | Parts in flight per object. Must be > 0; at most 10,000 on `s3_native`. |
| `multipart_abandon_after_secs` | `86400` | An incomplete multipart session older than this is treated as abandoned. Must be > 0. |

### `upload.transports`

Tuning for one transport, using the same keys for every transport. Only the
active `transport` may carry a non-empty block, and a key the transport cannot
honour is refused at startup.

| Key | Default | Meaning |
|---|---|---|
| `target_rate_bytes_per_second` | - | Intended rate. `tcp_tls` refuses it; the enforced cap is `upload.max_egress_bytes_per_second`. |
| `max_concurrency` | - | Objects in flight on this path; the smaller of this and `batching.max_concurrent_batches` applies. |
| `part_size_bytes` | - | Part size; for `tcp_tls` it maps onto `multipart_part_size_bytes`. |
| `parts_in_flight` | - | Parts per object; for `tcp_tls` it maps onto `multipart_max_parallelism`. |
| `redundancy` | `none` | Only `none` is implemented. |

## `partitioning`

| Key | Default | Meaning |
|---|---|---|
| `template` | `tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/` | Object key layout under `upload.prefix`. |
| `time_source` | `seal` | Which clock fills `{yyyy}` to `{hh}`: `seal`, when the batch was sealed; or `event`, the earliest record time the source reported (Kafka message timestamps), falling back to the seal time. |

Placeholders, in `template` and in `upload.bucket`: `{tenant}`, `{source}`,
`{format}` (the format's extension, such as `cef` or `jsonl`), `{yyyy}`,
`{mm}`, `{dd}`, `{hh}`, and `{retention_class}` for a stream that sets one. An
unknown placeholder resolves to nothing.

## Environment variables

Secrets and deployment overrides come from the environment; the list is on
[Config basics](basics.md#environment-variables).

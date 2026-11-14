# The archive path: sources, manifest, verification, recovery, metrics

Reference for the engine's verify-before-commit pipeline. The normative rules
are in [VTOP_PROTOCOL_DRAFT.md](../VTOP_PROTOCOL_DRAFT.md); how the code is
laid out is in [ARCHITECTURE.md](../ARCHITECTURE.md).

## The commit rule and the batch lifecycle

The protocol's **commit rule**
([VTOP_PROTOCOL_DRAFT.md §13](../VTOP_PROTOCOL_DRAFT.md#13-commit-rule)):

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A source progress marker is never committed until the batch completes this
lifecycle:

```text
DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED
  → OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED
```

Failure can happen from any state, and leads back to batching:

```text
ANY_STATE → FAILED → REPLAY_REQUIRED → BATCHING
```

Only `VERIFIED → SOURCE_COMMITTED` reaches the commit. Transitions such as
`SEALED → SOURCE_COMMITTED`, `COMPRESSED → SOURCE_COMMITTED`,
`CHECKSUMMED → SOURCE_COMMITTED`, `OBJECT_UPLOADED → SOURCE_COMMITTED` and
`MANIFEST_UPLOADED → SOURCE_COMMITTED` are rejected by
[`crates/vtop-core/src/state_machine.rs`](../../crates/vtop-core/src/state_machine.rs)
([protocol §12](../VTOP_PROTOCOL_DRAFT.md#12-state-machine)). The diagram is
[`docs/assets/vtop-state-machine.png`](../assets/vtop-state-machine.png).

## Source modes

| Source mode | Progress marker | Behavior |
|---|---|---|
| **Kafka** | topic, partition, offset range | Uses a Kafka consumer with auto-commit disabled. Each batch contains records from one topic and one partition. Offsets are committed only after verification. |
| **File** | path, inode, byte range | Reads append-only files line-oriented, or whole-file for binary/compressed sources (`whole_file`). Partial trailing lines are not committed. Replay resumes from the last safe byte offset. |
| **Syslog spool** | spool ID, byte range | Treats rsyslog or syslog-ng spool files as append-only inputs. External collectors own syslog delivery; VTOP owns batching, upload, verification, replay, and commit safety. |

## Format detection

When a stream does not explicitly define a format in
[`examples/streams.yaml`](../../examples/streams.yaml), the engine detects the
format per batch
([`crates/vtop-core/src/detect.rs`](../../crates/vtop-core/src/detect.rs)) and
records it in the manifest. A single engine can process different formats
across different streams (source A → CEF, source B → JSON Lines, source C →
syslog, source D → mixed batches).

| Format | Example output extension |
|---|---|
| CEF | `.cef.gz` |
| LEEF | `.leef.gz` |
| JSON | `.json.gz` |
| JSON Lines | `.jsonl.gz` |
| Syslog | `.syslog.gz` |
| Raw | `.raw.gz` |

## The manifest

The manifest is the transfer evidence record. It links the source type, source
name, source progress marker, object URI, object checksum, compression type,
detected format, batch metadata, and a reproducible manifest self-hash with
optional keyed-BLAKE3 authentication.

```json
{
  "protocol": "VTOP",
  "version": "0.3",
  "batch_id": "vtop-20260618T150000Z-app_events-p0-481000-482499-1a2b3c4d",
  "tenant": "default",
  "source_type": "kafka",
  "source_name": "app_events",
  "format": "cef",
  "compression": "gzip",
  "record_count": 1500,
  "source_progress": {
    "source_type": "kafka",
    "topic": "app_events",
    "partition": 0,
    "start_offset": 481000,
    "end_offset": 482499,
    "consumer_group": "vtop-engine"
  },
  "object": {
    "uri": "s3://telemetry-data/tenant=default/source=app_events/format=cef/year=2026/month=06/day=18/hour=15/vtop-....cef.gz",
    "size_bytes": 924822,
    "checksum_algorithm": "sha256",
    "checksum": "abc123..."
  },
  "manifest": {
    "uri": "s3://telemetry-data/.../vtop-....manifest.json",
    "sha256": "def456...",
    "mac": "0123abcd..."
  },
  "partitioning": {
    "path_template": "tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}",
    "resolved_prefix": "tenant=default/source=app_events/format=cef/year=2026/month=06/day=18/hour=15"
  },
  "upload_backend": "s3_native",
  "state": "manifest_uploaded",
  "verification_status": "not_verified",
  "created_at": "2026-06-18T15:00:02Z"
}
```

> [!NOTE]
> The manifest is written at `MANIFEST_UPLOADED`, before storage-side verification.
>
> The object is named by its source, range and content digest, plus the engine and inode for a file or spool range (`vtop-[<engine>-]<source>-<range>[-i<inode>]-<digest>`), so a replay that produces the same bytes resolves to the object already stored, and is never written over an object that does not verify (#626). The manifest is named by the per-attempt `batch_id`, whose trailing token is a random suffix. A replay with different batch boundaries (a source that grew since), a later seal-time hour, no checksum, or a command backend (`awscli`, `minio`, `s3cmd`) still writes a second object: see [ARCHITECTURE.md §13](../ARCHITECTURE.md) and the deterministic-naming rule of [VTOP_PROTOCOL_DRAFT.md §15.1](../VTOP_PROTOCOL_DRAFT.md#151-object-key).
>
> The authoritative post-verification state lives in the state store and can be queried with `vtopctl status` or `vtopctl list-batches --json`.
>
> This is a version `0.3` manifest, the default. With `manifest.write_version: "0.4"` (#671) new manifests say `"version": "0.4"` and add `"verification_plan": {"method": "read_back", "require_strong": true}`: the verification method configured for the backend and algorithm, and the strong-verification policy. Enable it only once every engine, `vtopctl scrub` and auditor of the archive runs v0.8.0 or later; v0.7.0 and earlier report `0.4` manifests as tampered. See [VTOP_PROTOCOL_DRAFT.md §11.2](../VTOP_PROTOCOL_DRAFT.md#112-field-reference).

The manifest self-hash is reproducible and detects accidental changes, but an
attacker able to rewrite the manifest can recompute it. Set
`manifest_mac_key_env` to the name of an environment variable containing a
32-byte hex key to add `manifest.mac`, a keyed BLAKE3 authenticator. Both
embedded values are blanked for canonicalization. The key itself is never
serialized. Enabling a key intentionally rejects older unsigned manifests;
verify the backlog before cutover. Key rotation is not implemented yet.

## Upload and verification

| Backend | Purpose | Verification level |
|---|---|---|
| native S3 | primary S3-compatible backend | service-computed SHA-256 or streamed BLAKE3 |
| AWS CLI | command-based backend | downloads and hashes stored content |
| s3cmd | command-based backend | downloads and hashes stored content |
| MinIO client | command-based backend | downloads and hashes stored content |
| LocalFS | local/air-gapped backend | streams stored files through the configured digest |
| mock | tests and local integration flow | hashes stored in-memory content |

> [!IMPORTANT]
> Strong verification is the default. A sidecar, ETag, or uploader-written user
> metadata is never accepted as proof of stored content.

The `awscli`, `s3cmd`, and `minio` compatibility backends are an explicit
opt-in. They require `upload.command_binary` to be an absolute path, verify the
tool's `--version` identity at startup, clear the child environment, and apply
wall-clock and captured-output limits. Add only the exact runtime variable
names the selected tool needs to `upload.command_env_allowlist`; values are
resolved at startup and are never serialized. Native `s3_native` does not
spawn an external process and does not use these settings.

## Replay and recovery

Recovery is designed around one rule: **unverified data must remain
replayable.**

| Crash point | Recovery action |
|---|---|
| before object upload | replay from source |
| after object upload but before verification | replay from source |
| after verification but before source commit | retry source commit |
| after source commit | batch is complete |

If verification fails, the batch goes to `FAILED` and source progress is not
committed. If the commit fails after verification, the batch stays `VERIFIED`
and recovery retries the source commit. The tests that hold this are
[`tests/integration_replay.rs`](../../tests/integration_replay.rs) and
[`tests/integration_state_recovery.rs`](../../tests/integration_state_recovery.rs).

## Metrics

VTOP emits structured per-batch metrics, for example:

```text
3 records, 114 B->80 B (1.43x, 29.8% saved) in 6 ms | 500 rec/s, 0.00 MiB/s up |
stages: compress=0ms checksum=0ms put_obj=0ms put_manifest=0ms verify=0ms commit=0ms
```

| Metric area | Examples |
|---|---|
| **Size and transfer** | uncompressed bytes, compressed bytes, compression ratio, percentage saved |
| **Latency** | compression, checksum, object upload, manifest upload, verification, commit |
| **Throughput** | records/sec, uncompressed MiB/sec, effective upload MiB/sec |

`vtopctl process-once --json` includes the full metrics object per batch.
Prometheus metrics are exported by the engine at `/metrics` when
`VTOP_METRICS_ADDR` is set. See [observability/](../../observability/) for the
optional Grafana LGTM stack (Alloy + Mimir/Loki/Tempo) and dashboards, and
[`crates/vtop-core/src/metrics.rs`](../../crates/vtop-core/src/metrics.rs) for
the implementation.

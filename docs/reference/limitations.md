# Known limitations, and what is done

VTOP is a prototype. The limits below are known and intentional. Per-release
limitations of the cluster plane are in [ROADMAP.md](../ROADMAP.md), and the
engine's HA status is in [PRODUCTION_HA.md](../PRODUCTION_HA.md).

## Known limitations

| Area | Current behavior | Planned direction |
|---|---|---|
| Deterministic object naming | partly — objects are keyed by source, range and content digest, so a replay of the same bytes reuses the stored object (#626); a replay with other boundaries, a later seal-time hour, no checksum or a command backend still writes a second object, and manifests stay per attempt ([protocol §15.1](../VTOP_PROTOCOL_DRAFT.md#151-object-key)/§16) | reproducible batch boundaries, per-range manifests ([PRODUCTION_HA.md Phase 4](../PRODUCTION_HA.md)) |
| Verification classes | strong (default) / backend-limited / disabled (size-only), per [protocol §17](../VTOP_PROTOCOL_DRAFT.md#17-verification-semantics); non-strong commit requires explicit opt-out | — |
| Large objects | native S3 + mock support resumable multipart with persisted sessions (`vtopctl tier copy --multipart-state-dir`); compatibility backends still single-shot `put_object` | wire remaining backends / streaming rehydrate |
| Large records / whole files | `max_bytes` is a hard per-source/per-batch ceiling; an oversized record is rejected without advancing source progress | raise the explicit budget only when the deployment has matching memory headroom |
| Partial upload recovery | replays from source instead of resuming half-written local objects | add resumable local staging |
| Command backend verification cost | `aws`, `s3cmd`, and `mc` download each stored object to hash it | prefer native S3 SHA-256 when read-back bandwidth is costly |
| Syslog timestamps | `received_time_*` is not yet extracted into the spool marker | add timestamp extraction |
| Manifest integrity | self-hash plus optional keyed-BLAKE3 authentication; key rotation not implemented | add multi-key rotation and public-key signatures if required |
| Object immutability | manifest version pinning + bucket-versioning validation implemented (hardened profile, `upload.require_object_versioning`); Object Lock retention itself is deployment policy | document an Object Lock deployment profile |
| Metrics export | **Prometheus `/metrics` implemented** (opt-in via `VTOP_METRICS_ADDR`); OpenTelemetry trace export not yet | add OTLP span export |
| Kafka integration test | requires live broker and is ignored by default | add optional CI service profile |
| Binary / pre-compressed inputs | **supported** via the file source `whole_file` mode (archived verbatim, byte-exact) | streaming for very large files |
| Local filesystem backend | **available** (`backend: localfs`, objects under `local_path/<bucket>/<key>`; sidecars are inventory hints only) | — |
| Checksums | **SHA-256 and BLAKE3**, or disabled (size-only); strong verification defaults on and `require_strong_verification: false` is an explicit weak-mode opt-out | — |
| Size-only verification | `upload.require_strong_verification: false` **accepts same-size corruption**: a corrupted or replaced object of the recorded size is committed as verified. It is announced, not refused: one WARN at engine startup and a `vtopctl validate` warning naming the key, `vtop_verification_mode{mode="size_only"} 1` from startup (exported by `vtopctl run` with `VTOP_METRICS_ADDR`), and a WARN with the batch id for every size-only verification, counted in `vtop_verification_backend_limited_total` where `vtop_verified_total` counts it: at VERIFIED in the running engine, at the commit in recovery (#629) | — |

## Done

- [x] local filesystem upload backend (`backend: localfs`)
- [x] BLAKE3 checksum strategy (and checksum-disabled mode)
- [x] binary / pre-compressed input framing (file source `whole_file` mode)
- [x] strong-verification gate (`require_strong_verification`)
- [x] **Prometheus metrics exporter** — the engine serves `/metrics` (`/healthz`,
      `/readyz`) behind `VTOP_METRICS_ADDR`; see [observability/](../../observability/)
      for the optional Grafana LGTM stack and dashboards
- [x] **end-to-end smoke + live-broker Kafka CI** over the full compose lab
- [x] Kafka is isolated behind the optional `kafka` Cargo feature
- [x] first Kafka-independent native segment-log storage kernel
- [x] bounded native produce/fetch wire codec and TLS-1.3 mTLS local-broker
      library with durable producer-epoch fencing and committed-only fetch
- [x] bounded file/syslog/whole-file reads, pre-clone Kafka record checks, and
      streaming local compression; `max_bytes` is enforced before source
      progress advances
- [x] multipart resumable upload (native S3 + mock; persisted sessions, fencing, abandoned cleanup)
- [x] optional keyed-BLAKE3 manifest authentication via a named secret env var
- [x] **native three-node metadata/control plane** — an embedded Raft group
      with linearizable reads, membership change, and an mTLS admin endpoint
      whose commands are authorized by verified caller identity
- [x] **leader→follower quorum replication** — persistent mTLS streams,
      pipelined batches, per-follower flow control and memory budgets, and a
      committed high-water mark that fetch never reads past
- [x] **lease-driven range leadership with fencing epochs** — metadata grants
      the range, a grant always mints `epoch + 1`, and both leaders and
      followers refuse anything carrying an older one
- [x] **verified promotion** — a new leader establishes its committed boundary
      from a quorum of *fenced* replicas before serving, and refuses rather
      than guessing when a quorum cannot be reached
- [x] **epoch-qualified recovery (v0.2.0)** — each replica records which
      fencing epoch wrote each stretch of its log (KIP-101 style), reconciles
      against the new leader while fenced, and truncates a diverged tail
      instead of being stranded — bounded so it can never discard
      acknowledged records
- [x] **15-scenario live-chaos suite in CI** — real processes over real TLS:
      SIGKILL durability, disk-full and fsync failure, metadata partition,
      clock skew, membership change under load, and range-leader failover
- [x] **signed, attested releases** — multi-arch image and per-target binaries
      with keyless Sigstore signatures, SBOM, and provenance attestations

## Not implemented yet

- [ ] manifest MAC key rotation / optional public-key signatures
- [ ] S3 Object Lock profile
- [ ] OpenTelemetry trace export (the metrics endpoint exists; spans do not yet)
- [ ] million-file benchmark suite

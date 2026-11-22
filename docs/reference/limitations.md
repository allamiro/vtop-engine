# Known limitations, and what is done

VTOP is a prototype. The limits below are known and intentional. Per-release
limitations of the cluster plane are in [ROADMAP.md](../ROADMAP.md), and the
engine's HA status is in [PRODUCTION_HA.md](../PRODUCTION_HA.md). Every
data-safety bug found so far, with when it was introduced and fixed, is in the
[data-safety register](data-safety-register.md).
Which components are production, preview or experimental is in the
[component status](component-status.md).

## Known limitations

| Area | Current behavior | Planned direction |
|---|---|---|
| Deterministic object naming | partly — objects are keyed by source, range and content digest, so a replay of the same bytes reuses the stored object (#626); replays are rebuilt as first cut; a range that cannot be rebuilt as cut, a later seal-time hour for a row from before v0.10.0, no checksum, or a command backend whose failed attempt's object was not recorded or does not verify, still writes a second object (a command backend adopts an intact one, #773), and manifests stay per attempt ([protocol §15.1](../VTOP_PROTOCOL_DRAFT.md#151-object-key)/§16) | reproducible batch boundaries, per-range manifests ([PRODUCTION_HA.md Phase 4](../PRODUCTION_HA.md)) |
| Verification classes | strong (default) / backend-limited / disabled (size-only), per [protocol §17](../VTOP_PROTOCOL_DRAFT.md#17-verification-semantics); non-strong commit requires explicit opt-out | — |
| Large objects | `s3_native` (and the mock) upload above `upload.multipart_threshold_bytes` as resumable multipart with persisted sessions, in the engine (#482, #668) and in `vtopctl tier copy --multipart-state-dir`; the command backends hand the whole object to their tool, which may split it internally but cannot resume, and `localfs` writes one file | wire remaining backends / streaming rehydrate |
| Large records / whole files | `max_bytes` is a hard per-source/per-batch ceiling; an oversized record is rejected without advancing source progress | raise the explicit budget only when the deployment has matching memory headroom |
| Ranges held at recovery | a range whose replay decision recovery could not record (#639) keeps its ledger row as it was (`BATCHING`, `FAILED` or `VERIFIED`), and a range above it in the same source unit, whose rewind recovery applies at the held start instead (#782), stays `REPLAY_REQUIRED`, until the next start retires or replays them. Both are read again in that process, so nothing is lost; the ranges above the held one are archived twice, and the old rows wait for one restart. A held range with no rewind above it is read again from where the source starts, which is the committed point except for a file rotated away while the engine was down and a Kafka partition whose consumer group has never committed (it follows `auto.offset.reset`): there it waits for a start that can record its decision | record the decision again in the running engine instead of at the next start |
| Partial upload recovery | a failed multipart upload on `s3_native` keeps its completed parts, and the replay of the same bytes resumes it (#668); anything else, including a half-written local object, replays from the source | add resumable local staging |
| Command backend verification cost | `aws`, `s3cmd`, and `mc` download each stored object to hash it | prefer native S3 SHA-256 when read-back bandwidth is costly |
| Syslog timestamps | `received_time_*` is not yet extracted into the spool marker | add timestamp extraction |
| Manifest integrity | self-hash plus optional keyed-BLAKE3 authentication; key rotation through key ids and a keyring (`manifest.mac`, version 0.4 manifests only, every reader on the keyring release first) | add public-key signatures if required |
| Object immutability | manifest version pinning + bucket-versioning validation (hardened profile, `upload.require_object_versioning`); **S3 Object Lock profile implemented** (#684, `upload.object_lock`, `s3_native` only): objects and manifests written under retention, the bucket's Object Lock checked, the stored retention verified before VERIFIED. The bucket and its default retention are the operator's; older engines ignore the key ([OBJECT_LOCK_DEPLOYMENT.md](../OBJECT_LOCK_DEPLOYMENT.md)); `vtopctl tier copy` writes cold-tier copies under the profile and checks their retention before the tier evidence | — |
| Encryption at rest | **S3 server-side encryption implemented** (`upload.server_side_encryption`, `s3_native` only): `sse-s3` or `sse-kms` asked for on every object, manifest, receipt and multipart create, and the store's reported encryption verified before VERIFIED, so an unencrypted write is never committed. VTOP holds no keys, and does not do SSE-C, DSSE-KMS or client-side encryption. With `sse-kms` and no `kms_key_id`, only the algorithm is checked, not which key; for a key alias likewise. The compatibility backends (`awscli`, `s3cmd`, `minio`) and `localfs` refuse the key. The state ledger, the work directory and the local segment log are not encrypted by VTOP (use disk or volume encryption). Objects written before the mode was set are reported by `vtopctl scrub` as not encrypted ([SECURITY_MODEL.md §7.1](../SECURITY_MODEL.md#71-encryption-at-rest-uploadserver_side_encryption)) | key checks beyond the algorithm for a default key or an alias |
| Metrics and trace export | **Prometheus `/metrics` implemented** (opt-in via `VTOP_METRICS_ADDR`); **OpenTelemetry export implemented** (#683): traces of the batch lifecycle, recovery and scrub, the same metrics over OTLP, and trace ids in the JSON logs, built in and idle until `OTEL_EXPORTER_OTLP_ENDPOINT` is set (HTTP/protobuf only, no gRPC) | gRPC transport if a deployment needs it |
| Kafka brokers | the live-broker suite runs against Apache Kafka 3.7 to 4.4 (KRaft, one broker) nightly, and against a three-broker 4.4 cluster with a leader stopped and started mid-read, through plaintext, TLS, mutual TLS (a client certificate, `ssl_certificate_location` and `ssl_key_location`; 3.7 and 4.4) and SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) listeners, with a smoke on Redpanda; the 3.7 and 4.3 lines run on every pull request that changes the Kafka path (plaintext only); ZooKeeper-era brokers are expected to work and untested ([install/kafka.md](../install/kafka.md)) | — |
| Binary / pre-compressed inputs | **supported** via the file source `whole_file` mode (archived verbatim, byte-exact) | streaming for very large files |
| Local filesystem backend | **available** (`backend: localfs`, objects under `local_path/<bucket>/<key>`; sidecars are inventory hints only; each object, manifest and sidecar is fsynced, renamed into place and the directory fsynced, along with any directory it created, before the write is reported done, so verification reads what a power loss would keep. A power-loss test checks this against the worst case POSIX allows, at every sync of a write: an object is never torn, a replaced one is old or new, and one the write reported done survives (`localfs_backend::power_loss`; a model of what the disk keeps, since no CI runner can drop its page cache). The durability then depends on the filesystem and device honouring `fsync`) | — |
| Checksums | **SHA-256 and BLAKE3**, or disabled (size-only); strong verification defaults on and `require_strong_verification: false` is an explicit weak-mode opt-out | — |
| Size-only verification | `upload.require_strong_verification: false` **accepts same-size corruption**: a corrupted or replaced object of the recorded size is committed as verified. It is announced, not refused: one WARN at engine startup and a `vtopctl validate` warning naming the key, `vtop_verification_mode{mode="size_only"} 1` from startup (exported by `vtopctl run` with `VTOP_METRICS_ADDR`), and a WARN with the batch id for every size-only verification, counted in `vtop_verification_backend_limited_total` where `vtop_verified_total` counts it: at VERIFIED in the running engine, at the commit in recovery (#629) | — |
| Long-run stability | a 72 hour Kafka to S3 soak with a fault every 30 minutes (kill -9, broker restart, partition, store outage, clock step) and a pass/fail evaluator is built (#726, [SOAK_VALIDATION.md](../SOAK_VALIDATION.md)); **no 72 hour result is attached to a release yet**, and the soak injects no disk-full fault | a maintainer runs it before each release candidate and attaches the evidence |
| Cluster-plane long-run stability | a 7-day soak of three metadata and three data nodes under continuous produce with a fault every hour (leader kills, partition, pause, disk full, clock step) and a pass/fail evaluator is built (#727, [CLUSTER_SOAK_VALIDATION.md](../CLUSTER_SOAK_VALIDATION.md)); **no 7-day result exists yet**, and on this build the 12-minute smoke profile does not pass reliably: in two of three full runs the range stopped serving after a disk filled under a segment roll or a lagging replica won the lease | a maintainer runs it on a dedicated host and attaches the evidence to #727 |

## Done

- [x] local filesystem upload backend (`backend: localfs`)
- [x] BLAKE3 checksum strategy (and checksum-disabled mode)
- [x] binary / pre-compressed input framing (file source `whole_file` mode)
- [x] strong-verification gate (`require_strong_verification`)
- [x] **Prometheus metrics exporter** — the engine serves `/metrics` (`/healthz`,
      `/readyz`) behind `VTOP_METRICS_ADDR`; see [observability/](../../observability/)
      for the optional Grafana LGTM stack and dashboards
- [x] **OpenTelemetry export** (#683) — a span per batch with a child per step
      (seal, compress, checksum, upload, manifest, verify, commit), plus
      recovery and scrub; the `/metrics` series over OTLP; trace ids in the
      JSON logs. Built into `vtopctl` and `vtop-node`, idle until
      `OTEL_EXPORTER_OTLP_ENDPOINT` is set; see
      [observability/](../../observability/README.md#opentelemetry)
- [x] **end-to-end smoke + live-broker Kafka CI** over the full compose lab
- [x] Kafka is isolated behind the optional `kafka` Cargo feature
- [x] first Kafka-independent native segment-log storage kernel
- [x] bounded native produce/fetch wire codec and TLS-1.3 mTLS local-broker
      library with durable producer-epoch fencing and committed-only fetch
- [x] bounded file/syslog/whole-file reads, pre-clone Kafka record checks, and
      streaming local compression; `max_bytes` is enforced before source
      progress advances
- [x] multipart resumable upload (native S3 + mock; persisted sessions, fencing, abandoned cleanup; a failed part keeps the completed ones, #668)
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
- [x] **live-chaos suite in CI** — every scenario under
      `scripts/live-chaos/scenarios/`, on real processes over real TLS:
      SIGKILL durability, disk-full and fsync failure, metadata partition,
      clock skew, membership change under load, range-leader failover, the
      Kafka gateway with stock clients, and a rolling metadata upgrade
- [x] manifest MAC key rotation (#685)
- [x] S3 Object Lock profile (#684)
- [x] **signed, attested releases** — multi-arch image and per-target binaries
      with keyless Sigstore signatures, SBOM, and provenance attestations

## Not implemented yet

- [ ] optional public-key manifest signatures
- [ ] million-file benchmark suite

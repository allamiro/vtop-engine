# Security Model

> Security model for the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.
>
> Protocol-level behavior referenced here (state machine, commit rule §13, replay rule §14, verification semantics §17, manifest spec §11) is normatively defined in [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md); this document adds the security rules around it.

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are used as normative requirements for conformant behavior.

## Table of contents

1. [Threat model](#1-threat-model)
2. [Transport security](#2-transport-security)
3. [Credential handling](#3-credential-handling)
4. [Manifest confidentiality](#4-manifest-confidentiality)
5. [Object storage permissions and least privilege](#5-object-storage-permissions-and-least-privilege)
6. [Integrity verification and chain of custody](#6-integrity-verification-and-chain-of-custody)
7. [Data at rest and object immutability](#7-data-at-rest-and-object-immutability)
8. [Manifest authentication](#8-manifest-authentication)
9. [Resource exhaustion controls](#9-resource-exhaustion-controls)
10. [Audit and failure logging](#10-audit-and-failure-logging)
11. [Secret redaction](#11-secret-redaction)
12. [Container and runtime hardening](#12-container-and-runtime-hardening)
13. [Supply-chain security](#13-supply-chain-security)
14. [Security properties provided vs. not provided](#14-security-properties-provided-vs-not-provided)
15. [Summary of normative rules](#15-summary-of-normative-rules)
16. [Cryptographic primitive inventory](#16-cryptographic-primitive-inventory)

---

## 1. Threat model

### 1.1 Assets

| Asset | Why it matters |
|-------|----------------|
| Telemetry data in flight | May contain sensitive logs (auth, audit, security events). |
| Telemetry objects at rest | Long-lived archival; integrity and immutability matter for audit/compliance. |
| Manifests | Bind object hash to source markers; the chain-of-custody record. |
| Source progress markers / state store | Authoritative record of what has been safely committed. |
| Credentials | Kafka SASL/mTLS material, object-storage keys, manifest MAC/signing keys. |

### 1.2 Adversaries

| Adversary | Capability assumed |
|-----------|--------------------|
| Network attacker | Can observe/modify traffic between engine and Kafka or object storage if unprotected. |
| Storage tamperer | Can attempt to alter or overwrite stored objects/manifests after write. |
| Curious/over-broad operator | Has more storage permissions than needed; may read or delete objects. |
| Log/exfil observer | Reads logs, process arguments, or images hoping to recover secrets. |
| Malicious/compromised dependency | Reaches the engine via the software supply chain. |

### 1.3 Trust boundaries

| Boundary | Control |
|----------|---------|
| Source ↔ engine | Transport security (TLS/SASL/mTLS); engine owns commit, source never self-commits. |
| Engine ↔ object storage | TLS; integrity verification of stored object + manifest; least-privilege credentials. |
| Engine ↔ state store | The database is trusted for ledger correctness, progress durability, and availability, but not for telemetry-object integrity. Remote PostgreSQL requires hostname-verified TLS; its URL is resolved from an env/file secret reference and never serialized. |
| Engine ↔ external CLI backends | Version-pinned tools executing outside the Rust dependency graph; stored objects are downloaded and hashed. |
| Engine ↔ operator/logs | Secret redaction; manifests carry no secrets. |
| Native client ↔ broker | TLS 1.3 with mandatory client certificates, explicit certificate/principal/role authorization, bounded frames/sessions/windows, and range/producer fencing checks. |
| Build ↔ runtime | Supply-chain auditing; container hardening. |

---

## 2. Transport security

### TLS for Kafka

- Connections to Kafka brokers **SHOULD** use TLS.
- Certificate validation **SHOULD** be enabled; disabling validation **MUST** be an explicit, logged, non-default configuration.

### TLS for S3-compatible endpoints

- Connections to S3-compatible object storage endpoints **SHOULD** use TLS (HTTPS).
- Custom CA bundles **MAY** be supplied for private/self-hosted endpoints.

### TLS for PostgreSQL state stores

- PostgreSQL over a Unix socket or loopback address **MAY** use plaintext for local development and CI.
- Every non-loopback PostgreSQL connection **MUST** use `sslmode=verify-full`, which verifies both the issuing CA and the database hostname.
- A private database CA **MUST** be supplied with `sslrootcert` when it is not in the bundled public trust roots. `sslmode=require` and `verify-ca` are rejected for remote databases because they do not provide the required hostname verification.

### Authentication mechanisms

- Kafka authentication **SHOULD** support **SASL/SCRAM** and **mTLS**.
- The selected mechanism and identity **MAY** be logged, but associated secrets **MUST NOT** be logged.

### Native broker transport

- The native broker transport is restricted to TLS 1.3 and requires a client
  certificate chained to the configured client roots. The transport gained
  plaintext modes in #294 slices 3 and 4 (the replica plane, then the native
  produce/fetch plane), but no node config key, `vtopctl` flag or Helm value
  reaches either, so the restriction holds for every deployable
  configuration; #294 is where selecting it would be decided. A plaintext
  replica plane refuses the verbs whose authorization depends on a client
  certificate; a plaintext native plane admits a session only if the
  deployment's `SessionAuthorizer` accepts the declared principal without a
  certificate in so many words (`authorize_unverified`, refusing by default),
  and both refuse at bind time to serve on a non-loopback address unless
  constructed with the exposure acknowledged by name.
- Certificate validity alone does not grant a role. The embedding deployment
  **MUST** supply a `SessionAuthorizer` that binds the peer certificate chain,
  declared principal ID, and requested producer/consumer role. Produce requests
  additionally require `producer_id == principal_id` before the durable producer
  epoch journal can be changed.
- Native clients **MUST** validate the broker certificate against their
  configured roots and the expected server identity (no insecure or
  accept-any verifiers). TLS peer authentication is mutual: server-side client
  verification alone does not protect a client from connecting to an imposter
  broker.
- Wire frames are length-bounded and BLAKE3-checksummed independently of TLS.
  The checksum detects accidental framing corruption; TLS provides peer
  authentication, confidentiality, and active-tamper protection.
- Session count, global in-flight work, negotiated record/frame limits, idle
  timeouts, and fetch-response byte credit are explicit resource boundaries.

## 3. Credential handling

Normative rules:

- **Credentials MUST NOT be stored in manifests.**
- **Credentials MUST NOT be printed in logs.**
- **Credentials SHOULD be supplied through environment variables, mounted secrets, or external secret managers.**

Additional guidance:

- Configuration files containing secrets **SHOULD** have restrictive filesystem permissions.
- Credentials **SHOULD NOT** be passed as plaintext command-line arguments where avoidable, since arguments may be visible to other processes.
- The engine **SHOULD** support loading credentials from external secret managers without persisting them to disk.
- Inline SQLite paths remain valid. PostgreSQL URLs **MUST** be referenced as `engine.state_store: { env: VTOP_STATE_STORE }` or `engine.state_store: { file: /run/secrets/vtop-state-store }`; inline PostgreSQL URLs are rejected before startup.
- Secret files may contain one PostgreSQL URL with a trailing newline. The resolved URL is held only in an opaque runtime value and is not retained in serializable configuration.
- PostgreSQL schema migrations **MUST** run as a separate deployment identity via `vtopctl migrate`; normal engine startup performs no DDL.
- The PostgreSQL engine identity **SHOULD** have only database `CONNECT`, schema `USAGE`, and `SELECT, INSERT, UPDATE` on the `batches` ledger. It **MUST NOT** own the table or receive `CREATE`, `ALTER`, `DROP`, `DELETE`, or `TRUNCATE` privileges.
- The privileged migration secret **MUST NOT** be mounted into the engine workload. See [PostgreSQL deployment](POSTGRES_DEPLOYMENT.md) for the rollout and grant sequence.

## 4. Manifest confidentiality

- Manifests describe object integrity and source progress only.
- Manifests **MUST NOT** contain credentials, tokens, or other authentication material.
- Manifests **SHOULD NOT** contain raw telemetry payload contents beyond the integrity metadata necessary for verification.

## 5. Object storage permissions and least privilege

- Object storage credentials **SHOULD** follow **least privilege**: the minimum permissions required to put, get/head (for verification), and list within the configured prefix.
- Delete permissions **SHOULD NOT** be granted unless an operational lifecycle policy explicitly requires them.
- Separate read-only credentials **SHOULD** be used for downstream consumers.

### 5.1 Per-backend least privilege

| Backend | Minimum permissions | Notes |
|---------|---------------------|-------|
| `s3_native` | `PutObject`, `GetObject`/`HeadObject` (verify), `ListBucket` within prefix | SHA-256 uses a service-computed checksum; BLAKE3 requires read-back. |
| `awscli` / `s3cmd` / `minio mc` | Same as above | Strong verification downloads and hashes the stored body. |
| LocalFS | Filesystem write/read on the object tree only | The object tree directory **SHOULD** have restrictive permissions; the engine **SHOULD NOT** require broader filesystem access. |

Command compatibility backends (`awscli`, `s3cmd`, `minio mc`) **MUST** use an
explicit absolute executable path; PATH lookup is forbidden. VTOP resolves the
path and verifies the expected `--version` identity at startup. Each child
starts with an empty environment plus `LC_ALL=C`; only exact names declared in
`upload.command_env_allowlist` are copied from the runtime environment.
Invocations are killed and reaped after `upload.command_timeout_seconds`, and
captured stdout/stderr is bounded by `upload.command_max_output_bytes`.

### 5.2 On-demand bucket creation (`CreateBucket`) implications

Per-format buckets (e.g. `telemetry-{format}`) with optional on-demand creation require `CreateBucket` (and possibly bucket-policy) permissions. Granting `CreateBucket`:

- **SHOULD** be scoped to a dedicated provisioning identity, or buckets **SHOULD** be pre-created so the runtime identity does **not** hold `CreateBucket`.
- **MUST NOT** be combined with broad delete permissions on the same identity without an explicit lifecycle justification.
- Broadens blast radius (an over-broad identity could create unexpected buckets); operators **SHOULD** prefer pre-provisioned buckets in production.

## 6. Integrity verification and chain of custody

- The engine **MUST** compute a content checksum (SHA-256 or BLAKE3; or size-only when checksums are explicitly disabled) over the compressed telemetry object.
- The engine **MUST** verify the durably stored object against the manifest before transitioning to `VERIFIED`.
- The engine **MUST** verify the stored manifest before committing source progress.
- Strong verification **MUST** be derived from stored content or a checksum the storage service computed over that content. Uploader-written sidecars, ETags, and user-metadata digests **MUST NOT** be classified as strong ([VTOP_PROTOCOL_DRAFT.md §17.1](VTOP_PROTOCOL_DRAFT.md#17-verification-semantics)).
- A source progress marker **MUST NOT** be committed unless both object and manifest verification succeed (the commit rule, [VTOP_PROTOCOL_DRAFT.md §13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule)).
- The manifest binds the object hash to the covered source progress markers. Its unkeyed **self-hash** is reproducible corruption detection, not authenticity: a writer who can replace the document can recompute it.
- When `manifest_mac_key_env` is configured, the stored manifest **MUST** carry a valid keyed BLAKE3 `manifest.mac`; missing or invalid MACs fail pipeline, CLI, and recovery verification.
- Where only size/existence can be confirmed, verification is **backend-limited** and the engine **MUST** report it as such rather than as cryptographic verification. The engine defaults to rejecting this result; accepting it requires the explicit `require_strong_verification: false` compatibility opt-out.
- A keyed MAC authenticates data among key holders but does not provide public verification or non-repudiation.

## 7. Data at rest and object immutability

- Object immutability **SHOULD** be supported where the backend allows it ([VTOP_PROTOCOL_DRAFT.md §18](VTOP_PROTOCOL_DRAFT.md#18-security-considerations)). VTOP validates bucket versioning (§8.1); object-lock *configuration* is deployment policy, not automated by the engine.
- Where the backend supports it (e.g., S3 Object Lock / WORM), telemetry objects and manifests **SHOULD** be written as immutable for the configured retention period.
- Immutability complements verification: verification detects tampering, immutability prevents post-write tampering or accidental overwrite.
- At-rest encryption (server-side or bucket-default) **MAY** be enabled at the storage layer; it is orthogonal to VTOP's integrity guarantees.

## 8. Manifest authentication

- VTOP 0.2 supports an optional keyed BLAKE3 authenticator in `manifest.mac` ([VTOP_PROTOCOL_DRAFT.md §11.2](VTOP_PROTOCOL_DRAFT.md#11-manifest-object), §17.3).
- Config stores only the environment-variable name (`manifest_mac_key_env`); the 32-byte hex key **MUST NOT** appear in config serialization, manifests, or logs.
- Naming an absent or malformed key **MUST** fail startup rather than silently emit unsigned manifests.
- Enabling a key deliberately rejects unsigned pre-cutover manifests. Operators **MUST** verify or explicitly migrate their backlog before enabling it.
- One active key is supported. Rotation and public-key signatures are not implemented.

### 8.1 Freshness: version pinning and the hardened profile (#135)

The MAC establishes *authenticity* (these bytes were produced by a key
holder); it cannot establish *freshness* — a writer can delete a signed
manifest or replay an older, still-validly-signed one over the current key.
Version pinning plus storage retention closes that gap; the controls are
complementary.

> Version pinning is an **implementation extension** of the reference
> implementation, layered under the extensibility contract of
> [VTOP_PROTOCOL_DRAFT.md §20](VTOP_PROTOCOL_DRAFT.md#20-extensibility): it
> strengthens manifest verification (§17.3) without weakening the commit rule
> (§13) or replay rule (§14). The protocol draft itself does not (yet) define
> manifest version pinning; the MUSTs below bind this implementation's
> hardened profile, not every conformant implementation.

- When the backend assigns an immutable object version on manifest upload
  (S3 `x-amz-version-id`), the engine records it in the durable ledger
  (`manifest_version_id`), and every later read — the pre-commit stored-bytes
  authentication and the recovery re-check — **MUST** address that exact
  version, never the mutable current key.
- A recorded version that can no longer be read **MUST** fail closed: the
  batch is transitioned to `FAILED` then `REPLAY_REQUIRED` (the only legal
  path, protocol §12/§14), and source progress is not committed. Recovery
  **MUST NOT** fall back from a pinned version to the current key.
- With `upload.require_object_versioning = true` (the hardened profile), the
  backend **MUST** expose immutable object versions, bucket versioning
  **MUST** be preflighted before the first upload to each bucket, and a
  manifest upload that returns no immutable version (including S3's literal
  `null` version from a suspended bucket) **MUST** fail the batch.
- Retention is the storage layer's half of the guarantee: bucket versioning
  keeps overwritten versions, and S3 **Object Lock** (compliance or
  governance retention covering the archive's audit window) **SHOULD** be
  configured so a privileged deleter cannot remove the pinned version itself.
  VTOP validates versioning; object-lock configuration is deployment policy.
- Backends without object versions (`localfs`, the `awscli`/`s3cmd`/`minio`
  command backends) record no version and keep today's current-key behavior
  with ledger hash binding; the hardened profile refuses to run on them.
  Rows written before this feature (no recorded version) are verified the
  same legacy way.

## 9. Resource exhaustion controls

Resource exhaustion controls are part of the security boundary:

- `batching.max_bytes` is a hard ceiling for a source read. File and syslog
  readers stop before allocating beyond it; whole-file and Kafka records that
  exceed it fail without advancing source progress.
- Compression streams directly to the staging file instead of holding both an
  uncompressed aggregate and compressed aggregate in memory.
- Successfully processed staging objects/manifests are removed immediately.
  Crash-left VTOP artifacts are removed after `engine.work_retention_seconds`,
  with `engine.work_max_bytes` enforcing an oldest-first aggregate ceiling on
  mutating-engine startup. Locks, directories, and unrelated files are excluded
  from cleanup. Symlinks are never followed; on Unix, deletion is anchored to
  an open directory and the regular file's device, inode, size, and no-follow
  entry type are revalidated immediately before unlinking. Changed entries are
  retained for the next cleanup pass.

## 10. Audit and failure logging

- The engine **SHOULD** emit structured audit logs for batch lifecycle events (seal, upload, verify, commit) including `batch_id`, object key, and outcome.
- Failures **SHOULD** be logged with enough context to support replay and forensic review, **without** including secrets or raw sensitive payloads.
- Audit logs **SHOULD** be append-oriented and suitable for retention alongside the archived objects.

## 11. Secret redaction

- Any log path, error type, or diagnostic that could surface credentials **MUST** redact them.
- Connection strings, headers, and configuration dumps **MUST** have secret fields masked before logging.
- PostgreSQL parse/connect errors **MUST NOT** echo the supplied URL. VTOP connects from parsed options and applies URL redaction at the state-store error boundary as defense in depth.
- The redaction layer **SHOULD** default to redacting unknown sensitive-looking fields rather than printing them.

## 12. Container and runtime hardening

- Container images **SHOULD** run as a non-root user.
- Images **SHOULD** use minimal/distroless-style bases to reduce attack surface.
- Filesystems **SHOULD** be mounted read-only except for required working/state directories (e.g. the SQLite state store and any LocalFS object tree).
- Linux capabilities **SHOULD** be dropped to the minimum required.
- Secrets **SHOULD** be provided via mounted secrets or the orchestrator's secret store, never baked into images.

## 13. Supply-chain security

- Dependencies **SHOULD** be pinned and audited (e.g., dependency vulnerability scanning).
- Builds **SHOULD** be reproducible where practical, and release artifacts **SHOULD** be checksummed and **MAY** be signed.
- A software bill of materials (SBOM) **SHOULD** be produced for releases.
- Third-party upload backends invoked as external tools (s3cmd, awscli, minio client) **MUST** be selected by absolute path and pass VTOP's startup identity check; operators **SHOULD** additionally pin/package the approved version because it executes outside the Rust dependency graph.

### Dependency auditing (`cargo audit`)

CI runs `cargo audit` (the `supply-chain` job) on every push and pull request. It
**fails the build on any advisory** except those explicitly documented in
[`.cargo/audit.toml`](../.cargo/audit.toml), so new or actionable vulnerabilities
block merges while known, unfixable transitive advisories are still printed.

Currently tracked (re-evaluate on every dependency bump):

| Advisory | Crate | Why it is accepted for now |
|----------|-------|----------------------------|
| RUSTSEC-2023-0071 | `rsa` | Pulled only by sqlx's optional MySQL driver, which is **not enabled** (sqlite-only). Not compiled or executed in any VTOP build. No upstream fix exists. |
| RUSTSEC-2026-0235 | `rkyv 0.7.46` | Optional `rust_decimal` feature pulled into `Cargo.lock` through `openraft → byte-unit`; the feature is not enabled and `rkyv` is absent from every build graph. Current `rust_decimal 1.42.1` still pins the vulnerable major; remove this exception when upstream adopts `rkyv >=0.8.17`. |
| RUSTSEC-2026-0098 / -0099 / -0104 | `rustls-webpki 0.101.x` | Transitive via `aws-smithy-http-client`'s legacy `hyper-rustls 0.24` connector. Not removable by feature flags in the current AWS SDK; requires an upstream release. The modern `rustls 0.23` / `rustls-webpki 0.103` stack is also present and used by the default HTTPS path. |
| RUSTSEC-2026-0253 | `lru 0.16.4` | An **unsoundness**, not a vulnerability: `LruCache::pop()` lacks panic safety, reachable only if a cached value's `Drop` panics during eviction. No fixed release exists — 0.16.4 is the newest published version. Reaches the build solely through `aws-sdk-s3`, whose cached values are SDK-internal types this project neither supplies nor controls. Remove when upstream publishes a fix, or re-evaluate on the next AWS SDK bump. |

When the AWS SDK ships an `aws-smithy-http-client` release that drops
`hyper-rustls 0.24`, the three `rustls-webpki` entries **MUST** be removed from
the ignore list and the build re-audited.

## 14. Security properties provided vs. not provided

| Property | Provided? | Notes |
|----------|-----------|-------|
| Object integrity (cryptographic) | Yes, with SHA-256/BLAKE3 | Stored object hash verified against manifest before commit. |
| Manifest corruption detection | Yes | Reproducible unkeyed self-hash. |
| Manifest authentication | Optional | Keyed BLAKE3 MAC; required when configured. |
| Chain of custody (object ↔ source markers) | Yes | Manifest binds object hash to covered markers. |
| Replay safety / no premature commit | Yes | Enforced in state machine, state store, and pipeline. |
| Transport confidentiality | Yes (native broker) / Configurable (Kafka, S3, PostgreSQL) | Every deployable configuration of the native broker is TLS 1.3 mTLS: the transport gained plaintext modes in #294 slices 3 and 4 (replica and native planes), but no node config key, `vtopctl` flag or Helm value reaches them, so plaintext is not selectable by any deployment method today. A plaintext replica plane also refuses the verbs whose authorization depends on a client certificate, and a plaintext native plane admits only principals the authorizer accepts unverified by name. Kafka/S3/PostgreSQL confidentiality is configured on those clients, not implemented in core. |
| PostgreSQL transport authentication | Yes for remote hosts | Non-loopback connections require `sslmode=verify-full`; loopback/socket plaintext is limited to local operation. |
| Backend-limited verification disclosure | Yes | Size-only mode is labeled and rejected by default. |
| Data-at-rest encryption | Not by VTOP | Delegated to storage layer (SSE/bucket default). |
| Object immutability (WORM) | Partial | Bucket versioning is validated and manifest versions pinned under the hardened profile (§8.1); object-lock retention itself is deployment policy, not engine-configured. |
| Public-key manifest signing / MAC rotation | Not yet | One shared MAC key is supported. |
| Multipart upload integrity for very large objects | Not yet | Native backend uses single-part `put_object`. |
| Authorization / multi-tenant isolation | Not by VTOP | Relies on storage-side IAM and least-privilege credentials. |

## 15. Summary of normative rules

| Rule | Level |
|------|-------|
| Credentials stored in manifests | **MUST NOT** |
| Credentials printed in logs | **MUST NOT** |
| Credentials via env vars / mounted secrets / external secret managers | **SHOULD** |
| TLS for Kafka and S3-compatible endpoints | **SHOULD** |
| Hostname-verified TLS for remote PostgreSQL | **MUST** |
| PostgreSQL URL supplied through an env/file reference | **MUST** |
| Least-privilege object storage permissions | **SHOULD** |
| `CreateBucket` scoped/avoided in runtime identity (per-format auto-create) | **SHOULD** |
| Verify object + manifest before commit | **MUST** |
| Report backend-limited verification as such (not cryptographic) | **MUST** |
| Configured manifest MAC verifies without downgrade | **MUST** |
| Pinned manifest version unreadable → fail closed (`FAILED` → `REPLAY_REQUIRED`) | **MUST** |
| Hardened profile: versioned bucket preflight + version on every manifest upload | **MUST** (when `require_object_versioning`) |
| Object Lock retention on versioned manifest buckets | **SHOULD** |
| Object lock / immutability | **SHOULD** (later) |
| Secret redaction in logs | **MUST** |
| Native broker transport restricted to TLS 1.3 with client certificates | **MUST** (no deployment method can select the plaintext transport; see §2) |
| Native broker sessions authorized by an explicit `SessionAuthorizer` | **MUST** |
| Native clients validate the broker certificate and expected server identity | **MUST** |
| Native produce bound to the authenticated principal (`producer_id == principal_id`) | **MUST** |

---

## 16. Cryptographic primitive inventory

The prerequisite for any FIPS conversation (#296): every cryptographic
primitive the workspace runs, where it runs, and whether it performs a
**security function** (defends against an adversary; would need an approved
algorithm under FIPS) or a **non-security integrity check** (detects
corruption by a trusted writer; FIPS does not constrain it). The
classification column is a **proposal** — merging this table ratifies the
uncontested rows, and the questions the code cannot answer by itself are
flagged and collected below the tables.

### 16.1 First-party primitives

| Primitive | Where it runs | What it protects | Proposed class |
|---|---|---|---|
| BLAKE3 keyed MAC (`manifest_mac_key_env`) | manifest build/verify (`vtop-core/manifest.rs`) | manifest authenticity against an adversary with object-store write access; constant-time compare, fail-closed when keyed | **security function** |
| BLAKE3 keyed MAC (segment commit key) | v2 commit statement (`vtop-log/types.rs`, seal/verify/tier) | signed assertion of a sealed segment's identity, boundaries, root and manifest digest | **security function** |
| BLAKE3 unkeyed content commitments: v2 chunk-tree root + Merkle proofs, v1 `blake3_root`, `manifest_core_digest`, tier-copy whole-file digest gate | seal, offline verify, `vtopctl tier copy/rehydrate`, Raft-pinned `CommitTierEvidence` | tamper-evidence of sealed content **when the root is pinned somewhere the adversary cannot rewrite** (Raft metadata, operator `--expect-root`, cursors); locally, corruption detection | **flagged — Q1 below** |
| SHA-256 / BLAKE3 object checksum (`checksum.algorithm`, default sha256) | archive-plane object write + read-back verify before commit | stored-object corruption; unkeyed, so substitution is the MAC's job | integrity check |
| SHA-256 manifest self-hash (fixed, not configurable) | every manifest | manifest corruption/truncation; the doc-stated "reproducible corruption-detection record" | integrity check |
| Ledger cross-check (`object_sha256` / `manifest_sha256` columns vs storage) | recovery re-check, `vtopctl` deep verify | rejects a coherently-replaced object+manifest pair that is internally self-consistent — a two-root-of-trust binding | **flagged — Q1 below** |
| BLAKE3 unkeyed 32-byte trailers | segment headers/frames, `.producers`, `.chunks`, commit-boundary, truncate/retention intent markers, Raft hard state/log/snapshots, VTPM + native wire frames, epoch journal | torn writes, bit rot, frame desync — written and re-read by the same trusted process; wire planes run under TLS, which owns authenticity | integrity check |
| CRC-32C (hand-rolled, vector-pinned) | `vtop-kafka` RecordBatch v2 | Kafka wire-format corruption; the code itself documents it as forgeable | integrity check |
| BLAKE3 as a stable PRF (derive-key): rendezvous placement, multipart session name, tier-copy request id, `storage_producer_id`; SHA-256 lock-file name | placement, upload resume, idempotency keys, single-instance lock | nothing adversarial — deterministic derivation and naming; any stable hash would do | **neither** (not a check, not a control) |
| Idempotent-producer content hash | duplicate-vs-conflict decision on retry | content-equality fingerprint; producer controls both sides | integrity check (accidental-collision resistance only) |
| UUID v4 (OS CSPRNG via `getrandom`) | request ids, batch ids, temp names, lineage ids, session nonce | uniqueness, never secrecy: fencing is monotonic epochs, principals are configured; the native `session_nonce` is generated and **not yet consumed** (reserved binding point) | **neither** |

### 16.2 Delegated primitives (an external library chooses and implements)

| Family | Stack | Function | Class |
|---|---|---|---|
| Cluster-plane TLS (admin, Raft peer, replica, native client) | rustls 0.23, **ring pinned at every construction site**, TLS 1.3 only, mutual; CN identity via `x509-parser` after chain validation on the admin, Raft-peer and replica planes — the native-client plane never parses the subject (Q3 below) | peer authentication, confidentiality; authorization input where the CN is read (CN is a Raft **safety** input on the peer plane) — native sessions match the declared hello principal against config, not the certificate | security function |
| Kafka source transport | librdkafka (vendored C, cmake) + **system OpenSSL** (`ssl`) + **system Cyrus SASL** (`sasl`) | broker TLS + SASL auth; plaintext unless configured (SHOULD-level, §2); missing password env is a hard startup error, never a silent downgrade | security function |
| S3 upload transport + signing | AWS SDK: hyper 1 + hyper-rustls + rustls 0.23 with the SDK's **aws-lc-rs** provider, system roots; SigV4 HMAC-SHA-256 (SigV4a/P-256 available, never explicitly invoked) | endpoint TLS (scheme floor via `verify_tls`; cert verification not disableable) and per-request authentication | security function |
| Postgres state store (`--features postgres`) | sqlx with `tls-rustls-ring-webpki` (the one place the workspace picks the stack); SCRAM-SHA-256 auth with ChaCha CSPRNG nonces — but the **server** chooses the method: sqlx also answers legacy `md5` (and cleartext) password requests, with no client-side way to refuse | verify-full floor for remote hosts (§2, MUST); database authentication | security function |
| Release supply chain | cosign keyless (Sigstore), SPDX SBOM, provenance, SHA256SUMS | image + artifact authenticity; SHA256SUMS alone is integrity only, and the release notes say so | security function |
| Test-only material | `rcgen` (dev-deps), ECDSA P-256 + SHA-256 cert minting in `gen-certs.sh` / `k8s-smoke.sh`, PEM key loading (`TlsMaterial::from_pem_files`) as the production ingestion boundary | throwaway harness PKI; production operators bring their own PEM | security function (test scope) |

Two rustls crypto providers coexist in the tree by design: **ring** (pinned
explicitly at every cluster-plane construction site, precisely because
feature unification pulls aws-lc-rs into the lockfile) and the AWS SDK's
own default for S3. First-party code contains **no HMAC, no SHA-1, no MD5**;
those appear only inside the delegated stacks above. Of the delegated
stacks, only the Postgres client will actually execute MD5 as an
authentication primitive, and only when the server requests it. Closing
that path is a server-side, two-part job: restrict the relevant
`pg_hba.conf` rules to `scram-sha-256` (the HBA method chooses the
exchange) and rotate any password set before the switch to
`password_encryption = scram-sha-256` — that setting governs only how
NEW passwords are stored, so a legacy MD5 verifier keeps the legacy
exchange alive until its password is reset. A FIPS-shaped deployment
does both, and only then can the MD5 path never be offered. At-rest
encryption is delegated to the storage layer (§7).

### 16.3 The questions this inventory exists to ask

**Q1 — the substantive one (#296 calls it that):** are the *unkeyed* BLAKE3
content commitments a security function? Locally each is a corruption
check, but the offline verifier's stated threat model is adversarial, and
the tier flow pins `content_root` / `manifest_core_digest` into Raft
metadata precisely so an untrusted object store cannot substitute content —
there, collision/second-preimage resistance is load-bearing. The same
question covers the ledger cross-check (an unkeyed two-root-of-trust
binding that defeats coherent substitution). If ratified as a security
function, BLAKE3 itself is inside the FIPS boundary and the v2 format
grows a pluggable-digest question; if ratified as defense-in-depth
integrity, the FIPS story needs only the keyed MACs addressed.

**Q2 — the FIPS consequence, either way:** a FIPS-shaped deployment can
already select `checksum.algorithm: sha256` (the default) and leave the
MAC keys unset — but then manifest **authentication does not exist**, and
the only authenticated-content mechanisms in the workspace (both MACs, the
chunk tree) are BLAKE3-only. There is no FIPS-approved authenticated path
today; that is the gap the later #296 checkboxes exist to close, not a
property this inventory can document around.

**Q3 — housekeeping the sweep surfaced:** the produce/fetch
`PrincipalAuthorizer` ignores the verified certificate chain (the declared
principal is matched against config, not bound to the cert — unlike the
meta and replica planes, where the leaf CN *is* the identity); and only one
CI container is digest-pinned (`actionlint`) while the shellcheck images
are tag-pinned. Both are posture notes, not inventory rows.

# Security Model

> Security model for the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.

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
9. [Audit and failure logging](#9-audit-and-failure-logging)
10. [Secret redaction](#10-secret-redaction)
11. [Container and runtime hardening](#11-container-and-runtime-hardening)
12. [Supply-chain security](#12-supply-chain-security)
13. [Security properties provided vs. not provided](#13-security-properties-provided-vs-not-provided)
14. [Summary of normative rules](#14-summary-of-normative-rules)

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
| Build ↔ runtime | Supply-chain auditing; container hardening. |

---

## 2. Transport Security

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

## 3. Credential Handling

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

## 4. Manifest Confidentiality

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

### 5.2 On-demand bucket creation (`CreateBucket`) implications

Per-format buckets (e.g. `telemetry-{format}`) with optional on-demand creation require `CreateBucket` (and possibly bucket-policy) permissions. Granting `CreateBucket`:

- **SHOULD** be scoped to a dedicated provisioning identity, or buckets **SHOULD** be pre-created so the runtime identity does **not** hold `CreateBucket`.
- **MUST NOT** be combined with broad delete permissions on the same identity without an explicit lifecycle justification.
- Broadens blast radius (an over-broad identity could create unexpected buckets); operators **SHOULD** prefer pre-provisioned buckets in production.

## 6. Integrity verification and chain of custody

- The engine **MUST** compute a content checksum (SHA-256 or BLAKE3; or size-only when checksums are explicitly disabled) over the compressed telemetry object.
- The engine **MUST** verify the durably stored object against the manifest before transitioning to `VERIFIED`.
- The engine **MUST** verify the stored manifest before committing source progress.
- Strong verification **MUST** be derived from stored content or a checksum the storage service computed over that content. Engine-written sidecars, ETags, and user metadata **MUST NOT** be classified as strong.
- A source progress marker **MUST NOT** be committed unless both object and manifest verification succeed (see the protocol commit rule).
- The manifest binds the object hash to the covered source progress markers. Its unkeyed **self-hash** is reproducible corruption detection, not authenticity: a writer who can replace the document can recompute it.
- When `manifest_mac_key_env` is configured, the stored manifest **MUST** carry a valid keyed BLAKE3 `manifest.mac`; missing or invalid MACs fail pipeline, CLI, and recovery verification.
- Where only size/existence can be confirmed, verification is **backend-limited** and the engine **MUST** report it as such rather than as cryptographic verification. The engine defaults to rejecting this result; accepting it requires the explicit `require_strong_verification: false` compatibility opt-out.
- A keyed MAC authenticates data among key holders but does not provide public verification or non-repudiation.

## 7. Data at rest and object immutability

- **Object lock SHOULD be supported later.**
- Where the backend supports it (e.g., S3 Object Lock / WORM), telemetry objects and manifests **SHOULD** be written as immutable for the configured retention period.
- Immutability complements verification: verification detects tampering, immutability prevents post-write tampering or accidental overwrite.
- At-rest encryption (server-side or bucket-default) **MAY** be enabled at the storage layer; it is orthogonal to VTOP's integrity guarantees.

## 8. Manifest Authentication

- VTOP 0.2 supports an optional keyed BLAKE3 authenticator in `manifest.mac`.
- Config stores only the environment-variable name (`manifest_mac_key_env`); the 32-byte hex key **MUST NOT** appear in config serialization, manifests, or logs.
- Naming an absent or malformed key **MUST** fail startup rather than silently emit unsigned manifests.
- Enabling a key deliberately rejects unsigned pre-cutover manifests. Operators **MUST** verify or explicitly migrate their backlog before enabling it.
- One active key is supported. Rotation and public-key signatures are not implemented; object versioning/lock remains necessary to resist deletion and rollback to an older valid manifest.

## 9. Audit and Failure Logging

- The engine **SHOULD** emit structured audit logs for batch lifecycle events (seal, upload, verify, commit) including `batch_id`, object key, and outcome.
- Failures **SHOULD** be logged with enough context to support replay and forensic review, **without** including secrets or raw sensitive payloads.
- Audit logs **SHOULD** be append-oriented and suitable for retention alongside the archived objects.

## 10. Secret Redaction

- Any log path, error type, or diagnostic that could surface credentials **MUST** redact them.
- Connection strings, headers, and configuration dumps **MUST** have secret fields masked before logging.
- PostgreSQL parse/connect errors **MUST NOT** echo the supplied URL. VTOP connects from parsed options and applies URL redaction at the state-store error boundary as defense in depth.
- The redaction layer **SHOULD** default to redacting unknown sensitive-looking fields rather than printing them.

## 11. Container and Runtime Hardening

- Container images **SHOULD** run as a non-root user.
- Images **SHOULD** use minimal/distroless-style bases to reduce attack surface.
- Filesystems **SHOULD** be mounted read-only except for required working/state directories (e.g. the SQLite state store and any LocalFS object tree).
- Linux capabilities **SHOULD** be dropped to the minimum required.
- Secrets **SHOULD** be provided via mounted secrets or the orchestrator's secret store, never baked into images.

## 12. Supply-Chain Security

- Dependencies **SHOULD** be pinned and audited (e.g., dependency vulnerability scanning).
- Builds **SHOULD** be reproducible where practical, and release artifacts **SHOULD** be checksummed and **MAY** be signed.
- A software bill of materials (SBOM) **SHOULD** be produced for releases.
- Third-party upload backends invoked as external tools (s3cmd, awscli, minio client) **SHOULD** be version-pinned and validated, since they execute outside the Rust dependency graph.

### Dependency auditing (`cargo audit`)

CI runs `cargo audit` (the `supply-chain` job) on every push and pull request. It
**fails the build on any advisory** except those explicitly documented in
[`.cargo/audit.toml`](../.cargo/audit.toml), so new or actionable vulnerabilities
block merges while known, unfixable transitive advisories are still printed.

Currently tracked (re-evaluate on every dependency bump):

| Advisory | Crate | Why it is accepted for now |
|----------|-------|----------------------------|
| RUSTSEC-2023-0071 | `rsa` | Pulled only by sqlx's optional MySQL driver, which is **not enabled** (sqlite-only). Not compiled or executed in any VTOP build. No upstream fix exists. |
| RUSTSEC-2026-0098 / -0099 / -0104 | `rustls-webpki 0.101.x` | Transitive via `aws-smithy-http-client`'s legacy `hyper-rustls 0.24` connector. Not removable by feature flags in the current AWS SDK; requires an upstream release. The modern `rustls 0.23` / `rustls-webpki 0.103` stack is also present and used by the default HTTPS path. |

When the AWS SDK ships an `aws-smithy-http-client` release that drops
`hyper-rustls 0.24`, the three `rustls-webpki` entries **MUST** be removed from
the ignore list and the build re-audited.

## 13. Security properties provided vs. not provided

| Property | Provided? | Notes |
|----------|-----------|-------|
| Object integrity (cryptographic) | Yes, with SHA-256/BLAKE3 | Stored object hash verified against manifest before commit. |
| Manifest corruption detection | Yes | Reproducible unkeyed self-hash. |
| Manifest authentication | Optional | Keyed BLAKE3 MAC; required when configured. |
| Chain of custody (object ↔ source markers) | Yes | Manifest binds object hash to covered markers. |
| Replay safety / no premature commit | Yes | Enforced in state machine, state store, and pipeline. |
| Transport confidentiality | Configurable | Via TLS/SASL/mTLS; not implemented in core. |
| PostgreSQL transport authentication | Yes for remote hosts | Non-loopback connections require `sslmode=verify-full`; loopback/socket plaintext is limited to local operation. |
| Backend-limited verification disclosure | Yes | Size-only mode is labeled and rejected by default. |
| Data-at-rest encryption | Not by VTOP | Delegated to storage layer (SSE/bucket default). |
| Object immutability (WORM) | Not yet | Designed; relies on backend object lock (future). |
| Public-key manifest signing / MAC rotation | Not yet | One shared MAC key is supported. |
| Multipart upload integrity for very large objects | Not yet | Native backend uses single-part `put_object`. |
| Authorization / multi-tenant isolation | Not by VTOP | Relies on storage-side IAM and least-privilege credentials. |

## 14. Summary of Normative Rules

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
| Object lock / immutability | **SHOULD** (later) |
| Secret redaction in logs | **MUST** |

# Security Model

> Security model for the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are used as normative requirements for conformant behavior.

## 1. Transport Security

### TLS for Kafka

- Connections to Kafka brokers **SHOULD** use TLS.
- Certificate validation **SHOULD** be enabled; disabling validation **MUST** be an explicit, logged, non-default configuration.

### TLS for S3-compatible endpoints

- Connections to S3-compatible object storage endpoints **SHOULD** use TLS (HTTPS).
- Custom CA bundles **MAY** be supplied for private/self-hosted endpoints.

### Authentication mechanisms

- Kafka authentication **SHOULD** support **SASL/SCRAM** and **mTLS**.
- The selected mechanism and identity **MAY** be logged, but associated secrets **MUST NOT** be logged.

## 2. Credential Handling

Normative rules:

- **Credentials MUST NOT be stored in manifests.**
- **Credentials MUST NOT be printed in logs.**
- **Credentials SHOULD be supplied through environment variables, mounted secrets, or external secret managers.**

Additional guidance:

- Configuration files containing secrets **SHOULD** have restrictive filesystem permissions.
- Credentials **SHOULD NOT** be passed as plaintext command-line arguments where avoidable, since arguments may be visible to other processes.
- The engine **SHOULD** support loading credentials from external secret managers without persisting them to disk.

## 3. Manifest Confidentiality

- Manifests describe object integrity and source progress only.
- Manifests **MUST NOT** contain credentials, tokens, or other authentication material.
- Manifests **SHOULD NOT** contain raw telemetry payload contents beyond the integrity metadata necessary for verification.

## 4. Object Storage Permissions

- Object storage credentials **SHOULD** follow **least privilege**: the minimum permissions required to put, get/head (for verification), and list within the configured prefix.
- Delete permissions **SHOULD NOT** be granted unless an operational lifecycle policy explicitly requires them.
- Separate read-only credentials **SHOULD** be used for downstream consumers.

## 5. Integrity Verification

- The engine **MUST** compute a SHA-256 checksum over the compressed telemetry object.
- The engine **MUST** verify the durably stored object against the manifest checksum before transitioning to `VERIFIED`.
- The engine **MUST** verify the stored manifest before committing source progress.
- A source progress marker **MUST NOT** be committed unless both object and manifest verification succeed (see the protocol commit rule).

## 6. Manifest Signing (Future)

- **Manifest signing SHOULD be supported later.**
- When enabled, manifests **MAY** carry a detached or embedded signature to strengthen chain-of-custody and tamper-evidence guarantees.
- Signing keys, when introduced, **MUST** be handled under the same credential rules in §2 and **MUST NOT** appear in manifests or logs.

## 7. Object Immutability

- **Object lock SHOULD be supported later.**
- Where the backend supports it (e.g., S3 Object Lock / WORM), telemetry objects and manifests **SHOULD** be written as immutable for the configured retention period.
- Immutability complements verification: it protects stored objects from post-write tampering or accidental overwrite.

## 8. Audit and Failure Logging

- The engine **SHOULD** emit structured audit logs for batch lifecycle events (seal, upload, verify, commit) including `batch_id`, object key, and outcome.
- Failures **SHOULD** be logged with enough context to support replay and forensic review, **without** including secrets or raw sensitive payloads.
- Audit logs **SHOULD** be append-oriented and suitable for retention alongside the archived objects.

## 9. Secret Redaction

- Any log path, error type, or diagnostic that could surface credentials **MUST** redact them.
- Connection strings, headers, and configuration dumps **MUST** have secret fields masked before logging.
- The redaction layer **SHOULD** default to redacting unknown sensitive-looking fields rather than printing them.

## 10. Container and Runtime Hardening

- Container images **SHOULD** run as a non-root user.
- Images **SHOULD** use minimal/distroless-style bases to reduce attack surface.
- Filesystems **SHOULD** be mounted read-only except for required working/state directories.
- Linux capabilities **SHOULD** be dropped to the minimum required.
- Secrets **SHOULD** be provided via mounted secrets or the orchestrator's secret store, never baked into images.

## 11. Supply-Chain Security

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

## 12. Summary of Normative Rules

| Rule | Level |
|------|-------|
| Credentials stored in manifests | **MUST NOT** |
| Credentials printed in logs | **MUST NOT** |
| Credentials via env vars / mounted secrets / external secret managers | **SHOULD** |
| TLS for Kafka and S3-compatible endpoints | **SHOULD** |
| Least-privilege object storage permissions | **SHOULD** |
| Verify object + manifest before commit | **MUST** |
| Manifest signing | **SHOULD** (later) |
| Object lock / immutability | **SHOULD** (later) |
| Secret redaction in logs | **MUST** |

# Security

What the engine protects, and what it leaves to the systems around it. The
normative rules are in the full [security model](../SECURITY_MODEL.md).

## Secrets stay out of config files

The config names secrets; it never holds them. Object storage credentials
come from the AWS SDK's standard sources, a PostgreSQL URL from an
environment variable or a mounted file, the Kafka SASL password from the
variable `sasl_password_env` names, and the manifest MAC key from the
variable `manifest_mac_key_env` names. An inline `postgres://` URL is refused.
Credentials are never written to manifests or logs.

Where it is configured: [Config basics](../configure/basics.md#secrets).

## Transport

- **Object storage:** `upload.verify_tls` is on by default, and a plaintext
  `http://` endpoint is then refused. Turning it off is a lab opt-out.
- **PostgreSQL:** every connection that is not loopback or a Unix socket must
  use `sslmode=verify-full`.
- **Kafka:** set with `security_protocol` and the SASL and CA keys.
- **Cluster plane:** mutual TLS 1.3 on every plane unless a plane is switched
  to plaintext by name. The Helm chart renders TLS unless told twice.
- **Metrics endpoint:** unauthenticated by design. Bind it to loopback or a
  management network, or give it TLS.

## Integrity

- Each object's checksum (SHA-256 or BLAKE3) is checked against the stored
  copy before the source is committed. Size-only checking is an explicit
  opt-out that the engine announces at startup.
- Each manifest carries a self-hash, and optionally a keyed BLAKE3 MAC
  (`manifest_mac_key_env`) so a party who can write to the bucket cannot
  forge one.
- With `upload.require_object_versioning`, manifests are pinned by version,
  so overwriting a manifest cannot roll a batch back.
- `vtopctl scrub` re-verifies committed objects later.

Operated in [Scrub and coverage audit](../operate/scrub-and-audit.md).

## Least privilege

- **Object storage:** put, get and head, and list within the prefix. No
  delete unless a lifecycle policy needs it. `CreateBucket` only if you turn
  on `upload.create_bucket`.
- **PostgreSQL:** a migration identity owns the schema and runs
  `vtopctl migrate`; the engine's identity gets `SELECT, INSERT, UPDATE` on
  the ledger and nothing more. Pruning runs under a third, maintenance
  identity.
- **External upload clients** (`awscli`, `s3cmd`, `minio`) run from an
  absolute path, with an empty environment except the variables you list.

## Not provided by VTOP

Encryption at rest (use the store's), object lock retention (bucket policy),
access control between tenants (storage IAM), and manifest MAC key rotation
(not implemented). See the
[security model §14](../SECURITY_MODEL.md#14-security-properties-provided-vs-not-provided).

## Releases

Release binaries and the container image are signed with keyless Sigstore and
ship an SBOM. [Verify a release](../RELEASE_VERIFICATION.md) before you run
it.

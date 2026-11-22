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
  (`manifest_mac_key_env`, or a keyring in `manifest.mac`) so a party who can
  write to the bucket cannot forge one. With a keyring, a version 0.4
  manifest names the key that signed it, so the key can be
  [rotated](../operate/manifest-key-rotation.md) without re-signing the
  archive.
- With `upload.require_object_versioning`, manifests are pinned by version,
  so overwriting a manifest cannot roll a batch back.
- `vtopctl scrub` re-verifies committed objects later.

Operated in [Scrub and coverage audit](../operate/scrub-and-audit.md).

## Encryption at rest

With `upload.server_side_encryption` (`s3_native` only), every object,
manifest and verification receipt is written with S3 server-side encryption,
`sse-s3` (AES256) or `sse-kms`, single-part and multipart. The store encrypts;
VTOP asks on every write and then checks the store's `HeadObject`: an object
reported as unencrypted, as another algorithm, or (with a named `kms_key_id`)
as another key fails verification like a checksum mismatch, so a write that
should be encrypted and is not is never committed. `sse-kms` with no
`kms_key_id` sends no key and uses the store's default KMS key (on AWS, the
account's AWS-managed `aws/s3` key). SSE-C is not supported. Keys, key policy
and rotation stay with the store and KMS. Unset, VTOP asks for nothing and
checks nothing. The startup summary and `vtop_server_side_encryption_mode`
show the mode. Details and the permissions the identity needs:
[security model §7.1](../SECURITY_MODEL.md#71-encryption-at-rest-uploadserver_side_encryption).

## Least privilege

- **Object storage:** put, get and head, and list within the prefix. No
  delete unless a lifecycle policy needs it. `CreateBucket` only if you turn
  on `upload.create_bucket`. With `sse-kms`, `kms:GenerateDataKey` and
  `kms:Decrypt` on the key.
- **PostgreSQL:** a migration identity owns the schema and runs
  `vtopctl migrate`; the engine's identity gets `SELECT, INSERT, UPDATE` on
  the ledger and nothing more. Pruning runs under a third, maintenance
  identity.
- **External upload clients** (`awscli`, `s3cmd`, `minio`) run from an
  absolute path, with an empty environment except the variables you list.

## Not provided by VTOP

Encryption of the state ledger and work directory (use disk or volume encryption),
keys for the store's encryption (the object store's own and KMS's), creating a bucket with Object Lock and
its default retention (the engine writes each object under a lock with
`upload.object_lock`, and checks it, but the bucket is yours; see
[Object Lock](../OBJECT_LOCK_DEPLOYMENT.md)), access control between tenants (storage IAM), and public-key manifest
signatures (not implemented: a MAC key is a shared secret). See the
[security model §14](../SECURITY_MODEL.md#14-security-properties-provided-vs-not-provided).

## Releases

Release binaries and the container image are signed with keyless Sigstore and
ship an SBOM. [Verify a release](../RELEASE_VERIFICATION.md) before you run
it.

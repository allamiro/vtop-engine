# Security Model

> Security model for the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.
>
> Protocol-level behavior referenced here (state machine, commit rule §13, replay rule §14, verification semantics §17, manifest spec §11) is normatively defined in [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md); this document adds the security rules around it.

To report a vulnerability, and for response times and supported versions, see
the [security policy](../SECURITY.md).

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
17. [Threat map](#17-threat-map)

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
| Engine/node ↔ OTLP collector (#683) | Off unless an OTLP endpoint is set. Spans carry identifiers, counts, sizes, method names, outcomes and redacted error messages; metrics are the bounded `/metrics` series. No record content, credentials, keys, MACs, URLs with a path or query, or header values (§2). |
| Native client ↔ broker | TLS 1.3 with mandatory client certificates, explicit certificate/principal/role authorization, bounded frames/sessions/windows, and range/producer fencing checks. |
| Build ↔ runtime | Supply-chain auditing; container hardening. |

Every boundary above, and the cluster planes and the Kafka gateway the table only touches, has rows in [§17](#17-threat-map): each threat with its control, the test that proves it, and the gaps.

---

## 2. Transport security

### TLS for Kafka

- Connections to Kafka brokers **SHOULD** use TLS.
- When `sources.kafka.security_protocol` is unset (librdkafka's default is
  plaintext), `plaintext` or `sasl_plaintext`, and a bootstrap server is not a
  loopback address, the engine warns on every start and `vtopctl validate`
  reports it (#732): the records, and any SASL exchange, cross the network
  unencrypted and the broker is not authenticated. The engine still connects;
  set `security_protocol: ssl` or `sasl_ssl` with `ssl_ca_location`. A
  hostname that resolves to loopback is not recognised as loopback, so it is
  warned about.
- Certificate validation **SHOULD** be enabled; disabling validation **MUST** be an explicit, logged, non-default configuration.

### TLS for S3-compatible endpoints

- Connections to S3-compatible object storage endpoints **SHOULD** use TLS (HTTPS).
- Custom CA bundles **MAY** be supplied for private/self-hosted endpoints.

### TLS for PostgreSQL state stores

- PostgreSQL over a Unix socket or loopback address **MAY** use plaintext for local development and CI.
- Every non-loopback PostgreSQL connection **MUST** use `sslmode=verify-full`, which verifies both the issuing CA and the database hostname.
- A private database CA **MUST** be supplied with `sslrootcert` when it is not in the bundled public trust roots. `sslmode=require` and `verify-ca` are rejected for remote databases because they do not provide the required hostname verification.

### OpenTelemetry export (#683)

OpenTelemetry export is the one network egress the processes open for their
own telemetry rather than for the data. It is compiled into every `vtopctl`
and `vtop-node` binary and the image, and **off until configured**: it starts
only when `OTEL_EXPORTER_OTLP_ENDPOINT` or a per-signal endpoint
(`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `..._METRICS_ENDPOINT`) is set. Unset,
no exporter, thread or task exists (`crates/vtop-cli/tests/otel_off.rs`), and
startup logs one INFO line saying so. `OTEL_SDK_DISABLED=true` turns it off
with an endpoint set.

- **Destination and transport.** OTLP over HTTP with protobuf bodies, `POST`
  to the configured endpoints (`/v1/traces`, `/v1/metrics`). No gRPC, no
  proxy, no compression. `https://` uses rustls with the `ring` provider and
  the system's root certificates (`SSL_CERT_FILE` / `SSL_CERT_DIR` select
  others); certificate verification cannot be turned off. An off-host
  collector **SHOULD** be reached over `https://`; plain `http://` is for a
  collector on the same host or pod network.
- **What leaves.**
  - Traces: spans of the processes' own targets (`vtop_*`) only, never a
    library's, and never a log event. A batch span carries the batch id,
    source type and name, tenant, format, record and byte counts, checksum
    algorithm, verification method and result, the step that failed and the
    outcome. Recovery and scrub spans carry the same identity plus their
    decision.
  - A failed batch's span status carries the error message, **redacted**
    first (below).
  - Metrics: the same series `/metrics` serves, whose labels are a bounded set
    a test enforces (no batch id, no URI).
  - Both signals carry the resource attributes `service.name`,
    `service.version`, `service.instance.id` (the engine's name), `host.name`,
    and any the operator adds through `OTEL_RESOURCE_ATTRIBUTES`.
- **What never leaves, and is never logged.** Record content, credentials,
  keys, MAC values, object and manifest URIs, URLs with a path or query string
  (a presigned URL carries its signature there), and the values of the
  `OTEL_EXPORTER_OTLP_*HEADERS` variables. The one URL that is printed is the
  collector endpoint, as host and path (never user-info or query string), in
  the startup line and `validate --print-effective`: a collector token
  **MUST** go in a header, not in the endpoint's path or query.
  - Before an error message becomes a span status or a log line, every URL in
    it is cut to `scheme://host`, every run of 32 or more hex digits (a MAC, a
    key, a digest) becomes `<hex>`, and every AWS access key id becomes
    `<key-id>` (`vtop_observe::otel::redact`).
  - The OTLP exporter's own debug logging, which quotes request URLs and
    collector responses, is compiled out.
  - Tests assert that no exported attribute holds record content or a `://`
    URL, that no log line or `validate --print-effective` output holds a
    header value, user-info or query string, and that the redaction above
    holds.
  - A source name is an identifier the operator configured (a file path or a
    topic): a deployment that treats those as sensitive **SHOULD NOT** turn
    export on.
- **Credentials.** A collector that needs authentication takes it from
  `OTEL_EXPORTER_OTLP_HEADERS` (or the per-signal variables). These **MUST** be
  supplied as a secret like the storage keys (§3). They are never logged:
  `validate --print-effective` prints "set (values not shown)", and a
  malformed one is reported by variable name only. Credentials **MUST NOT** be
  put in the endpoint URL itself, although the processes never print one
  beyond its host and path.
- **Failure isolation.**
  - A setting that cannot be honoured stops the process at startup, naming
    the variable.
  - After startup, export never stalls the data path. A span's end appends to
    a bounded queue (`OTEL_BSP_MAX_QUEUE_SIZE`, 2048) or drops the span when
    the queue is full. One thread sends. A collector that is down or slow
    costs spans and data points, which are counted on `/metrics` and warned
    about at most once a minute.
  - On a stop, the process flushes for 5 s after its own shutdown flush (6 s
    at the outside: a 1 s backstop over the queue's own bound), or not at all
    once another stop signal arrives, then exits whatever the collector is
    doing. Before that flush it waits up to 1 s for spans that are still open
    (a library's thread can hold a span for a moment after it has answered), so
    the span of the last batch is not lost; if one stays open the stop warns
    and goes on without it.

### Authentication mechanisms

- Kafka authentication **SHOULD** support **SASL/SCRAM** and **mTLS**.
- The selected mechanism and identity **MAY** be logged, but associated secrets **MUST NOT** be logged.

### Native broker transport

- The native broker transport is TLS 1.3 with a client certificate chained
  to the configured client roots BY DEFAULT, on every plane. Each plane's
  transport is a node config knob (#294 slice 5): `peer_transport` and
  `admin_transport` on a metadata node, `replica_transport` and
  `native_transport` on a data node, `lease.transport` for a data node's
  dial of the admin plane — `tls` unless spelled otherwise, `plaintext` to
  serve without TLS on a loopback address only (the listener refuses to bind
  anywhere else), or `plaintext-on-any-interface` to acknowledge the
  exposure by name. No `vtopctl` flag or Helm value sets them, so every
  chart-rendered configuration is still TLS. A plaintext replica plane
  refuses the verbs whose authorization depends on a client certificate; a
  plaintext native plane admits a session on the declared principal alone
  (the node's authorizer accepts the configured `principal_id` unverified —
  anything that can reach the port and knows that UUID may produce and
  consume); a plaintext admin plane refuses an enforcing
  `admin_authorization`, since there is no CN to match.
- The Helm chart renders `admin_authorization` by default (#732): only the
  certificate CNs in `meta.adminAuthorization.operatorCommonNames` may run
  cluster-scoped admin commands, and the chart refuses to render with the
  policy on and no operator named rather than fall back to an open admin
  plane. `meta.adminAuthorization.enabled: false` is the lab opt-out, warned
  about in the install notes; a hand-written node config without the block is
  still authenticate-only and warns at startup.
- Certificate validity alone does not grant a role. The embedding deployment
  **MUST** supply a `SessionAuthorizer` that binds the peer certificate chain,
  declared principal ID, and requested producer/consumer role. Produce requests
  additionally require `producer_id == principal_id` before the durable producer
  epoch journal can be changed.
- The data node's authorizer binds the certificate to the principal: a session
  over TLS is admitted only when the chain-verified leaf certificate's Common
  Name is the node's configured `principal_id` (the same field the metadata,
  replica and admin planes read as an identity), the declared principal is
  that `principal_id`, and the role is producer or consumer. A certificate that
  chains to the CA but was issued to another identity is refused and leaves a
  `native.session` audit record naming the CN it presented. A client
  certificate whose CN is not the principal's UUID is refused; there is no
  setting that restores the earlier behaviour of accepting any CA-signed
  certificate that names the UUID.
- Native clients **MUST** validate the broker certificate against their
  configured roots and the expected server identity (no insecure or
  accept-any verifiers). TLS peer authentication is mutual: server-side client
  verification alone does not protect a client from connecting to an imposter
  broker. One scoped exception, and it is a lab tool rather than a client
  deployment: the `vtop-node produce`/`verify` scenario client treats an
  absent `tls` block in its config as a PLAINTEXT session (#294 slice 4) —
  no certificate on either side, so no server authentication and no
  confidentiality — and warns on every such connection. It exists so a
  loopback lab can exercise a plaintext native plane end to end; nothing
  a deployment ships uses it, and a node refuses to serve plaintext off
  loopback unless told so by name.
- Wire frames are length-bounded and BLAKE3-checksummed independently of TLS.
  The checksum detects accidental framing corruption; TLS provides peer
  authentication, confidentiality, and active-tamper protection.
- Session count, global in-flight work, negotiated record/frame limits, idle
  timeouts, and fetch-response byte credit are explicit resource boundaries.
- Every cluster listener caps its concurrent connections (#732). The native
  plane caps sessions; the metadata peer and admin listeners and the replica
  listener cap connections with `max_peer_connections` (default 256),
  `max_admin_connections` (default 1024) and `max_replica_connections`
  (default 256) in the node config. A connection over the cap is closed at
  accept, before its TLS handshake, so a client holding a CA-signed
  certificate (or anything that reaches a plaintext plane) cannot open
  connections until the node runs out of descriptors or memory. A connection
  holds its slot from accept, so it has 10 s from accept to its first request
  (dialect check, TLS handshake and first frame); one that misses that is
  closed, and a client with no certificate cannot keep slots by opening
  sockets and sending nothing. Each refusal is counted in
  `vtop_listener_connections_refused_total{plane,reason}` (`capacity` or
  `handshake_timeout`) and named in a rate-limited WARN. A cap bounds how many
  connections there are, not who holds them: a flood can still take every
  slot and keep legitimate peers out while it lasts, and an authenticated
  client that sends its first request may hold its slot while it stays
  connected. The counter and the WARN make both visible.
- The observability endpoint (`/metrics`, `/healthz`, `/readyz`) **MAY** be
  served over TLS 1.3 (#294 slice 4): `observability.tls` in a node config, or
  `VTOP_METRICS_TLS_CERT`/`_KEY` for the engine. Server-only by default so
  probes and scrapers keep working; a client CA makes it mutual, and then only
  a scraper the CA vouches for is served. Misconfigured TLS disables the
  endpoint rather than serving it plaintext, and a plaintext probe against a
  TLS endpoint is closed at the handshake.
- Cross-mode is refused by name on every plane (#294 slice 6). A plane's
  transport is fixed by configuration before any socket exists; on accept,
  the plane peeks at the first byte — a TLS handshake record, or the `V`
  every vtop frame opens with — and a peer speaking the other transport is
  closed with a warning that names both sides: what the peer spoke, the
  plane's knob, and the two ways to make them agree. Nothing is negotiated,
  so the check cannot be used to downgrade. A TLS client whose handshake was
  closed unanswered adds the same hint to its own error.


### The egress transport seam (#479, reviewed under #488)

The archive upload path selects its wire through a registered transport
(`upload.transport`, `crates/vtop-upload/src/transport.rs`). Exactly one
transport ships today.

| Transport | What it is | Schemes it admits | Status |
|---|---|---|---|
| `tcp_tls` (default) | kernel TCP inside TLS through the AWS SDK | `http`, `https` | shipping |

A datagram transport is *planned* behind the gate the egress epic defines. No
QUIC or HTTP/3 implementation ships in this tree today, and no statement may
imply otherwise.

**The scheme policy is transport-aware, and applied at every door — but the
doors do not all open at the same time.** An endpoint can reach the client from
five places:

1. the explicit `upload.endpoint_url`;
2. `AWS_ENDPOINT_URL_S3`;
3. `AWS_ENDPOINT_URL`;
4. whatever the SDK resolved and exposes as `SdkConfig::endpoint_url()`,
   including a profile-wide `endpoint_url` from its shared config file;
5. an S3-SPECIFIC `endpoint_url` in a shared-profile `[services]` section,
   which `SdkConfig::endpoint_url()` does not expose at all, because the
   service config is applied after the shared config is read.

Sources 1–4 are checked by one function before any client is built, so an
endpoint from any of them that the policy refuses fails `S3NativeBackend::new`
— which is where an operator wants to hear about a typo.

**Source 5 is not checked there, and cannot be**: nothing in the resolved
configuration reveals it. `S3NativeBackend::new` therefore SUCCEEDS for a
plaintext S3-specific profile endpoint, and the refusal arrives later — on the
first request the backend makes, from the interceptor described below. The
guarantee for that source is that such an endpoint is never transmitted to. It
is *not* that construction failed, and an operator who reads a successful
startup as proof that the endpoint passed the policy has read it wrong.

What either door refuses is the same, and it is two separate questions. First,
a scheme the resolved transport does not carry is refused by name, whatever
`verify_tls` says: a transport introducing a new scheme spelling is the single
most likely place a transport change quietly weakens the `verify_tls` promise,
so admission is explicit rather than implied, and the plaintext opt-out has no
standing to widen it. Second, plaintext `http://` with `verify_tls: true` is
refused; `verify_tls: false` remains the deliberate, warned opt-in the compose
lab uses, and is the only way plaintext is ever carried. A refusal from an
environment door names the VARIABLE, because "plaintext endpoint refused" would
otherwise send an operator to their config file for a value that came from
their shell.

Enforced by a cross-product test over every registered transport, all four
construction-time sources and several plaintext spellings including case and
whitespace variants; by a test that drives the constructor itself, so the
policy cannot be left un-called; and by a test that walks both doors with one
table, so they cannot answer the same endpoint differently.

**And once more where guessing stops.** Enumerating sources is only ever as
complete as the list, and the list was not complete — source 5 is the proof of
that, and the reason for what follows. A client interceptor reads the REQUEST'S
OWN URI immediately before transmission and asks the same two questions there:
the resolved transport must carry the scheme, and plaintext `http://` is
refused while `verify_tls` is true. It holds the resolved transport itself
rather than a copy of its name, so it consults the one policy the
construction-time doors consulted instead of a second, weaker one. That check
runs for every request, so it covers source 5, a transport's `install` having
redirected the client, and any source nobody has thought of yet. The
source-level checks remain, because they fail at construction where a typo is
cheap to fix — but they are the convenience and this is the guarantee.

**The gate is installed LAST, after the transport, and the order is the
guarantee** (review). `install` takes the client-config builder by value and
returns one, so a transport that honours the contract may return a *fresh*
builder — and `set_interceptors` replaces the list even on the builder it was
handed. Either discards an interceptor installed before the call, so installing
the gate first would have let a transport-introduced plaintext endpoint travel
with no check at all; installing it after `install` has returned leaves a
transport nothing to remove it with. Position within the interceptor list is
not relied on: the orchestrator runs every `modify_before_transmit` hook before
any `read_before_transmit` hook, so the gate reads the URI after every other
interceptor has finished changing it. It is also registered as a PERMANENT
interceptor, which drops the per-request `DisableInterceptor` lookup an ordinary
one carries: no route to that switch exists today, but that is a fact about two
other crates' visibility rather than a property of this gate, and a policy check
should not rest on one. What the gate reads is the URI the
orchestrator produced, NOT the socket the transport's HTTP client opens — a
connector that dials somewhere else is outside anything the client-config
builder can reach, and falls under the trusted-code statement below rather than
being closed here.

**The evidence boundary: what the seam does and does not guarantee.** A fast
transport is the component most tempted to report a pass from its own
acknowledgements. The seam is placed BELOW `UploadBackend`, and the whole
surface a transport is given is:

```rust
fn name(&self) -> &str;
fn install(&self, builder: Builder) -> Result<Builder, VtopError>;
fn permits_scheme(&self, scheme: &str) -> bool;
fn tuning_support(&self) -> TuningSupport;
```

It receives the S3 client-config builder and nothing else. A transport
therefore cannot CALL `verify_object`, `head_object` or the manifest read-back,
cannot see their results, and cannot report a verification outcome; the
verification logic is not reachable from a transport implementation. A test
pins that surface so a fifth method cannot appear without this paragraph being
revisited. It parses the crate's source as Rust (with `syn`) rather than
scanning its text, because three review rounds found layouts a text scan could
not see: it pins the four signatures exactly and the supertraits as
`Send + Sync`, and refuses what a parse cannot see through — any attribute other
than documentation on the trait or its methods (an attribute macro can emit
methods); any attribute outside a short list of built-in inert ones (`doc`,
`cfg`, the lint levels, `path`, and `cfg_attr` judged by what it expands to) on
anything ENCLOSING the trait, because an attribute macro there receives the
trait's tokens too — inline modules, functions and impls around it, the `mod`
declarations that load its file, and the inner attributes of every file on the
way up from `lib.rs`; a trait file it cannot reach from `lib.rs`; a macro or
non-method item in the trait body; a where clause; a second declaration of the
name; and any trait elsewhere in the crate that builds on it. It reads source,
not expanded code: it cannot see a trait built on this one outside
`vtop-upload`, and it is a review tripwire for accidental growth,
not a proof against a determined author.

**That is not an adversarial boundary, and it must not be read as one**
(review). `install` returns the client-config builder that `S3NativeBackend`
then builds its ONE client from, and that same client serves `head_object` and
the stored-body read-back. A transport that wanted to could therefore influence
the requests verification travels over — caching a body and answering a later
read from it, for instance — without ever touching the verification code.

So the accurate statement is: **a registered transport is trusted code.** The
seam bounds what a transport can do by ACCIDENT — it cannot mistake a transport
ACK for a verification result, because it never sees one — and it does not
defend against a transport written to deceive. Transports are in-tree, reviewed,
and registered in `BUILTIN_TRANSPORTS`; there is no plug-in mechanism and no way
for a deployment to load one. If that ever changes, this paragraph is the thing
that has to change with it, and an independently configured client for the
verification path becomes a requirement rather than an option.

**What is NOT settled here.** The fallback rule, the datagram path's TLS
posture and its egress-policy requirements are reviewed when that path exists;
there is nothing to fall back from while one transport is registered.


## 3. Credential handling

Normative rules:

- **Credentials MUST NOT be stored in manifests.**
- **Credentials MUST NOT be printed in logs.**
- **Credentials SHOULD be supplied through environment variables, mounted secrets, or external secret managers.**

Additional guidance:

- Configuration files containing secrets **SHOULD** have restrictive filesystem permissions.
- On Unix, the engine **MUST** refuse, before reading it, a secret or ledger file that group or other can write: a manifest MAC key file (`manifest.mac.keys[].file`), the `engine.state_store` secret file, the SQLite ledger and its `-wal` and `-shm` files, and the `VTOP_AUDIT_LOG` file (#733). No setting accepts such a file.
- The engine **MUST** refuse such a file that other users can read, unless the directory holding it is owned by the engine's user and closed to group and other, or (for group read only) the file is a read-only Kubernetes Secret mount: reached through the kubelet's `..data` layout, on `tmpfs`, and not writable by the engine. The audit file may be group-readable. `security.allow_loose_secret_permissions: true` accepts the readable cases only; each file accepted that way **MUST** be announced by a WARN, `vtop_secret_file_permission_overrides_total` and an audit record (§10). `vtopctl validate` reports every such file at once. Recipes for external secret managers: [Secret files and their permissions](operate/secrets.md).
- New SQLite ledgers and audit files are created with mode `0600`; the engine never changes the mode of an existing file.
- `vtop-node` **MUST** refuse, before reading it, a TLS private key named in its config (`tls.key`, `replica_tls.key`, `native_tls.key`, `lease.tls.key`, `observability.tls.key`, and a scenario client's `tls.key`) by the same rule (#732). A node reads no engine config, so no setting accepts a loose key. The Helm chart mounts keys at `0440` with `fsGroup`, which the Kubernetes Secret rule accepts.
- Not yet checked: the other files `vtop-node` reads, and the engine's `VTOP_METRICS_TLS_KEY` and `vtopctl`'s `client_key`. On platforms without Unix permission bits nothing is checked.
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

The PostgreSQL state store follows the same rule (#694): the engine's runtime role has `SELECT, INSERT, UPDATE` on `batches` and `SELECT, INSERT` on `audit_events` (with `USAGE` on its sequence), and no `UPDATE`, `DELETE` or `TRUNCATE` on the audit table; a trigger refuses those to every role but the table's owner, so even a maintenance role granted `DELETE` for `prune-ledger` cannot remove an audit row ([POSTGRES_DEPLOYMENT.md](POSTGRES_DEPLOYMENT.md)).

### 5.1 Per-backend least privilege

| Backend | Minimum permissions | Notes |
|---------|---------------------|-------|
| `s3_native` | `PutObject`, `GetObject`/`HeadObject` (verify), `ListBucket` within prefix | SHA-256 uses a service-computed checksum; BLAKE3 requires read-back. |
| `awscli` / `s3cmd` / `minio mc` | Same as above | Strong verification downloads and hashes the stored body. |
| LocalFS | Filesystem write/read on the object tree only | The object tree directory **SHOULD** have restrictive permissions; the engine **SHOULD NOT** require broader filesystem access. |
| `s3_native` with `upload.server_side_encryption` | The `s3_native` row. With `mode: sse-kms` the runtime identity also needs `kms:GenerateDataKey` and `kms:Decrypt` on the key (the AWS-managed `aws/s3` key when `kms_key_id` is unset), and the `s3:PutObject` that sends the encryption header | A read-back (BLAKE3, or the SHA-256 of a multipart object) decrypts the object, so without `kms:Decrypt` those batches fail. A bucket policy that denies `PutObject` without the header makes a misconfigured engine fail loudly instead of writing plaintext. |
| `s3_native` with `upload.object_lock` (#684) | The `s3_native` row, plus `s3:PutObjectRetention`, `s3:GetObjectRetention`, `s3:GetBucketObjectLockConfiguration`, `s3:GetObjectVersion`, `s3:GetBucketVersioning`; with `legal_hold: true` also `s3:PutObjectLegalHold` and `s3:GetObjectLegalHold` | S3 returns the lock headers on `HeadObject` only to a caller holding `s3:GetObjectRetention` (`s3:GetObjectLegalHold` for the legal hold), so without it every batch fails its retention check. The runtime identity **SHOULD NOT** hold `s3:BypassGovernanceRetention`. See [OBJECT_LOCK_DEPLOYMENT.md](OBJECT_LOCK_DEPLOYMENT.md). |

Command compatibility backends (`awscli`, `s3cmd`, `minio mc`) **MUST** use an
explicit absolute executable path; PATH lookup is forbidden. VTOP resolves the
path and verifies the expected `--version` identity at startup. Each child
starts with an empty environment plus `LC_ALL=C`; only exact names declared in
`upload.command_env_allowlist` are copied from the runtime environment.
Invocations are killed and reaped after `upload.command_timeout_seconds`, and
captured stdout/stderr is bounded by `upload.command_max_output_bytes`.
Invocations that do not stream an object back (uploads, manifest writes,
deletes, heads, downloads to a file, the `--version` probe) end when the tool
exits: its output pipes then get a grace of up to 1 s (never past the
timeout) to be read to the end, and the output read by then is the result
(#593). On Unix these invocations also run in their own process group, which
is killed on the timeout, when the call is cancelled before the tool has
exited (engine shutdown, a caller's own deadline), and when a pipe has not
been read to its end once the grace is over, which normally means something
the tool started still holds it. On other platforms there is no process
group: only the tool itself is killed, and a descendant it started can
outlive the call.

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
- Where only size/existence can be confirmed, verification is **backend-limited** and the engine **MUST** report it as such rather than as cryptographic verification. The engine defaults to rejecting this result; accepting it requires the explicit `require_strong_verification: false` compatibility opt-out. That opt-out accepts same-size corruption, so the engine announces it: one WARN at startup and a `vtopctl validate` warning naming the key, `vtop_verification_mode{mode="size_only"} 1` from `vtopctl run`'s metrics endpoint, and a WARN with the batch id for every size-only verification, counted in `vtop_verification_backend_limited_total` where `vtop_verified_total` counts it (at VERIFIED in the running engine, at the commit in recovery) (#629).
- A keyed MAC authenticates data among key holders but does not provide public verification or non-repudiation.
- With `manifest.verification_receipts: true`, a **verification receipt** (`<batch_id>.receipt.json`, [VTOP_PROTOCOL_DRAFT.md §11.3](VTOP_PROTOCOL_DRAFT.md#113-verification-receipt-extension)) records in the archive what the ledger records after `VERIFIED`: the class and the method the backend ran. It is **evidence from the engine, not a commit precondition**: the commit never waits for it, and a receipt that cannot be written is a WARN and a `vtop_verification_receipts_total{result="failed"}` count, never a failed batch. What it proves and does not prove is in §8.4.

## 7. Data at rest and object immutability

- Object immutability **SHOULD** be supported where the backend allows it ([VTOP_PROTOCOL_DRAFT.md §18](VTOP_PROTOCOL_DRAFT.md#18-security-considerations)). VTOP validates bucket versioning (§8.1), and with `upload.object_lock` set (the S3 Object Lock profile, #684, `s3_native` only) it writes every object and manifest under a retention lock and verifies it:
  - every object PUT, manifest PUT and `CreateMultipartUpload` carries `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date` (the write's time plus `retain_days`; for a multipart object, the time of its create) and, with `legal_hold`, `x-amz-object-lock-legal-hold: ON`;
  - a bucket without Object Lock enabled is refused before anything is written to it: at startup for a literal bucket name, at the first batch for each bucket a template names (and for a literal bucket whose startup check the store could not answer);
  - before a batch may reach `VERIFIED`, the retention of the object and manifest versions it wrote is read back (the exact versions, except a reused object and a multipart object whose version the store did not report again, which are read at the version whose content was just verified) and compared with what the engine asked for (same mode, a date no earlier, the legal hold). A missing, shorter, other-mode or hold-less lock **fails the batch** (`FAILED` then `REPLAY_REQUIRED`, source progress not committed). An object reused from an earlier attempt (#626) must still be under the configured mode, with a date ahead and the configured legal hold; a replay checks that before it reuses one, and writes a fresh locked object under its per-attempt key instead when it is not, leaving the old object untouched (#709). A retention the replay cannot read fails the batch;
  - recovery and the scrub never fail a batch for a retention that has ended; `vtopctl scrub --check-retention` reports locked, elapsed, missing and mode_mismatch, the last two as findings.
  - a verification receipt (§8.4) is written through the manifest write path, so it carries the same retention headers, and its retention is read back the same way. A receipt whose retention does not match is counted as a failed receipt and logged; it never fails the batch. Recovery writes a missing receipt only after the store says none is there, so it never adds a second version beside an existing one. When the scrub checks receipts, `--check-retention` also reads each existing receipt's retention at its current key (#710), counted apart from the batch's in `vtop_scrub_receipt_retention_total`: a receipt with no retention or under another mode is a finding (it can be deleted or overwritten), an ended one is not, and a missing receipt is not a retention finding. Receipts are not pinned in the ledger, so the read is of the receipt's current version.
- The engine does **not** create a locked bucket, set a bucket default retention, or remove a lock. Object Lock is enabled when a bucket is created, and a bucket default retention **SHOULD** be set as the floor: an engine older than v0.10.0 ignores `upload.object_lock` and writes unlocked objects, which only a bucket default retention locks.
- Where the backend supports it (e.g., S3 Object Lock / WORM), telemetry objects and manifests **SHOULD** be written as immutable for the configured retention period.
- Immutability complements verification: verification detects tampering, immutability prevents post-write tampering or accidental overwrite.
- At-rest encryption **MAY** also be enabled at the storage layer, by a bucket default or a store setting; it is orthogonal to VTOP's integrity guarantees. With `upload.server_side_encryption` unset VTOP asks for none and checks none, so an object is encrypted only if the bucket's default says so.

### 7.1 Encryption at rest (`upload.server_side_encryption`)

With `upload.server_side_encryption.mode` set to `sse-s3` or `sse-kms` (`s3_native` only; any other backend refuses a mode but `none` at config load, because it would write plaintext), VTOP asks the store to encrypt what it writes and then checks that the store did:

- **Every write carries it.** Every object PUT, manifest PUT and verification-receipt PUT (a receipt goes through the manifest write path) and every `CreateMultipartUpload` carries `x-amz-server-side-encryption` (`AES256` for `sse-s3`, `aws:kms` for `sse-kms`). `UploadPart` and `CompleteMultipartUpload` carry none: for SSE-S3 and SSE-KMS S3 takes the encryption from the create.
- **`sse-kms` and the key.** With `kms_key_id` set, `x-amz-server-side-encryption-aws-kms-key-id` carries it, and `bucket_key: true` adds `x-amz-server-side-encryption-bucket-key-enabled`. With `kms_key_id` **unset**, the request carries `aws:kms` and **no key id**, so the store uses its default KMS key: on AWS, the account's AWS-managed `aws/s3` key, which has no monthly charge.
- **Verification fails closed.** Before a batch may reach `VERIFIED`, the `HeadObject` of the object and manifest it wrote (the same call that reads the size and the store-computed SHA-256) must report the encryption that was asked for. A store that reports **none**, **another algorithm**, or, when `kms_key_id` is set, **another key** fails the verification the way a checksum mismatch does (`FAILED` then `REPLAY_REQUIRED`, source progress not committed, counted in `vtop_verification_failures_total`), whatever the size and the checksum say: a write that should be encrypted and is not is never committed. The failure message names which of the three it was. An object a replay finds already stored under its key (#626) is judged the same way, so an unencrypted one is never reused. `vtopctl tier copy` reads the setting from its `--upload-config` file and applies it to the segment and the manifest it writes, and verifies both before it returns tier evidence: a segment or a manifest the store reports unencrypted refuses the copy. A verification receipt carries the header too and is checked after it is written, but it is evidence and never holds a batch back (§8.4): one the store reports unencrypted is a failed receipt (a WARN and `vtop_verification_receipts_total{result="failed"}`), not a failed batch.
- **Every path that touches a stored object judges it.** The backend judges the store's report (`HeadObject`, of the exact version where one is pinned) in two places, which share one rule: `verify_object` for an object and for a manifest a batch writes, and `check_encryption` for a read that has no verification of its own.

  | Path | How it judges encryption |
  |---|---|
  | Object and manifest write, single-part and multipart, and a lock-fallback object | The headers on the write; `verify_object` on the object and on the manifest before `VERIFIED` |
  | An object a replay finds stored under its key (#626), and an earlier attempt's recorded object a batch would adopt (#773) | `verify_object` before it is reused or adopted; an unencrypted one is left as it is and the batch is written under its own key. (Adoption is for backends that cannot tell a missing key from a refused request, which the command backends are, and they refuse this setting; the call is the same `verify_object`.) |
  | Recovery's re-check of a `VERIFIED` row | The same `check_stored_batch` as the scrub: `verify_object` on the object, `check_encryption` on the pinned manifest version |
  | `vtopctl scrub` | `verify_object` on the object, `check_encryption` on the pinned manifest version and on the manifest's current key, and on each receipt it reads, each a `mismatched` finding when the store reports otherwise |
  | `vtopctl tier copy` | The segment and the manifest each through `verify_object` before evidence is returned |
  | Verification receipt write | `check_encryption` on the version written; a failure is a failed receipt |
  | `vtopctl verify` | `check_encryption` on the manifest and the object, reported on an `encryption` line and failing the verdict; no line when the backend asked for none |
  | Manifest write after a batch is already written | None exists: a manifest is written once per attempt, by the path in the first row |
  - With `kms_key_id` **unset**, an `aws:kms` object passes whichever key the store reports; the check is the algorithm. VTOP does not check that the key is the account's `aws/s3` key.
  - With `kms_key_id` **set** to a key id or a key ARN, the key the store reports must match it by key id or by key ARN (a store reports the key's ARN). A store that reports `aws:kms` and no key id cannot be used with a named key.
  - With `kms_key_id` set to an **alias**, only the algorithm is checked, because a store reports the key's ARN and never the alias. The engine logs a WARN at startup saying so. Name the key by id or ARN to have it checked.
- **Checksums are unchanged.** SSE-S3 and SSE-KMS change neither the store-computed `x-amz-checksum-sha256` nor the read-back: S3 computes and returns the checksum over the plaintext, and a `GetObject` of an encrypted object returns plaintext. The live MinIO test (`live_s3_sse`) verifies a SHA-256 object, a BLAKE3 object and a multipart object under each mode.
- **Not covered.** SSE-C, DSSE-KMS (`aws:kms:dsse`) and client-side encryption are not supported. A bucket default encryption is the operator's and is not read or changed. Objects written before the mode was set stay as they were, and `vtopctl scrub` reports each as a mismatch (the store reports no encryption), which is accurate: re-encrypt them with a store-side copy. A multipart session begun before the mode was set completes into an unencrypted object, which then fails verification and is replayed under the new setting.
- **Observability.** The startup summary logs the mode and what a mismatch does, and `vtop_server_side_encryption_mode{tenant,mode}` reads 1 for the configured mode (`none`, `sse-s3`, `sse-kms`), set once at startup by `run`. Alert on `vtop_server_side_encryption_mode{mode="none"} == 1` for a fleet that must encrypt.
- **Keys are the store's.** VTOP never sees an object key, a data key or a KMS key's material. Who may use the KMS key, rotation and key deletion are KMS policy. A KMS key that is disabled or deleted makes the archive unreadable.

## 8. Manifest authentication

- VTOP 0.2 supports an optional keyed BLAKE3 authenticator in `manifest.mac` ([VTOP_PROTOCOL_DRAFT.md §11.2](VTOP_PROTOCOL_DRAFT.md#11-manifest-object), §17.3).
- Config stores only where a key is read from: an environment-variable name (`manifest_mac_key_env`, or `env:` in a keyring entry) or a file path (`file:`). The 32-byte hex key **MUST NOT** appear in config serialization, manifests, logs, metric labels or `validate --print-effective`, which shows key ids and source kinds (`env`, `file`) only.
- Naming an absent or malformed key **MUST** fail startup rather than silently emit unsigned manifests. The error names the key id and the variable or file, never the value.
- Enabling a key deliberately rejects unsigned pre-cutover manifests. Operators **MUST** verify or explicitly migrate their backlog before enabling it.
- Several keys can be configured, each with an id, so the key can be rotated (§8.3). Public-key signatures are not implemented.
- What a forged, swapped, retired-key or unsigned manifest does to the scrub and to recovery, with the MAC on and off, is measured against a real S3 server in [Tamper evidence](reference/tamper-evidence.md).

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
  VTOP validates versioning, and with `upload.object_lock` (#684) it writes
  each manifest version under that retention and checks the store kept it
  before the batch is `VERIFIED` (§7). A bucket default retention remains the
  floor for writers that do not set it.
- Backends without object versions (`localfs`, the `awscli`/`s3cmd`/`minio`
  command backends) record no version and keep today's current-key behavior
  with ledger hash binding; the hardened profile refuses to run on them.
  Rows written before this feature (no recorded version) are verified the
  same legacy way.

### 8.2 A manifest that claims a newer version (#671)

A reader checks a manifest's `version` before it authenticates anything,
so it can tell a newer writer's manifest from a damaged one. The version is
read from bytes nothing has authenticated yet, so it is only a **claim**: a
storage tamperer can overwrite a manifest with `{"protocol": "VTOP",
"version": "0.9"}`. Acted on unchecked, that claim would turn an integrity
finding into "upgrade the binary". Worse, recovery would hold the batch
VERIFIED for good, when it should replay it. So the claim counts only for
bytes proven to be what the batch's writer stored:

- The bytes **MUST** be the row's pinned, immutable `manifest_version_id`
  (§8.1). An overwrite of the key cannot change a pinned version.
- They **MUST** carry the batch id and the self-hash the ledger recorded.
  On its own that binds nothing, because the self-hash was computed by a
  scheme this build cannot reproduce and a tamperer can copy it into a
  forgery. What it adds is that the pinned version is this batch's.

When both hold, recovery leaves the row VERIFIED and pending. It counts the
row in `vtop_recovery_unsupported_manifest_total`, and retires it as
SUPERSEDED once its source has committed past it. The scrub reports
`unsupported_version`.

When either fails (no pin, or a pin that does not carry the ledger's
binding), the manifest is **unauthenticated**, exactly as before #671:
recovery marks the row FAILED and then REPLAY_REQUIRED, and the scrub reports
`unauthenticated`. A deployment without object versions therefore replays a
genuine newer-release batch rather than trusting it. That costs duplicates,
never a gap.

`vtopctl verify-manifest`, run standalone with no ledger to bind the bytes
to, reports that the manifest at that URI claims a version this build cannot
authenticate. It never reports it as intact.

### 8.3 Key ids and rotation (#685)

Manifests are immutable and pinned (§8.1), so a rotation cannot re-sign the
archive. After a rotation the archive holds manifests signed by several keys
for as long as it is retained. Signing stays simple, and verification is
key-aware:

- **One active key signs.** `manifest.mac.active` names it, and a version 0.4
  manifest records its id in `manifest.mac_key_id`. The id sits beside
  `manifest.mac` and is not blanked for the hash, so the self-hash and the MAC
  both cover it: editing the id fails both.
- **The id exists only in version 0.4.** A 0.3 reader re-encodes the decoded
  manifest before hashing, so it would drop the id and report the manifest as
  tampered. A keyring therefore requires `manifest.write_version: "0.4"`, and
  config load refuses the combination otherwise. A legacy (0.1-0.3) reader
  path ignores an id found in a legacy document, as v0.8.0 does.
- **The key the manifest names verifies it, never a search.** A reader
  **MUST NOT** try every key it holds: that would make retiring a key
  meaningless and hide which key signed what. A manifest with no id (every
  manifest before key ids, and any written with `manifest_mac_key_env`) is
  verified with the one key marked `legacy: true`. A process configured with
  `manifest_mac_key_env` holds one key and no ids: it verifies with that key
  whatever id a manifest names, as v0.8.0 does. One key is not a search.
- **Unknown and retired ids are unauthenticated, not corrupt.** A manifest
  naming an id the reader does not hold, or a tombstoned one
  (`manifest.mac.retired`), or naming none where no key is legacy, fails
  authentication with a message that names the id and says which case it is.
  Recovery treats it like any unauthenticated manifest (FAILED, then
  REPLAY_REQUIRED: duplicates, never a gap), and the scrub reports
  `unauthenticated`. The bytes may be intact, so the message never calls them
  damaged.
- **Retire means stop signing.** A key rotated out in the normal way stays in
  `manifest.mac.keys` as a verify-only entry for as long as its manifests are
  audited. A **tombstone** is for a key that was withdrawn or compromised:
  manifests under it then fail by design, and the scrub names them. No key
  material is configured for a tombstone.
- **Metric labels are configured ids only.** `vtop_manifest_authentications_total`
  and `vtop_scrub_manifest_keys_total` label by a configured id, `none` or
  `other`. An id read from a manifest comes from storage and is never a label;
  messages quote it escaped and cut to 64 characters.
- **Key files** (`file:`) are read once at startup, at most 4 KiB, and
  refused before they are read when other local users can write them, or
  read them outside a Kubernetes Secret mount or a closed directory (§3,
  #733), as for `engine.state_store` files. Mount them readable by the
  engine's user only (`defaultMode: 0400` and `fsGroup` for a Kubernetes
  secret).
- **An older reader cannot follow a rotation.** v0.8.0 and earlier hold one
  key and ignore the id: they verify a manifest only if their key signed it,
  and report the rest as unauthenticated (a live engine replays them). Every
  engine, scrub and auditor **MUST** run the keyring release before the
  active key changes. The engine cannot see which releases read the archive,
  so this gate is the operator's. The procedure is
  [Rotate the manifest MAC key](operate/manifest-key-rotation.md).

### 8.4 Verification receipts (#671)

A receipt is signed like a version 0.4 manifest: the self-hash and the keyed
BLAKE3 MAC cover its stored document, canonicalized, and it names the key in
`receipt.mac_key_id`. A reader verifies it with that key only, and an
unknown or retired key is unauthenticated, not corrupt (§8.3).

**What a receipt proves**, when a MAC key is configured and the receipt
verifies with it: a holder of that key wrote it, and it is bound to one
batch's manifest, by that manifest's self-hash, which the manifest's own MAC
covers. A receipt copied to another batch's key, or naming another manifest
digest, version or object, or another class than the ledger's, is reported
by `vtopctl scrub` as `mismatched`.

**What it does not prove:**

- **Without a MAC key, nothing about who wrote it.** Its self-hash is
  corruption detection only: anyone who can write the bucket can write a
  receipt with a correct hash.
- **Not that the object is intact now.** It records a verification that
  passed when it was written. The scrub re-verifies the object itself.
- **Not when, or by whom, beyond the writer's word.** `verified_at` is the
  engine's clock and `verified_by` its configured name. Both are covered by
  the MAC, so they are the key holder's statement, not independent facts.
- **Not that the source was committed.** A receipt is written after
  `VERIFIED` and before `SOURCE_COMMITTED`; the ledger stays the authority for
  commit state (§6). A missing receipt never makes a batch suspect, and is
  reported by the scrub, as a finding only under `--require-receipts`.

The receipt carries no record content, no key material, and no URI with a
query string, fragment or user info; the writer refuses such a URI rather
than record it. The warning for a receipt that could not be written strips
URL query strings and user info from the store's error.

### 8.5 A manifest is bound to its key (`verify-manifest`)

A genuine manifest copied over another batch's manifest key is intact: its
self-hash, its MAC, its object and its ledger row all check out. The scrub
and recovery catch the copy through the ledger, which records which manifest
belongs at which key (`is batch X's, not Y`). `vtopctl verify-manifest` may
run with no ledger that knows the batch, so it binds the manifest to its key
itself: every manifest names the key it was written to, in `manifest.uri`,
and the check **MUST** find that name equal to the URI it read the manifest
from, exactly. Otherwise it fails, naming both keys and the batch whose
manifest it is (`manifest key : FAILED`, `"manifest_key_ok": false`).

What is bound, by manifest version:

| Version | `manifest.uri` present | Covered by the self-hash | Covered by the MAC |
|---|---|---|---|
| 0.1 | yes | yes | no MAC exists in 0.1; with a key configured, an unsigned manifest is refused anyway (§8) |
| 0.2, 0.3 | yes | yes (the re-encoded fields) | yes, when the manifest is signed and a key is configured |
| 0.4 | yes | yes (the stored document) | yes, when a key is configured |

- **With a key configured**, the binding is authenticated: the MAC covers
  `manifest.uri` and `batch_id` together, so the key and the batch a manifest
  names are its signer's statement, and editing either fails the MAC.
- **Without a key**, the self-hash makes the binding corruption detection
  only, like every other field: someone who can write the bucket can rewrite
  `manifest.uri` and recompute the hash. A copied manifest left as it is
  fails.
- **Not derived from other fields.** The check does not rebuild the key from
  the batch id and the partition template: not every writer names a
  manifest that way (`vtopctl tier` writes `<object>.manifest.json`), and
  under the MAC the name the manifest carries is already authenticated.

## 9. Resource exhaustion controls

Resource exhaustion controls are part of the security boundary:

- `batching.max_bytes` is a hard ceiling for a source read. File and syslog
  readers stop before allocating beyond it; whole-file and Kafka records that
  exceed it fail without advancing source progress.
- Compression streams directly to the staging file instead of holding both an
  uncompressed aggregate and compressed aggregate in memory.
- The metadata peer and admin listeners and the replica listener hold at
  most `max_peer_connections`, `max_admin_connections` and
  `max_replica_connections` connections at once (#732); the native plane
  caps its sessions. See "Native broker transport" in §2.
- Successfully processed staging objects/manifests are removed immediately.
  Crash-left VTOP artifacts are removed after `engine.work_retention_seconds`,
  with `engine.work_max_bytes` enforcing an oldest-first aggregate ceiling on
  mutating-engine startup. Locks, directories, and unrelated files are excluded
  from cleanup. Symlinks are never followed; on Unix, deletion is anchored to
  an open directory and the regular file's device, inode, size, and no-follow
  entry type are revalidated immediately before unlinking. Changed entries are
  retained for the next cleanup pass.

## 10. Audit and failure logging

**Audit records (who changed state, #694)** are a separate obligation from batch lifecycle logs, and are described in [docs/operate/auditing.md](operate/auditing.md):

- Every operator command that changes engine or cluster state (`prune-ledger`, `replay`, `migrate`, `scrub --cursor-file`, the audited `vtopctl meta`, `node` and `tier` commands, and the admin endpoint's and every metadata replica's handling of them) **MUST** write a versioned audit record naming its actor, action, target, parameters and outcome, including failures and refusals. Every refusal by the admin endpoint's authorization policy is recorded, whatever the command.
- Audit records go to the `vtop::audit` log target, which the operator's log filter cannot silence and which is never sampled or rate-limited; to the append-only file `VTOP_AUDIT_LOG` names (created with mode `0600`, synced per record); and, for the ledger commands, to the state store's append-only `audit_events` table.
- The destructive ledger commands (`prune-ledger`, `replay`, `migrate`) **MUST** be refused when their attempt record cannot be written. There is no switch to run them unrecorded.
- The actor of a local command is the process's uid, its `/etc/passwd` name, the host and `$SUDO_USER`: as strong as the host's logins, and not authentication. The actor of a cluster command is the admin endpoint's verified certificate identity, or `anonymous` on a plaintext endpoint. From metadata feature level 9 the serving leader also commits that identity, the connection's address and its own node id in the replicated log, in the same entry as the command; a client cannot supply it, and a certificate name the record cannot hold is refused rather than truncated. What it proves is the identity the leader verified, not what the client claimed; it does not cover commands committed before the raise to level 9.
- `audit_events` is hash-chained; with a manifest MAC key configured the chain is keyed (a key derived from the signing key, §16.1), so a writer who can edit the table cannot recompute it. `vtopctl audit verify` recomputes it. A chain cannot show rows removed from its end.
- Audit parameters are an allowlist per action and pass through the redaction of §11; no audit record or audit metric label carries a credential, a connection string, a key, a key variable's value or a URL with its path or query.
- A secret or ledger file accepted only because `security.allow_loose_secret_permissions` is true **MUST** leave a `security.override` record (target: the file; parameters: what it holds, its mode, who can read it), once per file per process (#733). The audit file itself **MUST NOT** be writable by group or other, and **MUST NOT** be world-readable unless its directory is closed; it is created `0600`.

**Batch lifecycle and failure logs:**

- The engine **SHOULD** emit structured logs for batch lifecycle events (seal, upload, verify, commit) including `batch_id`, object key, and outcome.
- Failures **SHOULD** be logged with enough context to support replay and forensic review, **without** including secrets or raw sensitive payloads.
- Audit logs **SHOULD** be append-oriented and suitable for retention alongside the archived objects.

## 11. Secret redaction

- Any log path, error type, or diagnostic that could surface credentials **MUST** redact them.
- Connection strings, headers, and configuration dumps **MUST** have secret fields masked before logging.
- The OTLP endpoint is logged as host and path only, never with its user-info or query; the OTLP header values are never logged or printed (§2).
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

| Advisory | Crate | Why it is accepted for now | Last reviewed |
|----------|-------|----------------------------|---------------|
| RUSTSEC-2026-0253 | `lru 0.16.4` | An **unsoundness**, not a vulnerability: `LruCache::pop()` lacks panic safety, reachable only if a cached value's `Drop` panics during eviction. Fixed in `lru >=0.18.2`, a semver-incompatible line; it reaches the build solely through `aws-sdk-s3 1.137.0`, which requires `lru ^0.16`, so `cargo update -p lru` moves nothing. `aws-sdk-s3 1.159.0` requires `lru ^0.18.2`, but `cargo update -p aws-sdk-s3` keeps 1.137.0, so the fix needs a deliberate AWS SDK bump. The cached values are SDK-internal types this project neither supplies nor controls. Ignored by `cargo audit` only: `cargo deny`'s default `unsound = "workspace"` reports unsound advisories only for direct dependencies, so it never raises this one. Remove when `aws-sdk-s3` moves to a release that depends on `lru >=0.18.2`. | 2026-11-16 |
| RUSTSEC-2024-0449 | `unsafe-libyaml 0.2.11` | An **unmaintained** notice, not a vulnerability: the upstream repository is archived, so no fix will arrive. Reached only through `serde_yaml 0.9` (itself deprecated), which parses operator-written YAML configuration, not network input. The advisory points at safe-Rust YAML parsers; moving to one is a deliberate migration of every config type. Remove this exception when that migration lands. | 2026-11-16 |
| RUSTSEC-2026-0387 | `crypto-bigint 0.5.5` | Timing variability in operations intended to be constant-time. Fixed only in `crypto-bigint >=0.7.6`, a major bump; pinned to the 0.5.x line by `aws-sigv4 1.4.5` and `elliptic-curve 0.13.8`, so no `cargo update` reaches it. Exercised only on the AWS SigV4/SigV4a request-signing path. Remove when the AWS signing / RustCrypto stack adopts `crypto-bigint >=0.7.6` (`elliptic-curve >=0.14`). | 2026-11-16 |
| RUSTSEC-2026-0459 | `libsqlite3-sys 0.30.1` | Bundles a SQLite affected by CVE-2026-119375. Fixed in `libsqlite3-sys >=0.39.0` (SQLite 3.54), reachable only through a newer sqlx — `sqlx-sqlite 0.9.0` caps it at `<0.38`. SQLite here parses only the engine's **local**, operator-owned state-store database, never network input, so a crafted-database CVE is not reachable by a remote party. Remove when sqlx depends on `libsqlite3-sys >=0.39`. | 2026-11-16 |
| RUSTSEC-2026-0441 | `pin-utils 0.1.0` | An **unmaintained** notice, not a vulnerability: upstream is archived and no fix will ship. `cargo audit` treats it as a non-failing warning; `cargo deny`'s `unmaintained` check fails on it, so it is tracked in both files. Reaches the build only through `aws-smithy-http` / `aws-smithy-runtime`. Remove when the AWS SDK drops its `pin-utils` dependency. | 2026-11-16 |
| RUSTSEC-2026-0543 | `chrono 0.4.45` | An **unmaintained** notice, not a vulnerability; no fixed release exists. `chrono` is a direct workspace dependency for the timestamps the engine writes and reads back (manifests, ledger, audit records, object keys). `cargo audit` warns; `cargo deny`'s `unmaintained` check fails, so it is tracked in both files. Replacing it (`jiff` or `time`) is a migration of every timestamp type, not a bump. Remove when that migration lands or chrono is maintained again. | 2026-11-20 |

#### Review dates

Every ignored advisory carries the date it was last reviewed, written as
`# reviewed: YYYY-MM-DD` on its line in `.cargo/audit.toml` and `deny.toml` and
in the "Last reviewed" column above. An exception that nobody re-checks becomes
a permanent one, so a date older than 90 days fails CI: the `advisory review
dates` check (`scripts/check-advisory-review.py`, workflow
`.github/workflows/advisory-review.yml`) runs on every pull request, on every
push to `main` and weekly, warns from day 75 and fails from day 91. It also fails
when an id has no date, a malformed or future date, a different date in the two
files or in this table, or appears in one place and not the others. The only
allowed difference is an id that `cargo deny` never reports and so is listed in
`.cargo/audit.toml` alone; the script names each such id (today only
RUSTSEC-2026-0253).

The maintainer owns the review. A review of an id means:

1. Read the advisory's patched range (`https://rustsec.org/advisories/<id>`).
2. Run `cargo update -p <crate> --dry-run`: if the newest version the graph
   allows crosses the patched range, update the lockfile and remove the ignore.
3. Run `cargo tree --target all -i <crate>`: if the crate has left the graph,
   remove the ignore; if the path that pulls it in changed, update the
   justification.
4. Otherwise move the date to the day of the review in all three places, in one
   pull request that says what was checked.

Removing an ignore removes it from both files and this table together.

## 14. Security properties provided vs. not provided

| Property | Provided? | Notes |
|----------|-----------|-------|
| Object integrity (cryptographic) | Yes, with SHA-256/BLAKE3 | Stored object hash verified against manifest before commit. |
| Manifest corruption detection | Yes | Reproducible unkeyed self-hash. |
| Manifest authentication | Optional | Keyed BLAKE3 MAC; required when configured. |
| Chain of custody (object ↔ source markers) | Yes | Manifest binds object hash to covered markers. |
| Replay safety / no premature commit | Yes | Enforced in state machine, state store, and pipeline. |
| Transport confidentiality | Configurable (native broker: TLS 1.3 mTLS on every plane whose transport knob is left at its default; a plane switched to plaintext has none) / Configurable (Kafka, S3, PostgreSQL) | The native broker is TLS 1.3 mTLS on every plane whose transport is left at its default. Five node config knobs choose (#294 slice 5). Four are listeners — `peer_transport`, `admin_transport`, `replica_transport`, `native_transport` — each `tls` unless set to `plaintext` (loopback only) or `plaintext-on-any-interface` (spelled out deliberately); the fifth, `lease.transport`, is a dial with `tls` or `plaintext` only, and its loopback bound comes from the admin listener it dials, not from the dial. A plane on any plaintext setting has no transport confidentiality, so a security review of a hand-written node config reads all five. No `vtopctl` flag or Helm value sets them, so chart-rendered deployments are TLS throughout. A plaintext replica plane refuses the verbs whose authorization depends on a client certificate, and a plaintext native plane admits the configured principal on its declaration alone. Kafka/S3/PostgreSQL confidentiality is configured on those clients, not implemented in core. |
| PostgreSQL transport authentication | Yes for remote hosts | Non-loopback connections require `sslmode=verify-full`; loopback/socket plaintext is limited to local operation. |
| Backend-limited verification disclosure | Yes | Size-only mode is labeled and rejected by default. |
| Data-at-rest encryption | Yes, with `upload.server_side_encryption` on `s3_native` (SSE-S3 or SSE-KMS); otherwise not by VTOP | The store encrypts every object, manifest and receipt; VTOP asks for it on every write and fails a batch whose stored object is not reported as encrypted the way it was asked for (§7.1). Keys and key policy are the store's and KMS's. SSE-C, client-side encryption and an encrypted ledger or work directory are not provided; without the setting, encryption is delegated to the storage layer (bucket default). |
| Object immutability (WORM) | Yes, with `upload.object_lock` on `s3_native` | Objects and manifests written under an S3 Object Lock retention, the bucket's Object Lock checked first, and the stored retention of the exact versions verified before `VERIFIED` (§7, #684). The bucket's Object Lock and its default retention are created by the operator. `vtopctl tier copy` writes cold-tier copies under the same profile and checks their retention before it proposes the tier evidence. |
| MAC key rotation | Yes | Key ids in version 0.4 manifests and a keyring (§8.3). |
| Public-key manifest signing | Not yet | MAC keys are shared secrets: an auditor needs the key to verify. |
| Multipart upload integrity for very large objects | Yes, on `s3_native` | Above `upload.multipart_threshold_bytes` the engine uploads in parts (#482); a failed part is retried alone and the replay of the same bytes resumes the session (#668). Part ETags are never evidence: the whole stored object is verified before `VERIFIED`, as for a single-part upload. The command backends (`awscli`, `s3cmd`, `minio`) hand the whole object to their tool, which may split it internally but cannot resume a failed upload; `localfs` writes one file. |
| Authorization / multi-tenant isolation | Not by VTOP | Relies on storage-side IAM and least-privilege credentials. |
| Refusing secret files other local users can reach | Yes, on Unix (#733) | Manifest key files, the state-store secret file, the SQLite ledger and the audit file; not yet TLS keys or `vtop-node`'s files (§3). |

Each tamper-evidence claim in this table and in §6 to §8 and §10 is exercised
by a test that changes a stored object, manifest, receipt, audit row or ledger
row and checks what the defence reports, and, where the defence is off or
cannot see the change, that it does not: [Tamper evidence](reference/tamper-evidence.md)
lists every case with its test, and what is not protected (a compromised
engine host, a leaked MAC key, encryption at rest).

## 15. Summary of normative rules

| Rule | Level |
|------|-------|
| Credentials stored in manifests | **MUST NOT** |
| Credentials printed in logs | **MUST NOT** |
| Credentials via env vars / mounted secrets / external secret managers | **SHOULD** |
| TLS for Kafka and S3-compatible endpoints | **SHOULD** |
| TLS for an off-host OTLP collector (export is off unless configured) | **SHOULD** |
| Record content, credentials, keys, MACs, URLs with a path or query (the collector endpoint aside, printed as host and path), or OTLP header values in exported telemetry or log lines | **MUST NOT** |
| Error messages in span statuses redacted (URLs to host, hex runs, key ids) | **MUST** |
| Hostname-verified TLS for remote PostgreSQL | **MUST** |
| PostgreSQL URL supplied through an env/file reference | **MUST** |
| Least-privilege object storage permissions | **SHOULD** |
| `CreateBucket` scoped/avoided in runtime identity (per-format auto-create) | **SHOULD** |
| Verify object + manifest before commit | **MUST** |
| Report backend-limited verification as such (not cryptographic) | **MUST** |
| Configured manifest MAC verifies without downgrade | **MUST** |
| A manifest is verified with the key it names (or the legacy key), never by trying every key | **MUST** |
| Every reader runs the keyring release before the active manifest key changes | **MUST** (operator) |
| Pinned manifest version unreadable → fail closed (`FAILED` → `REPLAY_REQUIRED`) | **MUST** |
| Hardened profile: versioned bucket preflight + version on every manifest upload | **MUST** (when `require_object_versioning`) |
| Object Lock retention on versioned manifest buckets | **SHOULD** |
| With `upload.object_lock`: bucket Object Lock preflight, lock headers on every write, stored retention verified before `VERIFIED` | **MUST** |
| Bucket default retention as the floor under `upload.object_lock` | **SHOULD** (operator) |
| With `upload.server_side_encryption`: encryption headers on every write, the store's reported encryption verified before `VERIFIED` | **MUST** |
| Secret redaction in logs | **MUST** |
| Refuse a key, state-store, ledger or audit file group or other can write (no override) | **MUST** |
| Refuse such a file other users can read, outside a closed directory or a read-only Kubernetes Secret mount, unless the documented override is set, and announce and audit each override | **MUST** |
| Native broker transport restricted to TLS 1.3 with client certificates | **MUST** by default; a node config knob selects plaintext per plane, loopback-only unless acknowledged by name (§2) |
| Native broker sessions authorized by an explicit `SessionAuthorizer` | **MUST** |
| Native clients validate the broker certificate and expected server identity | **MUST** (deployed clients; the loopback lab client may omit `tls` and connect plaintext, see §2) |
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
| BLAKE3 keyed MAC (`manifest_mac_key_env`, or the `manifest.mac` keyring) | manifest build/verify (`vtop-core/manifest.rs`, `manifest/keyring.rs`) | manifest authenticity against an adversary with object-store write access; constant-time compare, fail-closed when keyed; verified with the key the manifest names (`manifest.mac_key_id`, authenticated), never by search | **security function** |
| BLAKE3 keyed MAC, audit chain (#694): a key derived (`blake3::derive_key`, its own context string) from the manifest signing key | `audit_events` link (`vtop-core/manifest.rs`, `vtop-state/audit.rs`), recomputed by `vtopctl audit verify` | tamper evidence of the audit table against a writer without the key; an unkeyed BLAKE3 chain without a configured key (corruption and casual-edit detection only) | **security function** (keyed); integrity check (unkeyed) |
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
| Cluster-plane TLS (admin, Raft peer, replica, native client; each plane's transport is a node config knob defaulting to TLS; plaintext is selectable per plane, loopback-only unless named, and confidentiality holds only on the planes left at `tls`, §2) | rustls 0.23, **ring pinned at every construction site**, TLS 1.3 only, mutual; CN identity via `x509-parser` after chain validation on the admin, Raft-peer and replica planes — and, on the native-client plane, the leaf CN must equal the node's `principal_id` (Q3 below) | peer authentication, confidentiality; authorization input where the CN is read (CN is a Raft **safety** input on the peer plane) — a native session is admitted only for the configured principal and only on the certificate issued to it | security function |
| Kafka source transport | librdkafka (vendored C, cmake) + **system OpenSSL** (`ssl`) + **system Cyrus SASL** (`sasl`) | broker TLS + SASL auth; plaintext unless configured (SHOULD-level, §2); missing password env is a hard startup error, never a silent downgrade | security function |
| S3 upload transport + signing | AWS SDK: hyper 1 + hyper-rustls + rustls 0.23 with the SDK's **aws-lc-rs** provider, system roots; SigV4 HMAC-SHA-256 (SigV4a/P-256 available, never explicitly invoked) | endpoint TLS (scheme floor via `verify_tls`; cert verification not disableable) and per-request authentication | security function |
| OpenTelemetry export (off until configured, #683) | opentelemetry-otlp (traces) and opentelemetry-proto (metrics), HTTP/protobuf over the hyper 1 + hyper-rustls client already in the tree; rustls 0.23 with **ring pinned** at its one construction site (`crates/vtop-observe/src/otel/http.rs`), system roots | collector TLS for `https://` endpoints (verification not disableable); authentication only as the operator's `OTEL_EXPORTER_OTLP_HEADERS` | security function |
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
does both, and only then can the MD5 path never be offered. Archive
objects are encrypted at rest by the store when `upload.server_side_encryption`
is set (§7.1); otherwise at-rest encryption is delegated to the storage layer
(§7).

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
`PrincipalAuthorizer` ignored the verified certificate chain: the declared
principal was matched against config and not bound to the cert, unlike the
meta and replica planes, where the leaf CN *is* the identity. That is closed:
the node now admits a native session only when the chain-verified leaf's CN is
the configured `principal_id` (the model the Helm chart already documented as
"= client cert CN"), refuses any other CA-signed certificate with an audited
`native.session` record, and keeps a plaintext native plane's behaviour (the
declaration is all there is, §2). A deployment whose client certificates carry
another CN must reissue them; the refusal names the CN it saw. The other half
remains: only one CI container is digest-pinned (`actionlint`) while the
shellcheck images are tag-pinned. That is a posture note, not an inventory
row.

---

## 17. Threat map

Section 1 names the assets, the adversaries and the trust boundaries. This
section maps each threat on those boundaries, including the cluster plane
(`vtop-meta`, `vtop-node`, `vtop-broker`) and the Kafka gateway
(`vtop-kafka`), to the control that stops it, to the test or CI check that
proves the control, and to the risk that remains. A threat with no control, or
no test, is written as a **GAP** in its cell and listed in §17.9. It is not
hidden and not argued away.

**How to read a row.** A reference is written `path::symbol`, relative to the
repository root. A control reference names the function, constant or message
that enforces the rule. A test reference names the test function, the
workflow job or the scenario script that exercises it. Line numbers are not
written here because every unrelated edit would move them;
`python3 scripts/check-threat-map.py --lines` prints the current `file:line` of
every reference.

**What keeps the map true.** `scripts/check-threat-map.py` runs in the `claims`
job of CI on every pull request, with its unit tests
(`scripts/tests/test_check_threat_map.py`). It fails when a reference names a
file or symbol that no longer exists, when a test reference is not a function
or is `#[ignore]`d, when a row has neither a reference nor a GAP in a cell,
when a group of rows is missing, and when §17.9 disagrees with the rows. It
proves that the named things exist. It does not prove that a test still
asserts what its row says: a reviewer who changes a control or a test reads
its row.

**Reading the planes.** The native broker has four listeners (metadata peer
and admin, replica, native produce and fetch), each with its own transport
knob (§2, "Native broker transport"). Rows in §17.4 to §17.7 are per plane;
the rows in §17.7 that say "every plane" apply to all four.

### 17.1 Archive engine: source, engine, object store and state store

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| A1 | A network attacker downgrades the Kafka source's SASL login to an unauthenticated session | `crates/vtop-adapters/src/kafka_source.rs::build_client_config` passes the TLS and SASL settings through and refuses to start when a named password variable is unset | `crates/vtop-adapters/src/kafka_source.rs::missing_password_env_is_a_config_error_not_a_silent_downgrade` | The test covers the silent downgrade only. Whether the link is encrypted is A2. |
| A2 | The Kafka source link runs in plaintext because nothing requires TLS | `crates/vtop-core/src/config.rs::kafka_plaintext_warning`: `security_protocol` is still optional and unset is librdkafka's plaintext, but when it is unset, `plaintext` or `sasl_plaintext` and a bootstrap server is not a loopback address, the engine warns on every start and `vtopctl validate` reports it (the config's warnings). It warns and does not refuse; a hostname that resolves to loopback is warned about | `crates/vtop-core/src/config.rs::a_plaintext_kafka_source_off_loopback_is_warned_about`. The Kafka matrix runs the whole Kafka source suite over an SSL listener and a SASL_SSL listener (`.github/workflows/kafka-matrix.yml`, the `tls` and `sasl-ssl` legs, with `tests/kafka_broker_env.rs::apply` setting `security_protocol`), and its TLS refusal tests in `tests/integration_kafka_auth.rs` (`tls_a_client_without_the_ca_refuses_the_broker`, `tls_a_client_given_another_ca_refuses_the_broker`; ignored by default, run by the matrix with `--ignored`) check the source verifies the broker | Source records and the SASL exchange cross the network unprotected until the operator sets `security_protocol` (§2 says SHOULD). The matrix runs nightly, on dispatch and on pull requests that touch its paths. |
| A3 | A network attacker, or a stray configuration, sends object and manifest traffic to the store over plaintext `http://` | `crates/vtop-upload/src/s3_native.rs::refuse_plaintext_uri` reads the URI of every request before it is sent; `crates/vtop-upload/src/s3_native.rs::validate_every_endpoint_source` refuses it at construction | `crates/vtop-upload/src/s3_native.rs::plaintext_is_refused_on_every_endpoint_source_for_every_transport`, `crates/vtop-upload/src/s3_native.rs::a_plaintext_request_is_refused_at_the_moment_it_would_be_transmitted`, `crates/vtop-upload/src/s3_native.rs::the_constructor_itself_refuses_a_plaintext_endpoint` | `verify_tls: false` is a deliberate, warned lab opt-in. The policy is in the `s3_native` backend: the `awscli`, `s3cmd` and `mc` backends apply no scheme check of their own. |
| A4 | A remote PostgreSQL connection is made without a verified server identity | `crates/vtop-state/src/pg_store.rs::enforce_tls_policy` | `crates/vtop-state/src/pg_store.rs::remote_postgres_requires_hostname_verified_tls` | Loopback and Unix-socket connections may be plaintext. The database's own access control is out of scope. |
| A5 | A storage tamperer alters an object after upload | The stored object is verified against the manifest before `VERIFIED`: `crates/vtop-upload/src/s3_native.rs::verify_object` (size and the service-computed checksum) and `crates/vtop-upload/src/base.rs::verify_file_content` (read-back hash) | `crates/vtop-upload/src/mock.rs::same_size_corruption_fails_with_uploader_metadata_unchanged`, `crates/vtop-cli/tests/integration_scrub.rs::a_corrupted_object_byte_is_reported_as_mismatched` | Verification detects. It does not prevent: tampering after `VERIFIED` is found by the scrub, and prevented only by A11. `require_strong_verification: false` accepts same-size corruption and says so (§6). |
| A6 | A storage tamperer edits or forges a manifest | `crates/vtop-core/src/manifest.rs::verify_authentication` checks the self-hash, then the keyed BLAKE3 MAC when a key is configured | `crates/vtop-cli/tests/integration_scrub.rs::a_manifest_with_a_bad_mac_is_reported_as_unauthenticated`, `crates/vtop-core/src/manifest.rs::enabling_a_key_rejects_unsigned_and_legacy_manifests` | With no MAC key the self-hash is corruption detection only: a writer who can replace the document can recompute it. A MAC gives no non-repudiation (§14). |
| A7 | A tamperer replays an older, still validly signed manifest, or deletes one | The ledger pins the manifest's object version and every later read addresses that version: `crates/vtop-upload/src/base.rs::get_manifest_pinned`; an unreadable pin fails closed | `crates/vtop-cli/tests/integration_scrub.rs::a_deleted_pinned_manifest_version_is_reported_as_missing`, `crates/vtop-upload/src/mock.rs::pinned_version_is_immutable_across_overwrites` | Backends without object versions (`localfs` and the command backends) cannot pin; the hardened profile refuses them (§8.1). |
| A8 | A tamperer overwrites a manifest with a claim of a newer version, to turn an integrity finding into "upgrade the binary" | `crates/vtop-cli/src/scrub.rs::newer_manifest_is_the_fleets` accepts the claim only for the pinned version bound to the ledger's batch id and hash; recovery uses `crates/vtop-cli/src/engine.rs::retire_unsupported` only for those | `crates/vtop-cli/tests/integration_scrub.rs::an_unpinned_newer_manifest_is_unauthenticated`, `crates/vtop-cli/tests/integration_scrub.rs::a_pinned_newer_manifest_not_bound_to_the_ledger_is_unauthenticated` | A deployment without object versions replays a genuine newer-release batch: duplicates, never a gap (§8.2). |
| A9 | A manifest names a key id the reader should not trust (unknown, retired or tombstoned) | `crates/vtop-core/src/manifest/keyring.rs::pub(crate) fn check` verifies with the key the manifest names, never a search | `crates/vtop-core/tests/manifest_keyring.rs::an_unknown_key_id_is_unauthenticated_not_corrupt`, `crates/vtop-core/tests/manifest_keyring.rs::changing_the_key_id_fails_the_hash_and_the_mac` | A reader older than the keyring release cannot follow a rotation. That gate is the operator's (§8.3). |
| A10 | A writer forges or rebinds a verification receipt | `crates/vtop-core/src/manifest/receipt.rs::verify_self_hash` and the receipt's MAC; the scrub compares each receipt with the ledger | `crates/vtop-core/tests/verification_receipt.rs::a_tampered_receipt_fails_its_hash_and_a_rehashed_one_fails_its_mac`, `crates/vtop-cli/tests/integration_scrub.rs::a_tampered_or_rebound_receipt_is_reported` | Without a MAC key a receipt says nothing about who wrote it. A receipt is evidence, never a commit precondition (§8.4). |
| A11 | A privileged writer deletes or overwrites an object or manifest after it is verified | With `upload.object_lock`, every object and manifest is written under a retention lock and `crates/vtop-upload/src/s3_native.rs::verify_retention` reads it back before `VERIFIED` | `crates/vtop-cli/tests/object_lock.rs::scrub_check_retention_reports_locked_missing_and_mode_mismatch`, `crates/vtop-cli/tests/object_lock.rs::a_store_without_object_lock_or_versioning_is_refused_for_the_lock_first` | Opt-in, `s3_native` only. The bucket's Object Lock and default retention are the operator's; the engine never removes a lock (§7). |
| A12 | Source progress is committed for data that was never verified, so a corrupted or tampered object loses records for good | `crates/vtop-core/src/state_machine.rs::can_transition` allows `SOURCE_COMMITTED` only from `VERIFIED`; PostgreSQL repeats the rule in the trigger `vtop_enforce_commit_after_verify` (`crates/vtop-state/migrations/postgres/0001_state_store.sql`) | `crates/vtop-core/tests/prop_state_machine.rs::commit_is_reachable_only_from_verified`, `crates/vtop-core/src/state_machine.rs::test_cannot_commit_from_manifest_uploaded`, `crates/vtop-state/tests/pg_battery.rs::postgres_backend_passes_battery_and_enforces_invariant_at_db` | SQLite's only database-side rule is that a row is inserted as `batching`: the rest is the state machine and the store's write path. Anyone with write access to the ledger file defeats it (A15 limits who has). |
| A13 | The engine's own database role holds more than it needs, so a bug or a stolen credential can delete ledger or audit rows | The documented runtime grants in `docs/POSTGRES_DEPLOYMENT.md::GRANT SELECT, INSERT ON TABLE public.audit_events` give `SELECT, INSERT, UPDATE` on `batches` and no `DELETE` or `TRUNCATE` | `crates/vtop-state/tests/pg_battery.rs::postgres_backend_passes_battery_and_enforces_invariant_at_db` connects as that role and asserts CREATE, ALTER, DELETE, TRUNCATE and audit-table writes are denied | The operator applies the grants; the engine cannot see a role's scope on a database it was not given a privileged connection to. |
| A14 | An over-broad object-store credential lets the engine, or anyone holding its key, read, list or delete more than the archive needs | GAP: §5 lists the minimum permissions per backend; nothing in the engine can see a credential's scope, and A11 is the only engine-side control that limits a deleter | GAP: no test or check exercises the permissions the engine actually needs | Credential scope is storage-side IAM (§14: "Authorization / multi-tenant isolation: Not by VTOP"). |
| A15 | Another local user reads or edits a MAC key file, the state-store secret, the SQLite ledger or the audit file | `crates/vtop-core/src/secret_file.rs::judge` refuses a file group or other can write, or other users can read, before it is read; an accepted loose file is announced and audited | `crates/vtop-cli/tests/secret_file_permissions.rs::startup_refuses_a_key_file_other_users_can_reach`, `crates/vtop-cli/tests/secret_file_permissions.rs::the_override_starts_announced_and_audited_but_never_for_a_writable_file` | Unix only. `vtop-node` checks its TLS private keys (S1), not the other files it reads. |
| A16 | A hostile or runaway source exhausts the engine's memory | The source readers stop at `batching.max_bytes` and fail without advancing progress: `crates/vtop-adapters/src/file_source.rs::exceeds max_bytes`, `crates/vtop-adapters/src/syslog_spool_source.rs::exceeds max_bytes`, `crates/vtop-adapters/src/kafka_source.rs::MultiTopicAcc` | `crates/vtop-adapters/src/file_source.rs::rejects_a_line_before_it_can_exceed_the_memory_budget`, `crates/vtop-adapters/src/syslog_spool_source.rs::rejects_an_oversized_spool_record_without_advancing`, `crates/vtop-adapters/src/kafka_source.rs::multi_topic_acc_enforces_byte_budget_per_topic` | The ceiling bounds one batch. Total memory is that ceiling times the sources the operator configures. |
| A17 | A local attacker swaps a staging file for a symlink so the engine's cleanup deletes something else | `crates/vtop-core/src/work_dir.rs::cleanup_work_dir` never follows symlinks and revalidates the entry just before it unlinks | `crates/vtop-core/src/work_dir.rs::never_follows_or_removes_symlinks`, `crates/vtop-core/src/work_dir.rs::skips_an_entry_swapped_to_a_symlink_after_enumeration` | Unix only for the anchored delete. |
| A18 | An object key from a manifest or source escapes the local object tree | `crates/vtop-upload/src/localfs_backend.rs::refusing path-traversal segment` | `crates/vtop-upload/src/localfs_backend.rs::rejects_path_traversal` | Applies to the `localfs` backend only. |
| A19 | An external CLI backend is swapped, found through `PATH`, or inherits secrets from the environment | `crates/vtop-upload/src/command.rs::PATH lookup is forbidden` (absolute path), `env_clear` with an allowlist, and `crates/vtop-upload/src/command.rs::verify_version` | `crates/vtop-upload/src/command.rs::version_identity_accepts_stderr_and_rejects_the_wrong_tool`, `crates/vtop-upload/src/command.rs::clears_environment_and_copies_only_allowlisted_names` | The tool runs outside the Rust dependency graph; the operator pins its package (§13). |
| A20 | A secret reaches a log, an error, a metric label, an exported span or `validate --print-effective` | `crates/vtop-state/src/pg_store.rs::redact_postgres_urls`, `crates/vtop-observe/src/audit.rs::redact`, `crates/vtop-observe/src/otel/redact.rs::redact` | `crates/vtop-state/src/pg_store.rs::postgres_urls_are_removed_from_errors`, `crates/vtop-cli/tests/effective_config.rs::effective_output_never_resolves_or_prints_secret_references`, `crates/vtop-observe/src/otel/redact.rs::digests_macs_and_key_ids_are_removed`, `crates/vtop-cli/tests/otel_messages.rs::export_on_sends_traces_and_metrics_and_says_so` | Redaction is by pattern. A secret in a shape no pattern knows, quoted in an external tool's output, can pass. |
| A21 | A compromised or vulnerable dependency reaches the engine | `.cargo/audit.toml` and `deny.toml::unknown-git` (crates.io only, advisories fail the build); ignores carry a review date that expires | `.github/workflows/ci.yml::supply-chain (cargo audit)`, `.github/workflows/ci.yml::supply-chain (cargo deny)`, `scripts/tests/test_check_advisory_review.py::test_an_id_without_a_date_fails` | Advisory-based: a malicious crate no one has reported passes. |
| A22 | A release archive or image is replaced after build, or is not the output of the release workflow | `.github/workflows/release.yml::cosign sign` signs the image and `cosign sign-blob` the archives keyless, the build provenance action attests both, and `.github/workflows/release.yml::SHA256SUMS` lists the archives | `.github/workflows/release.yml::gh attestation verify` runs on the image and every archive before the release is published; `docs/RELEASE_VERIFICATION.md::cosign` is the procedure a consumer runs | The in-workflow check proves the workflow's own attestation, not that a consumer's download matches it: that check is the consumer's. A dispatch dry-run publishes and attests nothing. |
| A23 | A workflow action is swapped behind a mutable tag | Every action is pinned to a full commit SHA, for example `.github/workflows/ci.yml::actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1` | GAP: no check fails a workflow that uses a tag | A new workflow can add an unpinned action and CI stays green. |
| A24 | A compromised container runs as root, with a writable root filesystem and all capabilities | `docker/Dockerfile::USER vtop`; the chart sets `helm/vtop/values.yaml::readOnlyRootFilesystem`, `runAsNonRoot` and drops all capabilities | GAP: no check asserts the rendered pod keeps these settings | A values override or a chart edit can remove them unnoticed. |

### 17.2 Audit trail

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| I1 | An operator runs `prune-ledger`, `replay` or `migrate` and leaves no record | `crates/vtop-cli/src/audit.rs::attempt` writes the attempt to every sink first and refuses to act when one fails | `crates/vtop-cli/tests/audit_trail.rs::prune_ledger_is_refused_when_its_attempt_cannot_be_recorded`, `crates/vtop-cli/tests/audit_trail.rs::an_unwritable_audit_file_fails_startup_naming_the_path` | The actor of a local command is the host login, not authentication (§10). |
| I2 | A writer with database access edits or reorders `audit_events` rows | The table is append-only (SQLite triggers `trg_audit_events_no_update` and `trg_audit_events_no_delete`) and hash-chained, keyed when a MAC key is configured: `crates/vtop-state/src/audit.rs::verify_chain` | `crates/vtop-cli/tests/audit_trail.rs::audit_verify_finds_an_edited_row`, `crates/vtop-state/src/audit.rs::a_keyed_chain_cannot_be_recomputed_without_the_key`, `crates/vtop-state/src/sqlite_store.rs::sqlite_keeps_an_append_only_chained_audit_table` | A chain cannot show rows removed from its end. Unkeyed, a writer can recompute it. Ship `vtop::audit` to a sink the writer cannot reach. |
| I3 | A maintenance role granted `DELETE` for `prune-ledger` removes audit rows in PostgreSQL | The trigger `vtop_audit_events_append_only` and the no-truncate trigger refuse every role but the table's owner (`crates/vtop-state/migrations/postgres/0001_state_store.sql`) | `crates/vtop-state/tests/pg_battery.rs::postgres_backend_passes_battery_and_enforces_invariant_at_db` | The table's owner can. |
| I4 | A local user reads or rewrites the audit file | `crates/vtop-observe/src/audit.rs::options.mode(0o600)` creates it owner-only, and A15's check refuses a file others can write | `crates/vtop-cli/tests/secret_file_permissions.rs::a_new_ledger_and_a_new_audit_file_are_owner_only`, `crates/vtop-cli/tests/secret_file_permissions.rs::the_audit_file_may_be_group_readable_only` | The file is append-oriented, not append-only at the filesystem level: root, or the owner, can edit it. |
| I5 | An audit record, or an audit metric label, carries a credential | `crates/vtop-observe/src/audit.rs::redact` and a per-action allowlist of parameters | `crates/vtop-observe/src/audit.rs::secret_shaped_values_are_redacted_in_every_allowed_parameter`, `crates/vtop-observe/src/audit.rs::a_parameter_off_the_allowlist_is_refused` | Pattern-based, as A20. |
| I6 | A cluster command through the admin endpoint, accepted or refused, leaves no attributable record | `crates/vtop-meta/src/transport/admin.rs::crate::audit::refusal` and the edge record carry the certificate identity | `crates/vtop-meta/src/transport/admin.rs::an_operators_write_is_audited_at_the_edge_with_its_certificate_identity`, `crates/vtop-meta/src/transport/admin.rs::a_refused_write_is_audited_with_the_refusal_reason`, `crates/vtop-cli/tests/audit_cluster_client.rs::a_refused_or_failed_cluster_command_is_recorded_as_such` | On a plaintext admin plane the actor is `anonymous` (test `a_plaintext_write_is_audited_as_anonymous`). |
| I7 | An operator with a valid operator certificate misuses it: membership, topic and lease changes are all open to every operator | Detective only: `crates/vtop-meta/src/transport/admin.rs::command_audit` records the certificate identity, the command and the outcome (I6) | `crates/vtop-meta/src/transport/admin.rs::an_operators_write_is_audited_at_the_edge_with_its_certificate_identity` | No separation of duties: every identity in `admin_authorization` may run every cluster-scoped command, and nothing asks a second person. The record is the deterrent and the evidence. |

### 17.3 Cluster plane: metadata (Raft peer and admin planes)

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| M1 | A host with a certificate joins the Raft group as another node, or spoofs a vote or a version request | `crates/vtop-meta/src/transport/tls.rs::assert_peer_identity` binds the certificate's CN to the claimed sender | `crates/vtop-meta/src/transport/peer.rs::mtls_rejects_wrong_peer_cn`, `crates/vtop-meta/src/transport/peer.rs::mtls_rejects_vote_sender_spoof`, `crates/vtop-meta/src/transport/peer.rs::mtls_refuses_a_version_request_from_a_spoofed_sender` | Every node's certificate comes from one CA: a compromised CA key, or a stolen node key, is a node. |
| M2 | A certificate-holding data node, or any client, rewrites cluster membership or grants a range lease to an arbitrary holder | `crates/vtop-meta/src/transport/authz.rs::AdminAuthorizer`: reads for any authenticated client, a node's own lease commands for that node, everything cluster-scoped for named operators | `crates/vtop-meta/src/transport/admin.rs::a_node_may_not_change_membership_over_the_wire`, `crates/vtop-meta/src/transport/admin.rs::a_node_may_not_drive_another_nodes_lease_over_the_wire`, `scripts/live-chaos/scenarios/11-admin-authorization.sh` | On by default in the Helm chart only. A hand-written node config without `admin_authorization` makes every certificate the CA signed an operator, and the node only warns at startup (test `an_unconfigured_endpoint_still_permits_everything` documents it). The chart renders the policy unless `meta.adminAuthorization.enabled` is false, the lab opt-out, and refuses to render with the policy on and no operator named (`helm/vtop/values.yaml::adminAuthorization`, #864). A data node releases its own lease under the policy only from metadata feature level 9 (#870); below it the release is refused and the lease lapses on its deadline. |
| M3 | A plaintext admin plane is reachable, so anyone who reaches it runs `meta init`, membership and lease commands | `crates/vtop-meta/src/transport/admin.rs::plaintext_on_any_interface` is the only way off loopback; an enforcing policy on plaintext is refused because no CN exists to match | `crates/vtop-meta/src/transport/admin.rs::a_plaintext_admin_endpoint_refuses_a_routable_address`, `crates/vtop-meta/src/transport/admin.rs::a_plaintext_admin_endpoint_refuses_a_policy_it_could_not_enforce` | On loopback, or after the named opt-in, plaintext admin is unauthenticated: the node warns every start. |
| M4 | A plaintext peer plane lets any host that can reach it join the Raft group as any node id | `crates/vtop-meta/src/transport/peer.rs::plaintext_on_any_interface` is the only way off loopback | `crates/vtop-meta/src/transport/peer.rs::plaintext_refuses_a_non_loopback_bind`, `crates/vtop-meta/src/transport/peer.rs::plaintext_cannot_detect_a_spoofed_vote_sender` | The second test records the limit: on a plaintext peer plane the sender is self-asserted and membership rests on network isolation. |
| M5 | A malformed or oversized admin or peer frame exhausts or crashes a metadata node | `crates/vtop-meta/src/transport/wire.rs::MAX_FRAME_BYTES` and the per-field bounds in the same file | `crates/vtop-meta/src/transport/wire.rs::a_placement_response_claiming_an_absurd_replica_count_is_refused`, `crates/vtop-meta/src/transport/wire.rs::the_committed_fuzz_seeds_decode`, `fuzz/fuzz_targets/vtpm_frame.rs`, `.github/workflows/ci.yml::fuzz smoke (every target)` | The bounds are per frame. Concurrent connections are M6. |
| M6 | A client with a valid certificate opens connections to the peer or admin plane without limit | `crates/vtop-meta/src/transport/limit.rs::ConnectionLimit` caps each listener (`max_peer_connections`, default 256; `max_admin_connections`, default 1024; `crates/vtop-node/src/config.rs::check_connection_cap`), closes the connection over the cap at accept, before its TLS handshake, and closes one that sends no request within 10 s of accept (`DEFAULT_HANDSHAKE_DEADLINE`); both are counted on `vtop_listener_connections_refused_total{plane,reason}` (`crates/vtop-node/src/observe.rs::ConnectionLimitCollector`) and named in a rate-limited WARN | `crates/vtop-meta/src/transport/peer.rs::a_peer_connection_over_the_cap_is_closed_and_counted`, `crates/vtop-meta/src/transport/peer.rs::a_silent_peer_connection_is_closed_at_the_handshake_deadline`, `crates/vtop-meta/src/transport/admin.rs::an_admin_connection_over_the_cap_is_closed_and_counted`, `crates/vtop-meta/src/transport/admin.rs::a_silent_admin_connection_is_closed_at_the_handshake_deadline`, `crates/vtop-meta/src/transport/limit.rs::the_connection_over_the_cap_is_refused_and_counted_and_a_freed_slot_is_reused` | A cap bounds how many connections there are, not who holds them: a flood can still take every slot while it lasts, and an authenticated client that sends its first request may hold a slot while connected. The counter and the WARN make both visible. |

### 17.4 Cluster plane: replica plane

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| R1 | A plaintext replica peer fences a replica, which truncates its log, or pulls sealed segments | `crates/vtop-broker/src/replication/network.rs::fence_needs_identity` and `transfer_needs_identity` refuse both verbs when there is no certificate | `crates/vtop-broker/src/replication/network.rs::a_plaintext_peer_is_refused_a_fence`, `crates/vtop-broker/src/replication/network.rs::a_plaintext_peer_is_refused_a_sealed_segment_transfer` | A plaintext replica plane still accepts appends from any peer that can reach it; appends can be reconciled away, destroyed records cannot. Loopback only unless named. |
| R2 | A certificate-holding node that is not a follower of the range pulls the range's sealed segments | `crates/vtop-node/src/data_node.rs::authorize_transfer` allows the leader's followers and the UUIDs in `transfer_peers`, and audits each decision | GAP: no test presents an authenticated node outside the allowlist; the audit record's wording is tested in `crates/vtop-node/src/audit.rs`, and the allowed path in scenario 12 | A transfer hands over a whole range's bytes, so an untested allowlist is the property to test first. |
| R3 | A deposed leader keeps writing after a new leader is granted, or a lease holder stalls past its deadline and then writes | Replicas refuse a stale epoch: `crates/vtop-broker/src/lib.rs::ErrorCode::Fenced`; a leader fences replicas under the epoch metadata granted | `crates/vtop-broker/tests/replica_fencing.rs::fencing_stops_the_deposed_leader_from_writing`, `crates/vtop-broker/tests/meta_lease_fencing.rs::grant_produce_steal_fences_prior_broker`, `scripts/live-chaos/scenarios/09-range-leader-failover.sh` | Safety rests on the epoch check, not on clocks. Lease takeover timing rests on a clock-skew allowance that bounds, and does not remove, the error. |
| R4 | A peer fences a replica to an epoch metadata never granted, parking it in permanent refusal | `crates/vtop-broker/src/replication/mod.rs::fn fence` refuses an epoch above the granted one | `crates/vtop-broker/tests/replica_fencing.rs::a_fence_above_the_granted_epoch_is_refused` | Needs a reachable replica port and a certificate (or a plaintext plane, R1). |
| R5 | A frame is corrupted or forged in flight, or announces a length that exhausts memory before it is read | `crates/vtop-protocol/src/lib.rs::decode_frame` checks the BLAKE3 checksum and the length bound before allocating | `crates/vtop-protocol/src/lib.rs::rejects_corruption_trailing_bytes_and_oversized_lengths_before_allocation`, `fuzz/fuzz_targets/protocol_frame.rs`, `.github/workflows/ci.yml::fuzz smoke (every target)` | The checksum is unkeyed framing integrity. Authenticity is TLS's; a plaintext plane has none. |
| R6 | A slow or flooding replica holds leader memory | `crates/vtop-broker/src/memory_budget.rs::MemoryBudgetPool` bounds in-flight and catch-up bytes per follower | `crates/vtop-broker/tests/memory_budget.rs::follower_budget_bounds_slow_replica_inflight`, `crates/vtop-broker/tests/memory_budget.rs::producer_flood_stays_bounded_and_rejects_retryably` | A budget refusal is retryable. The bound is on memory held, not on how many peers connect (R7). |
| R7 | A peer with a valid certificate opens connections to the replica plane without limit | `crates/vtop-meta/src/transport/limit.rs::ConnectionLimit` on the replica listener (`crates/vtop-broker/src/replication/network.rs::with_max_connections`; `max_replica_connections`, default 256), with the same accept-time refusal and 10 s handshake deadline, counted on `vtop_listener_connections_refused_total` | `crates/vtop-broker/src/replication/network.rs::a_replica_connection_over_the_cap_is_closed_and_counted` (the cap of one, then a silent connection closed at its deadline) | As M6. |

### 17.5 Cluster plane: native produce and fetch

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| N1 | A client whose certificate verifies claims a principal or role it may not have | `crates/vtop-broker/src/lib.rs::SessionAuthorizer` is called before any request; `crates/vtop-node/src/native_auth.rs::PrincipalAuthorizer` admits a TLS session only when the chain-verified leaf's CN is the configured `principal_id`, as producer or consumer, and audits every refusal (#854) | `crates/vtop-node/src/native_auth.rs::a_certificate_issued_to_another_principal_is_refused`, `crates/vtop-node/src/native_auth.rs::the_certificate_issued_to_the_principal_is_accepted`, `crates/vtop-broker/src/lib.rs::mtls_session_acks_durable_produce_and_fetches_committed_data` (its first hello names another principal and is refused `Unauthorized`) | One principal per node: every client holding the certificate issued to it acts as it. A plaintext native plane admits the declaration alone (N4). |
| N2 | A session produces under another producer's identity, to poison its sequence or epoch | `crates/vtop-broker/src/lib.rs::producer ID must equal the authenticated session principal ID` | `crates/vtop-broker/src/lib.rs::mtls_session_acks_durable_produce_and_fetches_committed_data` (a produce with another producer id is refused before the epoch journal changes) | Bound to the principal, which is N1's residual. |
| N3 | One authenticated consumer moves another member's cursor | `crates/vtop-broker/src/lib.rs::request.member_id != principal_id` refuses a commit for another member | `crates/vtop-broker/src/lib.rs::mtls_session_binds_cursor_member_id_to_the_authenticated_principal` | As N1. |
| N4 | A plaintext native plane admits a session on a declared principal alone | `crates/vtop-broker/src/lib.rs::authorize_unverified` is refused by default; plaintext off loopback needs the named opt-in | `crates/vtop-broker/src/lib.rs::a_certificate_authorizer_admits_nothing_over_plaintext_by_default`, `crates/vtop-broker/src/lib.rs::a_plaintext_native_plane_refuses_to_bind_off_loopback_unless_told_so_by_name` | After the opt-in, anything that can reach the port and knows the principal UUID can produce and consume (§2). |
| N5 | Many sockets exhaust the session slots | `crates/vtop-broker/src/lib.rs::try_acquire_owned` enforces `ServerConfig::max_sessions` and counts the refusal | GAP: no test opens `max_sessions + 1` sessions; the gateway's cap is tested (K6), the native plane's is not | An unauthenticated peer can hold handshakes open until `handshake_timeout` (N6). |
| N6 | A peer opens a connection and never completes the handshake | `crates/vtop-broker/src/lib.rs::handshake_timeout` is one deadline from accept across the TLS handshake, authorization and the first frame | `crates/vtop-broker/src/lib.rs::the_handshake_budget_is_one_deadline_across_judgment_and_first_frame` | Default 5 s per connection, up to `max_sessions` at a time. |
| N7 | A zombie producer writes after its epoch was superseded | The producer epoch journal fences stale sessions: `crates/vtop-log/src/types.rs::ProducerFenced` is surfaced as `Fenced` | `crates/vtop-broker/src/lib.rs::v2_mode_broker_persists_real_producer_epochs_and_fences_stale_sessions`, `crates/vtop-broker/tests/epoch_journal_crash_sweep.rs` | Fencing is by epoch, not identity: who may open a session at all is N1. |
| N8 | A native client talks to an imposter broker | `crates/vtop-node/src/tls.rs::client_config` validates the server against the configured roots | `crates/vtop-node/src/native_auth.rs::a_client_verifies_the_servers_certificate` (a server certificate from another CA, and one for another name, are refused before the hello) | The client verifies the chain and the name it dials; a CA that signs for an attacker defeats it. |

### 17.6 Kafka gateway

The gateway is a `kafka:` block in a data node's config (`docs/deploy/cluster-plane.md`). Kafka's wire protocol as served here has no SASL and no TLS, and the gateway has no per-topic authorization: it relies on where its listener is bound.

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| K1 | The Kafka listener is reachable by hosts that should not reach it | `crates/vtop-node/src/config.rs::refuse_kafka_gateway_misuse` keeps the listener on loopback unless `kafka.any_interface: true`, and `crates/vtop-node/src/data_node.rs::resolved off loopback` judges the address the bind resolved to | `crates/vtop-node/src/config.rs::a_kafka_gateway_is_refused_on_a_follower_and_off_loopback_unless_named`, `scripts/live-chaos/scenarios/16-kafka-gateway.sh` | With `any_interface: true` the control is a network policy the operator provides (K2). The Helm chart does not render the block. |
| K2 | A client that can reach the listener produces to, consumes from or joins groups on any topic it serves | GAP: no TLS, no SASL (`SaslHandshake` is not a served API key, so a client that expects it is closed, not downgraded) and no ACLs; the gateway answers whoever connects | GAP: nothing tests an authenticated or authorized path, because none exists; `crates/vtop-kafka/src/messages.rs::an_unserved_api_is_refused_with_a_correlation_id_to_answer_on` pins only that an unserved API is refused | Anything that reaches the port has the gateway's full surface. The node warns at startup that the protocol carries no vtop identity. |
| K3 | A client announces a huge frame, or sends bytes that decode wrongly, to exhaust or crash the gateway | `crates/vtop-kafka/src/gateway.rs::max_frame_bytes` closes the connection before reading a frame above the bound; the decoders refuse impossible lengths before allocating | `crates/vtop-kafka/src/gateway.rs::every_gap_is_refused_by_name` (a frame length of `i32::MAX`), `crates/vtop-kafka/src/wire.rs::a_length_larger_than_the_buffer_is_refused_before_allocating`, `crates/vtop-kafka/src/records.rs::a_corrupted_payload_is_refused_by_the_crc`, `fuzz/fuzz_targets/kafka_request.rs` | The default bound is 32 MiB per frame and 256 sessions, so the worst case is the product, not one frame. |
| K4 | A client announces a frame and never sends it | `crates/vtop-kafka/src/gateway.rs::frame_read_timeout` ends the wait | `crates/vtop-kafka/src/gateway.rs::a_frame_announced_and_not_sent_is_closed_at_the_read_timeout` | The wait is 30 s per frame by default, for up to 256 sessions. |
| K5 | A client holds a connection open and silent, keeping a session slot | `crates/vtop-kafka/src/gateway.rs::idle_timeout` (10 minutes by default) closes it | GAP: no test lets a session idle past the limit; shutdown's close of idle sessions is tested, the timeout is not | A client can hold all slots for up to the idle timeout. |
| K6 | Sockets exhaust the gateway | `crates/vtop-kafka/src/gateway.rs::max_sessions` closes a session over the cap at once | `crates/vtop-kafka/src/gateway.rs::sessions_over_the_cap_are_closed_and_a_freed_slot_is_served` | As K5. |
| K7 | A Kafka client writes under a native producer's identity, or two gateways collide | The gateway appends under its own producer id, derived from the node principal and never equal to it: `crates/vtop-node/src/config.rs::kafka_producer_id` | `crates/vtop-node/src/config.rs::the_kafka_producer_id_is_derived_from_the_principal_and_never_equal_to_it`, `scripts/live-chaos/scenarios/18-kafka-idempotent-producer.sh` | Every Kafka client shares the gateway's identity: the audit trail cannot tell them apart. |
| K8 | A gateway routed to an external Kafka cluster sends records and offsets over a link with no TLS and no credentials | GAP: `crates/vtop-kafka/src/remote.rs` dials plain TCP and has no SASL or TLS settings | GAP: no test, because there is nothing to test | Records on `kafka`, `dual` and `shadow` routes cross the network unprotected, and a cluster that requires authentication cannot be used. |
| K9 | One Metadata request that names a topic many times forces a large answer | `crates/vtop-kafka/src/gateway.rs::One entry per topic NAMED` answers each name once | GAP: no test sends a repeated-name Metadata request | Bounded by the topology size (at most 256 brokers, 4 096 partitions). |

### 17.7 Transport on every plane

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| T1 | A peer with no certificate, or one from a CA the plane does not trust, completes a handshake on the metadata, replica or native plane | Each plane builds its acceptor with a required client-certificate verifier: `crates/vtop-meta/src/transport/tls.rs::build_server_acceptor`, `crates/vtop-broker/src/replication/network.rs::build_server_acceptor`, `crates/vtop-broker/src/lib.rs::WebPkiClientVerifier` | GAP: on the native plane `crates/vtop-node/src/native_auth.rs::an_absent_client_certificate_is_refused` and `crates/vtop-node/src/native_auth.rs::an_untrusted_client_certificate_is_refused` (#854); on the metadata and replica planes the tests present a wrong identity under a trusted certificate (M1) and a mutual-TLS client that is accepted, and none presents an untrusted or absent certificate | The control is the rustls verifier, whose behaviour is not in doubt; the wiring is what an untested line can lose. |
| T2 | The observability endpoint serves `/metrics` and `/readyz` to any host | `crates/vtop-observe/src/server.rs::start_tls` with a client CA serves only a scraper the CA vouches for | `crates/vtop-observe/src/server.rs::a_mutual_tls_endpoint_serves_only_a_scraper_with_a_certificate`, `crates/vtop-observe/src/server.rs::a_tls_endpoint_serves_the_routes_and_closes_a_plaintext_probe` | Off by default: the endpoint is plaintext and unauthenticated unless TLS is configured (`docs/deploy/cluster-plane.md`). |
| T3 | A plane is left plaintext, or exposed off loopback, by mistake | Every plane's transport defaults to TLS; `crates/vtop-node/src/config.rs::check_plaintext_exposure` refuses plaintext off loopback before anything binds | `crates/vtop-node/src/config.rs::plane_transports_default_to_tls_and_refuse_a_tls_plane_without_paths`, `crates/vtop-node/src/config.rs::a_plaintext_plane_off_loopback_is_refused_before_anything_binds`, `scripts/live-chaos/scenarios/15-plaintext-lab.sh` | `plaintext-on-any-interface` is the named opt-in. |
| T4 | A peer speaks the other transport to downgrade a plane | `crates/vtop-meta/src/transport/maybe_tls.rs::judge_dialect` refuses a peer speaking the other transport; nothing is negotiated | `crates/vtop-meta/src/transport/maybe_tls.rs::a_peer_speaking_the_other_transport_is_refused_naming_both_sides`, `crates/vtop-broker/src/lib.rs::a_tls_hello_on_a_plaintext_native_plane_is_refused_before_any_frame_is_read`, `crates/vtop-broker/src/replication/network.rs::a_tls_hello_on_a_plaintext_replica_plane_is_refused_naming_both_sides` | The refusal is by the first byte on the wire and nothing is negotiated, so there is no downgrade path; the metadata admin plane's version of this test is `crates/vtop-meta/src/transport/admin.rs::the_admin_plane_refuses_a_cross_mode_peer_naming_both_sides`. |

### 17.8 Node host and data at rest

| ID | Threat | Control | Test or CI check | Residual risk |
|----|--------|---------|------------------|---------------|
| S1 | Another local user reads a node's TLS private key | `crates/vtop-node/src/tls.rs::check_private_key` refuses every TLS private key the node's config names (`tls.key`, `replica_tls.key`, `native_tls.key`, `lease.tls.key`, `observability.tls.key`, a scenario client's `tls.key`) before reading it, by the A15 rule, with no override | `crates/vtop-node/src/tls.rs::a_tls_private_key_other_users_can_read_is_refused_before_it_is_read`, `crates/vtop-node/src/tls.rs::a_tls_private_key_other_users_can_write_is_refused`, `crates/vtop-node/src/tls.rs::an_owner_only_tls_private_key_is_read` | A readable key is a node's identity on every plane, and the CA signs for the whole cluster. |
| S2 | An attacker with write access to a node's log or metadata directory forges records or Raft state | GAP: segment and record checksums (`crates/vtop-log/src/codec.rs::record_content_hash`) are unkeyed, so they detect damage, not forgery | GAP: the crash sweeps and fuzz targets test damage and torn writes, not an adversarial rewrite | Host compromise is outside what the cluster defends. Disk encryption and host hardening are the operator's. |

### 17.9 Gaps

Each row below is a cell marked GAP above. Closing one means adding the control or the test and removing the GAP from its row, which removes the row here; the check fails until both are done.

| ID | Missing | What closes it |
|----|---------|----------------|
| A14 | both | A startup probe or a documented least-privilege policy test against the engine's actual calls; today the scope is storage-side IAM. |
| A23 | test | A check that every `uses:` in `.github/workflows` carries a 40-character SHA. |
| A24 | test | A chart test that renders the pod and asserts the non-root, read-only and capability settings. |
| R2 | test | A `vtop-node` test that a TLS peer outside the followers and `transfer_peers` is refused a transfer. |
| N5 | test | A broker test that opens `max_sessions + 1` sessions and sees the last refused and counted. |
| K2 | both | A Kafka-side authentication and authorization design (TLS, SASL, ACLs) for the gateway; until then the control is the loopback default and a network policy. |
| K5 | test | A gateway test with a short `idle_timeout` and a silent session. |
| K8 | both | TLS and SASL settings for the remote bridge, with a test against a broker that requires them. |
| K9 | test | A gateway test that a Metadata request repeating one name returns one entry. |
| T1 | test | A test on each of the metadata and replica planes that a client with no certificate, and one from another CA, is refused (the native plane has them, #854). |
| S2 | both | Not a cluster control today; it needs an integrity design for data at rest (a keyed checksum or storage-layer controls) before it can have a test. |

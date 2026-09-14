//! Native S3 backend built on `aws-sdk-s3` / `aws-config`.
//!
//! Supports AWS S3, MinIO, and Ceph RGW via a custom endpoint and optional
//! path-style addressing. Credentials are read from the environment by the SDK
//! credential chain and are never logged.
//!
//! Integrity: for **SHA-256** the precomputed digest is sent on `PUT`
//! (`x-amz-checksum-sha256`), so the store recomputes the body hash and rejects
//! a corrupted upload (server-validated), and verification reads that
//! store-computed checksum back via `head_object`. For **BLAKE3**, verification
//! streams the stored body through BLAKE3. The uploader-provided digest remains
//! user metadata for inventory tooling only and is never strong evidence.
//! When checksums are disabled, verification falls back to size + existence
//! (backend-limited).

use crate::base::{
    parse_s3_uri, read_bounded, ObjectChecksum, ObjectHead, StoredManifest, StoredObject,
    UploadBackend, UploadedPart, VerificationResult,
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumMode, CompletedMultipartUpload, CompletedPart,
};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use vtop_core::checksum::digest_reader;
use vtop_core::errors::VtopError;
use vtop_core::types::ChecksumAlgorithm;

// The egress transport seam (#479) lives at `src/transport.rs` and is declared
// HERE, as a submodule of the seam file, rather than in `lib.rs`: the issue
// requires `lib.rs` (and `build_backend`) to stay byte-for-byte unmodified, and
// the seam this module realizes is inside `S3NativeBackend::new` below. The
// public path is `vtop_upload::s3_native::transport`.
#[path = "transport.rs"]
pub mod transport;
use transport::{EgressTransport, TransportRegistry};

const CHECKSUM_META_KEY: &str = "vtop-checksum";

/// Convert a lowercase-hex SHA-256 into the base64 form S3 uses for
/// `x-amz-checksum-sha256` (base64 of the raw 32-byte digest).
fn hex_to_b64_sha256(hex_sha: &str) -> Option<String> {
    let raw = hex::decode(hex_sha).ok()?;
    if raw.len() != 32 {
        return None;
    }
    Some(B64.encode(raw))
}

/// Convert S3's base64 `x-amz-checksum-sha256` back into lowercase hex so it
/// compares against the engine's hex SHA-256 representation.
fn b64_to_hex_sha256(b64: &str) -> Option<String> {
    let raw = B64.decode(b64).ok()?;
    if raw.len() != 32 {
        return None;
    }
    Some(hex::encode(raw))
}

/// Connection / addressing settings for the native S3 backend.
#[derive(Debug, Clone)]
pub struct S3NativeConfig {
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub verify_tls: bool,
    /// The registered egress transport name (#479). `tcp_tls` (the default) is
    /// a strict no-op over the client config; the name is resolved against the
    /// registry in [`S3NativeBackend::new`], failing closed on an unknown name.
    pub transport: String,
    /// The egress tuning for the active transport (#480). A field the resolved
    /// transport cannot honour (per its `tuning_support`) is refused at
    /// construction in [`S3NativeBackend::new`], naming the field and the
    /// transport — never silently ignored.
    pub tuning: vtop_core::config::EgressTuning,
}

pub struct S3NativeBackend {
    client: Client,
}

/// Enforce the transport policy BEFORE any client is built (#75).
///
/// `verify_tls: true` (the default) means "telemetry must travel encrypted":
/// a plaintext `http://` endpoint under it is a configuration error, not a
/// warning — silently accepting one is exactly the downgrade the flag claims
/// to prevent. `verify_tls: false` is the explicit lab opt-out that permits
/// plaintext endpoints (e.g. the compose lab's `http://minio:9000`).
///
/// Honest scope: this flag does NOT disable certificate verification for
/// `https://` endpoints — the AWS SDK always verifies against the system
/// trust store. A self-signed or private-CA endpoint needs its CA in the
/// system trust store; skipping verification is deliberately unsupported.
/// The endpoint variables the SDK resolves from the environment, in the order
/// it applies them (#488). Named as data rather than written inline at the one
/// call site, so the "every source is checked" claim is something a test can
/// enumerate rather than a promise a reader has to take on trust.
pub(crate) const ENDPOINT_ENV_VARS: &[&str] = &["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"];

/// The last word on the scheme, at the moment the URI is finally knowable
/// (#488).
///
/// Enumerating endpoint SOURCES can only ever be as complete as the list, and
/// the list was not complete: a shared-profile `[services]` section can carry
/// an S3-specific `endpoint_url` that `SdkConfig::endpoint_url()` does not
/// expose, because the service config applies it later (review). Rather than
/// grow the list again and hope, this refuses at the point where guessing
/// stops — the request's own URI, immediately before it is transmitted.
///
/// It runs for EVERY request, so it also covers a transport's `install` having
/// redirected the client, and any future source nobody has thought of. The
/// source-level checks stay: they fail at construction, which is where an
/// operator wants to hear about a typo, while this one cannot be out-run.
///
/// It enforces BOTH halves of the scheme policy, because for the source it was
/// written for they are the only halves there are (review): a scheme the
/// resolved transport does not carry is refused whatever `verify_tls` says, and
/// plaintext `http://` is refused while `verify_tls` is true.
struct RefusePlaintextTransmit {
    verify_tls: bool,
    /// The RESOLVED transport, not its name (review). An endpoint that comes
    /// only from an S3-specific shared-profile `[services]` section is invisible
    /// to every source validator, which makes this interceptor the SOLE policy
    /// gate for it — so it has to be able to ask what the source doors ask. A
    /// name can only be printed; the transport itself answers `permits_scheme`,
    /// and holding the very one the client was built from is what keeps the
    /// request-time answer and the construction-time answer from drifting.
    transport: Arc<dyn EgressTransport>,
}

// `Intercept` requires `Debug`, and a transport is a trait object carrying no
// such bound. Render it by the name it registered under: that is the part of it
// an operator would recognise in a log line, and the rest is a client builder.
impl std::fmt::Debug for RefusePlaintextTransmit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefusePlaintextTransmit")
            .field("verify_tls", &self.verify_tls)
            .field("transport", &self.transport.name())
            .finish()
    }
}

impl aws_smithy_runtime_api::client::interceptors::Intercept for RefusePlaintextTransmit {
    fn name(&self) -> &'static str {
        "vtop::refuse_plaintext_transmit"
    }

    fn read_before_transmit(
        &self,
        context: &aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        refuse_plaintext_uri(
            context.request().uri(),
            self.verify_tls,
            self.transport.as_ref(),
        )
        .map_err(Into::into)
    }
}

/// The transport's `install`, and THEN the last gate — in that order, because
/// the order is the whole guarantee (#488, review).
///
/// [`EgressTransport::install`] takes a `Builder` by value and returns one, so a
/// transport that honours the contract may legitimately return a FRESH builder
/// rather than the one it was handed; and even on the handed-in one,
/// `Builder::set_interceptors` replaces the interceptor list outright. Either
/// discards anything installed before the call. Installing the gate first — and
/// asserting in a comment that this prevented its removal, which was the
/// previous shape here — was therefore backwards: a transport introducing a
/// plaintext endpoint would have dropped the one check that would have caught
/// it, and the bytes would have reached the wire. Installing after `install` has
/// already returned leaves a transport nothing to remove it with.
///
/// Ordering alone is not the whole answer, so it is not the whole mechanism.
/// The gate is registered as a PERMANENT interceptor: an ordinary one carries a
/// per-request `DisableInterceptor<T>` lookup in the config bag, and permanence
/// removes that lookup rather than relying on nobody ever reaching the bag. No
/// route to it exists today — the S3 builder's `push_runtime_plugin` is crate
/// private and `RefusePlaintextTransmit` is not exported — but both of those
/// are accidents of other crates' visibility, and a policy gate should not rest
/// on an accident. Turning this one off now takes editing this line.
///
/// Position WITHIN the interceptor list is deliberately not relied on. The
/// orchestrator runs every `modify_before_transmit` hook before any
/// `read_before_transmit` hook, so this gate reads the URI after every other
/// interceptor has finished changing it wherever it sits in the list; what
/// matters here is only that it is on the builder the client is built from.
///
/// RESIDUAL GAP, stated rather than implied. This gates the URI the
/// orchestrator hands to the HTTP client; it does not gate the socket that
/// client opens. Installing an HTTP client is exactly what the seam exists for,
/// and a connector that dials somewhere other than the URI it was given is
/// beyond anything the client-config builder can prevent — no ordering and no
/// permanence closes that. It is the trusted-code boundary described in
/// SECURITY_MODEL.md §2, and it is why "a registered transport is trusted code"
/// is the accurate claim rather than "a transport cannot reach the wire
/// unchecked".
fn install_transport_then_the_last_gate(
    builder: aws_sdk_s3::config::Builder,
    transport: &Arc<dyn EgressTransport>,
    verify_tls: bool,
) -> Result<aws_sdk_s3::config::Builder, VtopError> {
    let mut gated = transport.install(builder)?;
    // The gate carries the RESOLVED transport, not the configured name, so the
    // one door a shared-profile [services] endpoint reaches asks the full scheme
    // policy rather than just the plaintext half of it (#488, review).
    gated.push_interceptor(aws_sdk_s3::config::SharedInterceptor::permanent(
        RefusePlaintextTransmit {
            verify_tls,
            transport: Arc::clone(transport),
        },
    ));
    Ok(gated)
}

/// The scheme a URI carries, lowercased — or `None` when it carries none.
///
/// Strict about the spelling on purpose: RFC 3986 says a scheme is a letter
/// followed by letters, digits, `+`, `-` or `.`, so a relative URI whose PATH
/// happens to contain `://` is not mistaken for a scheme and refused for one it
/// never had. One helper for the construction-time doors and the request-time
/// interceptor, because two spellings of "what is a scheme" would be two
/// policies, and the weaker one would be the one that decided.
fn uri_scheme(uri: &str) -> Option<String> {
    let (candidate, _) = uri.trim_start().split_once("://")?;
    let mut chars = candidate.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic()
        || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        return None;
    }
    Some(candidate.to_ascii_lowercase())
}

/// The decision the interceptor makes, as a pure function.
///
/// Separated so it is testable: constructing a real interceptor context means
/// standing up a request, a runtime and a config bag, and the thing worth
/// pinning is the RULE — which URIs are refused, and that the lab's deliberate
/// opt-out still works.
///
/// TWO questions, deliberately kept apart (review). `permits_scheme` asks what
/// wire the transport carries at all; `verify_tls` asks whether plaintext is
/// permitted on it. Answering only the second is how a future HTTPS-only
/// transport would be handed a hidden `http://` endpoint under the lab opt-out,
/// and how any scheme nobody admitted would travel unremarked: the opt-out
/// speaks to plaintext and has no standing to speak to capability. So the
/// admission check runs FIRST and regardless of `verify_tls`, exactly as it does
/// on the source doors, and only then does `verify_tls: false` return early.
fn refuse_plaintext_uri(
    uri: &str,
    verify_tls: bool,
    transport: &dyn EgressTransport,
) -> Result<(), String> {
    if let Some(scheme) = uri_scheme(uri) {
        if !transport.permits_scheme(&scheme) {
            return Err(format!(
                "refusing to transmit to {uri}: the {} transport does not carry {scheme}:// \
                 endpoints. The endpoint did not come from the configuration or the \
                 environment — a shared-profile [services] section or a transport can supply \
                 one that no source check sees — and a wire nobody admitted is a wire nobody \
                 chose, so it is refused here, at the request itself",
                transport.name()
            ));
        }
    }
    if !verify_tls {
        // The deliberate, warned lab opt-out — of PLAINTEXT, and of nothing
        // else. Refusing here would break the compose lab, which is the one
        // configuration that asks for plaintext on purpose; widening it to the
        // scheme check above would let it excuse a wire the transport never
        // claimed to speak.
        return Ok(());
    }
    if uri.trim_start().to_ascii_lowercase().starts_with("http://") {
        return Err(format!(
            "refusing to transmit to plaintext {uri} while verify_tls is true (transport \
             {}). The endpoint did not come from the configuration or the \
             environment — a shared-profile [services] section or a transport can supply one \
             that no source check sees — so it is refused here, at the request itself",
            transport.name()
        ));
    }
    Ok(())
}

/// Apply the scheme policy to EVERY endpoint source, in one place (#488).
///
/// One unit rather than three call sites, because the property that matters is
/// not "each validator refuses" but "the constructor still consults all of
/// them" — and three calls are three things that can be deleted individually
/// while the tests stay green (review). With one call, deleting it removes ALL
/// validation, which no test survives.
///
/// The sources are parameters, not lookups: the environment doors read fixed
/// variable names that a test cannot set without racing every other test in
/// the process, and the SDK-resolved endpoint is only known after the loader
/// has run. Passing all three in is what makes the cross product a table.
pub(crate) fn validate_every_endpoint_source(
    explicit: Option<&str>,
    read_env: impl Fn(&str) -> Option<String>,
    sdk_resolved: Option<&str>,
    verify_tls: bool,
    transport: &dyn EgressTransport,
) -> Result<(), VtopError> {
    validate_endpoint_scheme(explicit, verify_tls, transport)?;
    validate_env_endpoint_schemes(read_env, verify_tls, transport)?;
    // Whatever endpoint actually resolved — explicit config, environment, or
    // the SDK's shared config file — is what the client will talk to.
    validate_endpoint_scheme(sdk_resolved, verify_tls, transport)
}

/// Apply the scheme policy to every endpoint the ENVIRONMENT can supply.
///
/// The reader is injected rather than calling `std::env::var` directly (#488):
/// these are fixed variable names, so a test that set them for real would race
/// every other test in the process, and the security property most worth
/// pinning would be the one property left untested. With the reader as a
/// parameter the cross product of transport × source is a table.
///
/// A refusal names the VARIABLE, because "plaintext endpoint refused" sends an
/// operator to their config file when the value came from their shell.
pub(crate) fn validate_env_endpoint_schemes(
    read: impl Fn(&str) -> Option<String>,
    verify_tls: bool,
    transport: &dyn EgressTransport,
) -> Result<(), VtopError> {
    for var in ENDPOINT_ENV_VARS {
        if let Some(ep) = read(var) {
            validate_endpoint_scheme(Some(&ep), verify_tls, transport)
                .map_err(|e| VtopError::Config(format!("{var}: {e}")))?;
        }
    }
    Ok(())
}

fn validate_endpoint_scheme(
    endpoint_url: Option<&str>,
    verify_tls: bool,
    transport: &dyn EgressTransport,
) -> Result<(), VtopError> {
    let Some(ep) = endpoint_url else {
        return Ok(()); // default AWS endpoints are always https
    };
    let ep_trim = ep.trim();
    // A transport that admits a different scheme spelling is the single most
    // likely place a transport change quietly weakens the verify_tls promise
    // (#479): refuse a scheme the resolved transport does not carry, BY NAME,
    // before the plaintext check. tcp_tls admits only http/https, so this is a
    // no-op for the shipping path and a real gate for any future wire.
    if let Some(scheme) = uri_scheme(ep_trim) {
        if !transport.permits_scheme(&scheme) {
            return Err(VtopError::Config(format!(
                "endpoint_url {ep} uses scheme {scheme}:// which the {} transport does not carry",
                transport.name()
            )));
        }
    }
    let plaintext = ep_trim.to_ascii_lowercase().starts_with("http://");
    if plaintext && verify_tls {
        return Err(VtopError::Config(format!(
            "endpoint_url {ep} is plaintext http:// while verify_tls is true; refusing to send \
             telemetry unencrypted. Use an https:// endpoint, or set verify_tls: false \
             (VTOP_S3_VERIFY_TLS=false) to explicitly opt into a plaintext LAB endpoint"
        )));
    }
    if plaintext {
        tracing::warn!(
            endpoint = %ep,
            "plaintext S3 endpoint permitted because verify_tls=false (lab use only)"
        );
    }
    Ok(())
}

/// Refuse any configured egress-tuning field the transport cannot honour (#480),
/// naming the field and the transport. This is what makes the symmetric shape
/// honest: a knob set on a path that cannot apply it fails loudly at
/// construction rather than being silently dropped. The support flags map to
/// fields as: `rate_control` → `target_rate_bytes_per_second`; `parallelism` →
/// `max_concurrency` / `part_size_bytes` / `parts_in_flight`; `redundancy` →
/// a non-`none` `redundancy` policy.
fn reject_unsupported_tuning(
    transport: &dyn EgressTransport,
    tuning: &vtop_core::config::EgressTuning,
) -> Result<(), VtopError> {
    use vtop_core::config::RedundancyPolicy;
    let support = transport.tuning_support();
    let name = transport.name();
    let refuse = |field: &str| {
        Err(VtopError::Config(format!(
            "the {name} transport cannot honour upload.transports.{name}.{field}; \
             it is configured but this transport does not support it"
        )))
    };
    if tuning.target_rate_bytes_per_second.is_some() && !support.rate_control {
        return refuse("target_rate_bytes_per_second");
    }
    if tuning.max_concurrency.is_some() && !support.parallelism {
        return refuse("max_concurrency");
    }
    if tuning.part_size_bytes.is_some() && !support.parallelism {
        return refuse("part_size_bytes");
    }
    if tuning.parts_in_flight.is_some() && !support.parallelism {
        return refuse("parts_in_flight");
    }
    if tuning.redundancy != RedundancyPolicy::None && !support.redundancy {
        return refuse("redundancy");
    }
    Ok(())
}

/// Refuse a tuned `part_size_bytes` outside the range S3 can actually carry
/// (#480).
///
/// BELOW the floor, such a size passes the generic tuning check but
/// `should_multipart` rejects it against the backend's minimum and silently
/// falls back to a whole-object PUT, so the recorded tuning never applies.
/// ABOVE the ceiling is worse and was missed on the first pass (review): S3's
/// maximum part is 5 GiB, and a larger one is not refused by anything
/// downstream — `read_part_bytes` goes on to ALLOCATE a buffer of that size
/// and `upload_part` then sends a request the service cannot accept, so an
/// accepted knob becomes an out-of-memory or a runtime upload failure rather
/// than a configuration error. Both ends are refused at construction rather
/// than accepted and discovered.
///
/// The legacy `multipart_part_size_bytes` keeps its `should_multipart`
/// fallback (mock and local backends use sub-floor sizes in tests); only the
/// tuning knob is strict.
fn reject_out_of_range_part_size(
    floor: u64,
    ceiling: u64,
    tuning: &vtop_core::config::EgressTuning,
    transport_name: &str,
) -> Result<(), VtopError> {
    if let Some(size) = tuning.part_size_bytes {
        if size < floor {
            return Err(VtopError::Config(format!(
                "upload.transports.{transport_name}.part_size_bytes = {size} is below S3's \
                 {floor}-byte minimum for a non-final multipart part; a smaller size cannot be \
                 honoured (it would fall back to a whole-object PUT)"
            )));
        }
        if size > ceiling {
            return Err(VtopError::Config(format!(
                "upload.transports.{transport_name}.part_size_bytes = {size} is above S3's \
                 {ceiling}-byte maximum for a multipart part; the part would be allocated in \
                 full and then refused by the service, so it is refused here instead"
            )));
        }
    }
    Ok(())
}

impl S3NativeBackend {
    /// S3's hard minimum for a non-final multipart part (#482).
    pub const fn part_size_floor() -> u64 {
        5 * 1024 * 1024
    }

    /// S3's hard maximum for a single multipart part (#480, review). Above it
    /// the part is allocated in full before the service refuses the request.
    pub const fn part_size_ceiling() -> u64 {
        5 * 1024 * 1024 * 1024
    }

    /// Build the backend from config, resolving credentials via the standard
    /// AWS credential chain (env vars, profile, instance metadata).
    pub async fn new(cfg: &S3NativeConfig) -> Result<Self, VtopError> {
        // Resolve the transport FIRST (#479): an unknown name fails closed here,
        // and the resolved transport drives both the scheme policy below and the
        // client-config install just before construction. It is shared rather
        // than owned because the request-time gate outlives this function and
        // must consult the SAME transport the doors here consulted (#488,
        // review) — two answers to "does this transport carry that scheme" is
        // one answer too many.
        let transport: Arc<dyn EgressTransport> = TransportRegistry::with_builtins()
            .resolve(&cfg.transport)?
            .into();
        // A tuning knob the resolved transport cannot honour is refused HERE,
        // naming the field and the transport (#480) — never silently ignored.
        // The shape is symmetric across paths; what each path can honour is not.
        reject_unsupported_tuning(transport.as_ref(), &cfg.tuning)?;
        reject_out_of_range_part_size(
            Self::part_size_floor(),
            Self::part_size_ceiling(),
            &cfg.tuning,
            &cfg.transport,
        )?;
        if !cfg.verify_tls {
            tracing::warn!(
                "verify_tls is false: plaintext endpoints are permitted (lab use only). \
                 Certificate verification for https:// endpoints is NOT disabled - \
                 private CAs must be in the system trust store"
            );
        }

        // The SDK resolves endpoints from its OWN configuration too —
        // AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3 and the shared config file —
        // and those must not bypass the policy the explicit config obeys.
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cfg.region.clone()));
        if let Some(ep) = &cfg.endpoint_url {
            loader = loader.endpoint_url(ep.clone());
        }
        let shared = loader.load().await;
        // EVERY endpoint source, in ONE call (#488, review). Three separate
        // calls were three things that could be deleted individually while the
        // tests stayed green; this one cannot be removed without removing all
        // validation, which no test survives. It runs after the loader because
        // the SDK-resolved endpoint is the last source to become knowable —
        // the loader only resolves configuration, it sends nothing, so nothing
        // has left the process before the policy is applied.
        validate_every_endpoint_source(
            cfg.endpoint_url.as_deref(),
            |var| std::env::var(var).ok(),
            shared.endpoint_url(),
            cfg.verify_tls,
            transport.as_ref(),
        )?;

        // THE SEAM (#479): the transport installs itself into the client config
        // here, after endpoint validation and before the client is built. The
        // default tcp_tls install is a strict no-op, so the shipping client is
        // `Builder::from(&shared).force_path_style(..)` carrying the #488 scheme
        // gate and nothing else — the seam itself contributes no difference.
        let s3_conf = install_transport_then_the_last_gate(
            aws_sdk_s3::config::Builder::from(&shared).force_path_style(cfg.force_path_style),
            &transport,
            cfg.verify_tls,
        )?
        .build();

        Ok(Self {
            client: Client::from_conf(s3_conf),
        })
    }

    async fn put(
        &self,
        local_path: &Path,
        uri: &str,
        content_type: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<Option<String>, VtopError> {
        let (bucket, key) = parse_s3_uri(uri)?;
        let body = ByteStream::from_path(local_path)
            .await
            .map_err(|e| VtopError::Upload(format!("reading {}: {e}", local_path.display())))?;

        let mut req = self
            .client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .content_type(content_type)
            .body(body);

        if let Some(c) = checksum {
            // Always retain the hex digest as user metadata (any algorithm),
            // for tooling and verification of objects from older writers.
            req = req.metadata(CHECKSUM_META_KEY, c.hex);
            // For SHA-256 only, also request server-validated integrity: S3
            // recomputes SHA-256 over the body and rejects the upload
            // (BadDigest) if it does not match, so in-transit corruption fails
            // the PUT itself. (BLAKE3 is 32 bytes too, so it MUST NOT be sent
            // here — S3 would recompute SHA-256 and reject it.)
            if c.is_sha256() {
                if let Some(b64) = hex_to_b64_sha256(c.hex) {
                    req = req.checksum_sha256(b64);
                }
            }
        }

        let out = req
            .send()
            .await
            .map_err(|e| sdk_failure("put_object", uri, e))?;
        tracing::info!(uri, "object uploaded via s3_native");
        // A suspended-versioning bucket reports the literal version "null",
        // which later writes overwrite — it is not an immutable pin. Surface
        // it as unversioned so it is never persisted as one (#135).
        Ok(out
            .version_id()
            .filter(|id| *id != "null")
            .map(str::to_owned))
    }

    /// Recompute a digest from the bytes returned by S3 without buffering the
    /// full object. Used for algorithms S3 does not compute natively.
    async fn digest_stored_body(
        &self,
        object_uri: &str,
        algo: ChecksumAlgorithm,
        max_bytes: u64,
    ) -> Result<(String, u64), VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| sdk_failure("get_object", object_uri, e))?;
        // BOUNDED (review): a same-size adversary is caught by the digest,
        // but an OVERSIZED replacement must not be hashed whole — read at
        // most max_bytes (the caller passes expected_size + 1), exactly as
        // verify_reader_content does, so a huge object cannot be pulled
        // down before it is rejected on size.
        use tokio::io::AsyncReadExt;
        digest_reader(algo, out.body.into_async_read().take(max_bytes))
            .await?
            .ok_or_else(|| VtopError::Upload("cannot hash with disabled checksum mode".into()))
    }
}

/// The engine's error for a failed SDK call, telling a throttle apart (#102).
///
/// The SDK has already retried a throttle with its own backoff by the time
/// one reaches here, so what arrives is "still overloaded after retries" —
/// exactly the signal a concurrency controller wants and a same-rate retry
/// would only worsen. Classified on the wire facts the SDK keeps: the HTTP
/// status (429, or 503 as S3 spells `SlowDown`) and the error code.
fn sdk_failure<E>(operation: &str, target: &str, error: SdkError<E, HttpResponse>) -> VtopError
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    // The wire facts, from whichever variant kept them: a service error has
    // a status and usually a code; a response the SDK could not parse as an
    // S3 error document — a proxy's HTML, an empty body — still has its
    // status (review), and a 429/503 there is the same throttle.
    let (status, code) = match &error {
        SdkError::ServiceError(context) => (
            Some(context.raw().status().as_u16()),
            context.err().code().map(str::to_owned),
        ),
        SdkError::ResponseError(context) => (Some(context.raw().status().as_u16()), None),
        _ => (None, None),
    };
    let detail = match (&error, status) {
        (SdkError::ServiceError(context), Some(status)) => format!(
            "{operation} {target}: {} (http {status}{})",
            context.err(),
            code.as_deref()
                .map(|code| format!(", {code}"))
                .unwrap_or_default()
        ),
        (_, Some(status)) => format!(
            "{operation} {target}: unparseable response (http {status}): {}",
            DisplayErrorContext(&error)
        ),
        _ => format!("{operation} {target}: {}", DisplayErrorContext(&error)),
    };
    let throttled = status.is_some_and(crate::base::is_throttle_status)
        || code.as_deref().is_some_and(crate::base::is_throttle_code);
    if throttled {
        VtopError::UploadThrottled(detail)
    } else {
        VtopError::Upload(detail)
    }
}

#[async_trait]
impl UploadBackend for S3NativeBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredObject, VtopError> {
        let version_id = self
            .put(local_path, object_uri, "application/octet-stream", checksum)
            .await?;
        Ok(StoredObject { version_id })
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        let version_id = self
            .put(local_path, manifest_uri, "application/json", checksum)
            .await?;
        Ok(StoredManifest { version_id })
    }

    async fn get_manifest_pinned(
        &self,
        manifest_uri: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        self.get_object_pinned(manifest_uri, version_id, max_bytes)
            .await
    }

    async fn get_object_pinned(
        &self,
        object_uri: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .version_id(version_id)
            .send()
            .await
            .map_err(|e| {
                sdk_failure(
                    "get_object",
                    &format!("{object_uri} (version {version_id})"),
                    e,
                )
            })?;
        if out
            .content_length()
            .is_some_and(|size| size < 0 || size as u64 > max_bytes as u64)
        {
            return Err(VtopError::Upload(format!(
                "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
            )));
        }
        read_bounded(out.body.into_async_read(), max_bytes, object_uri).await
    }

    fn supports_object_versions(&self) -> bool {
        true
    }

    async fn verify_bucket_versioning(&self, bucket: &str) -> Result<(), VtopError> {
        let out = self
            .client
            .get_bucket_versioning()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| sdk_failure("get_bucket_versioning", bucket, e))?;
        match out.status() {
            Some(BucketVersioningStatus::Enabled) => Ok(()),
            other => Err(VtopError::Upload(format!(
                "bucket {bucket} does not have versioning enabled (status: {other:?}); \
                 the hardened manifest profile requires it"
            ))),
        }
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| sdk_failure("get_object", object_uri, e))?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| VtopError::Upload(format!("get_object body {object_uri}: {e}")))?;
        Ok(bytes.into_bytes().to_vec())
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| sdk_failure("get_object", object_uri, e))?;
        if out
            .content_length()
            .is_some_and(|size| size < 0 || size as u64 > max_bytes as u64)
        {
            return Err(VtopError::Upload(format!(
                "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
            )));
        }
        read_bounded(out.body.into_async_read(), max_bytes, object_uri).await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .map_err(|e| sdk_failure("head_object", object_uri, e))?;

        // Only expose the checksum S3 itself computed over the stored body.
        // x-amz-meta-vtop-checksum is written by the uploader and therefore
        // cannot establish content integrity (#64).
        let checksum_sha256 = out.checksum_sha256().and_then(b64_to_hex_sha256);

        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: out.content_length().map(|v| v as u64),
            etag: out.e_tag().map(|s| s.to_string()),
            checksum_sha256,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let head = self.head_object(object_uri).await?;

        if let Some(sz) = head.size_bytes {
            if sz != expected_size {
                return Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {sz}"
                )));
            }
        } else {
            return Ok(VerificationResult::failed("object size unavailable"));
        }

        // Checksums disabled: size + existence is all we can confirm.
        let Some(expected) = expected else {
            return Ok(VerificationResult::limited(
                "object present and size matches (checksums disabled)",
            ));
        };

        let algo = match expected.algorithm.parse::<ChecksumAlgorithm>() {
            Ok(ChecksumAlgorithm::None) => {
                return Ok(VerificationResult::failed(
                    "checksum value supplied with disabled algorithm",
                ))
            }
            Ok(algo) => algo,
            Err(e) => return Ok(VerificationResult::failed(e)),
        };

        match algo {
            ChecksumAlgorithm::Sha256 => {
                let judged = crate::base::judge_service_sha256(
                    head.checksum_sha256.as_deref(),
                    expected.hex,
                );
                if !judged.backend_limited {
                    return Ok(judged);
                }
                // NO SERVICE CHECKSUM IS NOT NO EVIDENCE (#482): S3 returns
                // no whole-object SHA-256 for a multipart upload — only a
                // composite the head decoding rejects — and the limited
                // answer here used to fail every above-threshold batch
                // under require_strong_verification. The stored body is
                // still there to hash: one bounded read-back, the same
                // strong evidence the BLAKE3 arm has always used, at the
                // cost of a GET the measurement half of #482 publishes.
                // Which path ran stays observable in the message.
                let (actual, bytes_read) = self
                    .digest_stored_body(object_uri, algo, expected_size.saturating_add(1))
                    .await?;
                if bytes_read != expected_size {
                    return Ok(VerificationResult::failed(format!(
                        "size mismatch: expected {expected_size}, read {bytes_read} stored bytes"
                    )));
                }
                if actual.eq_ignore_ascii_case(expected.hex) {
                    Ok(VerificationResult::passed(
                        "stored content SHA-256 verified by read-back (no service \
                         whole-object checksum; multipart or unchecked upload)",
                    ))
                } else {
                    Ok(VerificationResult::failed(
                        "stored content SHA-256 mismatch on read-back",
                    ))
                }
            }
            ChecksumAlgorithm::Blake3 => {
                let (actual, bytes_read) = self
                    .digest_stored_body(object_uri, algo, expected_size.saturating_add(1))
                    .await?;
                if bytes_read != expected_size {
                    return Ok(VerificationResult::failed(format!(
                        "size mismatch: expected {expected_size}, read {bytes_read} stored bytes"
                    )));
                }
                if actual.eq_ignore_ascii_case(expected.hex) {
                    Ok(VerificationResult::passed("stored content BLAKE3 verified"))
                } else {
                    Ok(VerificationResult::failed("stored content BLAKE3 mismatch"))
                }
            }
            ChecksumAlgorithm::None => unreachable!("handled above"),
        }
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        self.client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| sdk_failure("delete_object", object_uri, e))?;
        Ok(())
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), VtopError> {
        // Idempotent: treat "already exists / already owned by you" as success.
        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                tracing::info!(bucket, "bucket created");
                Ok(())
            }
            Err(e) => {
                // Idempotence first, on the service error's own words; then
                // the same classification as every other request (review),
                // so a throttled CreateBucket is a throttle, not a mystery.
                let already_owned = matches!(&e, SdkError::ServiceError(context) if {
                    let msg = context.err().to_string().to_lowercase();
                    msg.contains("alreadyexists")
                        || msg.contains("already exists")
                        || msg.contains("alreadyownedbyyou")
                        || msg.contains("already owned")
                        || msg.contains("bucketalreadyownedbyyou")
                });
                if already_owned {
                    Ok(())
                } else {
                    Err(sdk_failure("create_bucket", bucket, e))
                }
            }
        }
    }

    fn backend_name(&self) -> &'static str {
        "s3_native"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        true
    }

    fn min_part_size_bytes(&self) -> u64 {
        Self::part_size_floor()
    }

    async fn create_multipart_upload(
        &self,
        object_uri: &str,
        content_type: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<String, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let mut req = self
            .client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .content_type(content_type);
        if let Some(c) = checksum {
            // Inventory metadata only — never strong evidence. BLAKE3 must not
            // be sent as x-amz-checksum-sha256.
            req = req.metadata(CHECKSUM_META_KEY, c.hex);
        }
        let out = req
            .send()
            .await
            .map_err(|e| sdk_failure("create_multipart_upload", object_uri, e))?;
        out.upload_id().map(str::to_owned).ok_or_else(|| {
            VtopError::Upload(format!(
                "create_multipart_upload {object_uri}: service returned no upload id"
            ))
        })
    }

    async fn upload_part(
        &self,
        object_uri: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<UploadedPart, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let out = self
            .client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| sdk_failure("upload_part", &format!("{object_uri}#{part_number}"), e))?;
        let etag = out.e_tag().map(str::to_owned).ok_or_else(|| {
            VtopError::Upload(format!(
                "upload_part {object_uri}#{part_number}: service returned no etag"
            ))
        })?;
        Ok(UploadedPart { part_number, etag })
    }

    async fn complete_multipart_upload(
        &self,
        object_uri: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<StoredObject, VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        let mut ordered = parts.to_vec();
        ordered.sort_by_key(|p| p.part_number);
        let completed_parts: Vec<CompletedPart> = ordered
            .iter()
            .map(|p| {
                CompletedPart::builder()
                    .part_number(p.part_number as i32)
                    .e_tag(&p.etag)
                    .build()
            })
            .collect();
        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        let out = self
            .client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(multipart)
            .send()
            .await
            .map_err(|e| sdk_failure("complete_multipart_upload", object_uri, e))?;
        let version_id = out
            .version_id()
            .filter(|id| *id != "null")
            .map(str::to_owned);
        tracing::info!(uri = object_uri, "object uploaded via s3_native multipart");
        Ok(StoredObject { version_id })
    }

    async fn abort_multipart_upload(
        &self,
        object_uri: &str,
        upload_id: &str,
    ) -> Result<(), VtopError> {
        let (bucket, key) = parse_s3_uri(object_uri)?;
        self.client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| sdk_failure("abort_multipart_upload", object_uri, e))?;
        Ok(())
    }
}

/// Build an [`S3NativeConfig`] from a [`vtop_core::config::UploadConfig`] and
/// the standard VTOP environment overrides.
pub fn config_from_upload(upload: &vtop_core::config::UploadConfig) -> S3NativeConfig {
    let endpoint_url = std::env::var("VTOP_S3_ENDPOINT_URL")
        .ok()
        .or_else(|| upload.endpoint_url.clone());
    let force_path_style = std::env::var("VTOP_S3_FORCE_PATH_STYLE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(upload.force_path_style);
    let verify_tls = std::env::var("VTOP_S3_VERIFY_TLS")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(upload.verify_tls);
    // VTOP_S3_TRANSPORT follows the same VTOP_S3_* precedent as the fields
    // above (#479), resolved through the SAME helper VtopConfig::validate uses
    // so the two cannot diverge: a non-empty override is trimmed and wins, so a
    // " tcp_tls " that passed load-time validation resolves to the same trimmed
    // name here rather than being rejected at backend construction (review).
    // The result is still resolved against the registry in `new`, so an
    // override typo fails closed there just as a config typo does.
    let transport = vtop_core::config::resolve_effective_transport(
        &upload.transport,
        std::env::var("VTOP_S3_TRANSPORT").ok().as_deref(),
    );
    // The tuning for the RESOLVED transport (#480): tuning follows the wire the
    // engine will actually use, so an override that changes the transport also
    // selects that transport's block. Absent, the default is all-None tuning.
    let tuning = upload
        .transports
        .get(&transport)
        .cloned()
        .unwrap_or_default();

    S3NativeConfig {
        region: upload.region.clone(),
        endpoint_url,
        force_path_style,
        verify_tls,
        transport,
        tuning,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_core::checksum::sha256_bytes;

    #[test]
    fn hex_b64_round_trips() {
        let hex = sha256_bytes(b"vtop object body");
        let b64 = hex_to_b64_sha256(&hex).expect("hex -> b64");
        let back = b64_to_hex_sha256(&b64).expect("b64 -> hex");
        assert_eq!(back, hex);
    }

    #[test]
    fn known_empty_string_vector() {
        // SHA-256("") in hex and the base64 S3 reports for it.
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(
            hex_to_b64_sha256(hex).unwrap(),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }

    #[test]
    fn rejects_non_sha256_lengths() {
        // Not 32 bytes once decoded -> no conversion (avoids sending a bogus
        // checksum that S3 would reject opaquely).
        assert!(hex_to_b64_sha256("abcd").is_none());
        assert!(hex_to_b64_sha256("zz").is_none()); // not valid hex
        assert!(b64_to_hex_sha256("not-base64!!").is_none());
        assert!(b64_to_hex_sha256(&B64.encode([0u8; 16])).is_none()); // 16 bytes
    }

    /// #75: verify_tls=true must REJECT plaintext endpoints, not warn past
    /// them; verify_tls=false is the explicit lab opt-out.
    #[test]
    fn plaintext_endpoint_policy() {
        // The hole this closes: verify_tls promised encryption but plaintext
        // was accepted anyway.
        // The default transport carries http/https, so this exercises the same
        // plaintext policy as before the seam (#479).
        let t = transport::TcpTlsTransport;
        let err = validate_endpoint_scheme(Some("http://minio:9000"), true, &t)
            .expect_err("plaintext + verify_tls=true must fail");
        assert!(matches!(err, VtopError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("http://minio:9000"),
            "names the endpoint: {msg}"
        );
        assert!(msg.contains("verify_tls"), "names the fix: {msg}");

        // Explicit lab opt-out still works (the compose lab is plaintext).
        assert!(validate_endpoint_scheme(Some("http://minio:9000"), false, &t).is_ok());
        // Scheme check is case-insensitive and trims whitespace.
        assert!(validate_endpoint_scheme(Some("  HTTP://minio:9000"), true, &t).is_err());
        // https endpoints pass under either setting.
        assert!(validate_endpoint_scheme(Some("https://s3.example.com"), true, &t).is_ok());
        assert!(validate_endpoint_scheme(Some("https://s3.example.com"), false, &t).is_ok());
        // No custom endpoint = default AWS https endpoints.
        assert!(validate_endpoint_scheme(None, true, &t).is_ok());
    }

    #[test]
    fn plaintext_is_refused_on_every_endpoint_source_for_every_transport() {
        // The cross product, because the scheme policy is only as good as its
        // WEAKEST call site (#488). `validate_endpoint_scheme` is applied to
        // the explicit config value, to both endpoint environment variables,
        // and to whatever the SDK finally resolved — four doors, and a policy
        // enforced on three of them is not a policy. The env doors go through
        // `validate_env_endpoint_schemes` with an injected reader, so this can
        // enumerate them without racing every other test in the process over
        // real environment variables.
        use super::TransportRegistry as Reg;
        let registry = Reg::with_builtins();
        // Spellings a careless endpoint could arrive in. Case and surrounding
        // whitespace must not be a way past the check.
        let plaintext = [
            "http://minio:9000",
            "HTTP://minio:9000",
            "  http://minio:9000  ",
        ];

        for name in registry.names() {
            let transport = registry.resolve(&name).expect("a registered name resolves");
            for ep in plaintext {
                // Door 1: the explicit config value.
                let err = validate_endpoint_scheme(Some(ep), true, transport.as_ref())
                    .expect_err("plaintext with verify_tls must be refused at the config door");
                assert!(
                    err.to_string().contains("plaintext"),
                    "the refusal must say what it objected to: {err}"
                );

                // ALL FOUR DOORS, through the ONE unit the constructor calls
                // (review). Each source in turn, with the others clean, so a
                // policy that stopped consulting any single one fails here —
                // and because `new` makes exactly this call, a deletion there
                // removes every door at once rather than one quietly.
                for (label, explicit, env_var, resolved) in [
                    ("explicit config", Some(ep), None, None),
                    (
                        "AWS_ENDPOINT_URL_S3",
                        None,
                        Some("AWS_ENDPOINT_URL_S3"),
                        None,
                    ),
                    ("AWS_ENDPOINT_URL", None, Some("AWS_ENDPOINT_URL"), None),
                    ("the SDK-resolved endpoint", None, None, Some(ep)),
                ] {
                    let err = validate_every_endpoint_source(
                        explicit,
                        |var| env_var.filter(|v| *v == var).map(|_| ep.to_string()),
                        resolved,
                        true,
                        transport.as_ref(),
                    )
                    .expect_err("plaintext must be refused whichever door it arrives through");
                    assert!(
                        err.to_string().contains("plaintext"),
                        "{label} let a plaintext endpoint past with verify_tls: true: {err}"
                    );
                }

                // Doors 2 and 3: the endpoint environment variables. The
                // refusal must name the VARIABLE — "plaintext endpoint
                // refused" sends an operator to their config file when the
                // value came from their shell.
                for var in ENDPOINT_ENV_VARS {
                    let err = validate_env_endpoint_schemes(
                        |v| (v == *var).then(|| ep.to_string()),
                        true,
                        transport.as_ref(),
                    )
                    .expect_err("plaintext from the environment must be refused too");
                    assert!(
                        err.to_string().contains(var),
                        "the refusal must name the source that supplied it, or the operator \
                         looks in the wrong place: {err}"
                    );
                }
            }

            // Door 4 is the SDK-resolved endpoint, validated in `new` with the
            // same function against whatever actually resolved; it shares this
            // implementation, so what is pinned here is that the function
            // refuses. That the CONSTRUCTOR still calls it is a separate
            // claim, and a separate test below makes it for the door that can
            // be reached without the SDK.

            // ... and the deliberate lab opt-out still works, on every
            // transport: verify_tls: false is how the compose lab runs, and a
            // policy that broke it would be discovered by every developer.
            assert!(
                validate_endpoint_scheme(Some("http://minio:9000"), false, transport.as_ref())
                    .is_ok(),
                "verify_tls: false is the explicit plaintext opt-in the lab depends on"
            );
            assert!(
                validate_endpoint_scheme(Some("https://s3.example.com"), true, transport.as_ref())
                    .is_ok(),
                "https must pass on every transport, or nothing can upload"
            );
        }
    }

    #[test]
    fn a_plaintext_request_is_refused_at_the_moment_it_would_be_transmitted() {
        // Enumerating endpoint SOURCES is only ever as complete as the list,
        // and the list was not: a shared-profile [services] section can carry
        // an S3-specific endpoint_url that SdkConfig::endpoint_url() does not
        // expose, because the service config applies it later (review). This
        // is the check that cannot be out-run — it reads the request's own URI
        // immediately before transmission, so it covers that source, a
        // transport that redirected the client, and any source nobody has
        // thought of yet.
        let t = transport::TcpTlsTransport;
        let err = refuse_plaintext_uri("http://minio:9000/bucket/key", true, &t)
            .expect_err("plaintext with verify_tls must never reach the wire");
        assert!(
            err.contains("plaintext") && err.contains("tcp_tls"),
            "the refusal must name what it saw and which transport carried it: {err}"
        );

        // https is the ordinary case and must not be disturbed.
        assert!(refuse_plaintext_uri("https://s3.example.com/b/k", true, &t).is_ok());
        // Case is not a way past it.
        assert!(refuse_plaintext_uri("HTTP://minio:9000/b/k", true, &t).is_err());

        // And the lab's deliberate opt-out still works: verify_tls: false is
        // how the compose stack runs, and refusing here would break the one
        // configuration that asks for plaintext on purpose.
        assert!(
            refuse_plaintext_uri("http://minio:9000/b/k", false, &t).is_ok(),
            "verify_tls: false is the explicit, warned opt-in the lab depends on"
        );
    }

    /// A transport that carries HTTPS and nothing else — the shape an
    /// HTTPS-only or datagram-over-TLS wire would have. It lives in the tests
    /// because no such transport ships yet, and that is the point: the property
    /// it pins is one a transport would weaken, and a test written before the
    /// transport is a test the transport has to pass.
    struct HttpsOnlyTransport;

    impl EgressTransport for HttpsOnlyTransport {
        fn name(&self) -> &str {
            "https_only_test"
        }
        fn install(
            &self,
            builder: aws_sdk_s3::config::Builder,
        ) -> Result<aws_sdk_s3::config::Builder, VtopError> {
            Ok(builder)
        }
        fn permits_scheme(&self, scheme: &str) -> bool {
            scheme == "https"
        }
        fn tuning_support(&self) -> transport::TuningSupport {
            transport::TuningSupport {
                rate_control: false,
                parallelism: true,
                redundancy: false,
            }
        }
    }

    #[test]
    fn a_scheme_the_transport_does_not_carry_is_refused_at_both_doors_whatever_verify_tls_says() {
        // Capability and policy are different questions, and the request-time
        // gate used to answer only the second (review): it checked `http://`
        // against verify_tls and returned the moment verification was off,
        // without ever consulting the resolved transport's `permits_scheme`. So
        // an HTTPS-only transport could be handed a plaintext endpoint under the
        // lab opt-out, and any unadmitted scheme sailed past on every
        // transport — and this is the ONE gate an endpoint from an S3-specific
        // shared-profile [services] section ever meets, because no source
        // validator can see that endpoint at all.
        //
        // Both doors are walked with the same table, because a policy that two
        // doors answer differently is two policies, and the operator meets
        // whichever one is weaker.
        let tcp_tls = transport::TcpTlsTransport;
        let https_only = HttpsOnlyTransport;
        let cases: [(&str, &str, &dyn EgressTransport, bool, bool); 6] = [
            (
                "an unadmitted scheme under the lab opt-out",
                "quic://minio:9000/b/k",
                &tcp_tls,
                false,
                false,
            ),
            (
                "an unadmitted scheme with verification on",
                "quic://minio:9000/b/k",
                &tcp_tls,
                true,
                false,
            ),
            (
                "plaintext handed to an HTTPS-only transport under the lab opt-out",
                "http://minio:9000/b/k",
                &https_only,
                false,
                false,
            ),
            (
                "the compose lab's own plaintext opt-in",
                "http://minio:9000/b/k",
                &tcp_tls,
                false,
                true,
            ),
            (
                "https on an HTTPS-only transport",
                "https://s3.example.com/b/k",
                &https_only,
                true,
                true,
            ),
            (
                "https on the shipping transport",
                "https://s3.example.com/b/k",
                &tcp_tls,
                true,
                true,
            ),
        ];

        for (label, uri, transport, verify_tls, permitted) in cases {
            let at_construction = validate_endpoint_scheme(Some(uri), verify_tls, transport);
            let at_transmit = refuse_plaintext_uri(uri, verify_tls, transport);
            assert_eq!(
                at_construction.is_ok(),
                permitted,
                "[{label}] the construction-time door decided wrongly: {at_construction:?}"
            );
            assert_eq!(
                at_transmit.is_ok(),
                permitted,
                "[{label}] the request-time gate decided wrongly, and for an endpoint only a \
                 shared-profile [services] section supplies it is the only gate there is: \
                 {at_transmit:?}"
            );
        }

        // The refusal names the transport and the scheme, because an operator
        // reading "refused" has to know whether to change the endpoint or the
        // transport — they are different mistakes with different fixes.
        let err = refuse_plaintext_uri("http://minio:9000/b/k", false, &https_only)
            .expect_err("an HTTPS-only transport must not receive a plaintext endpoint");
        assert!(
            err.contains("https_only_test") && err.contains("http://"),
            "the refusal must name the transport and the scheme it would not carry: {err}"
        );

        // A URI that names no wire is not refused for a wire it never named:
        // the scheme spelling is strict, so a relative URI whose path contains
        // "://" is not read as a scheme. Refusing those would refuse every
        // request the moment the SDK handed us a path-only URI.
        assert!(
            refuse_plaintext_uri("/bucket/a://b", true, &tcp_tls).is_ok(),
            "a path that merely contains \"://\" carries no scheme, and must not be judged \
             as though it did"
        );
    }

    /// What a transport's `install` does to the builder it is handed.
    type InstallShape = fn(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder;

    /// A transport whose only interesting behaviour is what its `install` does
    /// to the builder it is handed. Both shapes exercised below are permitted
    /// by the contract — `install` takes a `Builder` by value and returns one —
    /// and both discard whatever was already on it.
    struct DiscardingTransport {
        install_fn: InstallShape,
    }

    impl EgressTransport for DiscardingTransport {
        fn name(&self) -> &str {
            "discarding_test"
        }
        fn install(
            &self,
            builder: aws_sdk_s3::config::Builder,
        ) -> Result<aws_sdk_s3::config::Builder, VtopError> {
            Ok((self.install_fn)(builder))
        }
        fn permits_scheme(&self, _scheme: &str) -> bool {
            true
        }
        fn tuning_support(&self) -> transport::TuningSupport {
            transport::TuningSupport {
                rate_control: false,
                parallelism: true,
                redundancy: false,
            }
        }
    }

    #[test]
    fn a_transport_cannot_discard_the_last_gate_by_replacing_the_builder() {
        // The gate used to be installed BEFORE `transport.install`, under a
        // comment claiming that ordering stopped a transport removing it. It
        // was backwards (review). `install` takes a `Builder` by value and
        // returns one, so returning a fresh builder is a CONFORMING
        // implementation, not a misbehaving one — and `set_interceptors`
        // replaces the list even on the builder that was handed over. Either
        // way the gate went with it, and for an endpoint only a shared-profile
        // [services] section supplies — which no construction-time door can
        // see — that gate is the only one there is, so a transport-introduced
        // plaintext endpoint would have reached the wire.
        //
        // Read through `Debug`, because the built config exposes no public
        // accessor for its interceptors and the property under test is
        // "survived into the config the client is built from". The existing
        // identity test reads the same rendering for the same reason.
        let marker = "RefusePlaintextTransmit";

        // The shipping transport first: the reordering must not have cost the
        // gate on the path every real run takes.
        let shipping: Arc<dyn EgressTransport> = Arc::new(transport::TcpTlsTransport);
        let built = install_transport_then_the_last_gate(
            aws_sdk_s3::config::Builder::new().force_path_style(true),
            &shipping,
            true,
        )
        .expect("tcp_tls install never fails")
        .build();
        let rendered = format!("{built:?}");
        assert!(
            rendered.contains(marker),
            "the default transport's client must carry the last gate, or every ordinary run \
             transmits with no scheme policy at all"
        );
        // ... and it must be PERMANENT. An ordinary interceptor consults
        // `DisableInterceptor<T>` in the config bag before every request, and a
        // scheme policy with a runtime off switch is one some unrelated config
        // change can silence. Nothing in tree can reach that switch today, but
        // that is a fact about two other crates' visibility, not a property of
        // this gate.
        let from_gate = rendered
            .split_once(marker)
            .expect("the gate was just asserted present")
            .1;
        assert!(
            from_gate
                .split_once("permanent: ")
                .is_some_and(|(_, rest)| rest.starts_with("true")),
            "the last gate must be registered permanent, so no config-bag entry can switch \
             the scheme policy off for a request: {rendered}"
        );

        let shapes: [(&str, InstallShape); 2] = [
            ("install returns a fresh builder", |_handed| {
                aws_sdk_s3::config::Builder::new()
            }),
            (
                "install clears the interceptor list it was handed",
                |handed| {
                    let mut kept = handed;
                    kept.set_interceptors(
                        std::iter::empty::<aws_sdk_s3::config::SharedInterceptor>(),
                    );
                    kept
                },
            ),
        ];

        for (label, install_fn) in shapes {
            let transport: Arc<dyn EgressTransport> = Arc::new(DiscardingTransport { install_fn });
            let gated = install_transport_then_the_last_gate(
                aws_sdk_s3::config::Builder::new().force_path_style(true),
                &transport,
                true,
            )
            .expect("these installs never fail")
            .build();
            assert!(
                format!("{gated:?}").contains(marker),
                "[{label}] the last gate must survive the transport's install, because it is \
                 the only check a shared-profile [services] endpoint ever meets"
            );

            // Proof the assertion above is not a decoration: the ordering it
            // replaced loses the gate on this very transport. If this stops
            // holding, the case has stopped being reachable and the assertion
            // above has stopped proving anything.
            let old_ordering = transport
                .install(
                    aws_sdk_s3::config::Builder::new().interceptor(RefusePlaintextTransmit {
                        verify_tls: true,
                        transport: Arc::clone(&transport),
                    }),
                )
                .expect("these installs never fail")
                .build();
            assert!(
                !format!("{old_ordering:?}").contains(marker),
                "[{label}] this case must still DISCARD a gate installed before the transport, \
                 or it no longer exercises the defect the ordering above exists to prevent"
            );
        }
    }

    /// The `EgressTransport` body with whole-line comments removed, ready to be
    /// read as code.
    ///
    /// The surface guard reads source text, so it has to be honest about what
    /// it can read: it drops comments that occupy a WHOLE line, and refuses to
    /// go on if a `//` or `/*` survives that. A trailing comment — or a `//`
    /// inside a literal — would let a text scan mistake prose for code or code
    /// for prose, and a guard whose whole job is to see everything in this body
    /// must fail rather than read part of it.
    fn trait_body_code(body: &str) -> String {
        let code = body
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.contains("//") && !code.contains("/*"),
            "a comment shares a line with code in the EgressTransport declaration, and this \
             guard strips whole-line comments only — so a trailing one could hide a \
             declaration from it, or invent one. Put the comment on its own line, or replace \
             this guard with something that expands macros"
        );
        code
    }

    /// The first macro invocation in a trait body, rendered as a reader would
    /// recognise it, or `None` when there is none.
    ///
    /// LAYOUT IS NOT THE QUESTION (review). The first version of this check
    /// recognised only invocations whose `!` and closing delimiter landed on
    /// the same line, so `transport_methods!(\n);` matched neither line and
    /// sailed past both this check and the `fn` parser it exists to protect —
    /// the guard was green for precisely the construct it was written to stop,
    /// and a newline was the whole of the trick. What identifies an invocation
    /// is the TOKEN SHAPE, a `!` followed by an opening delimiter, and that
    /// shape reads the same however the newlines fall.
    fn macro_invocation_in(code: &str) -> Option<String> {
        let (bang, _) = code
            .char_indices()
            .find(|(i, c)| *c == '!' && code[i + 1..].trim_start().starts_with(['(', '[', '{']))?;
        let head = code[..bang]
            .char_indices()
            .rev()
            .take_while(|(_, c)| c.is_alphanumeric() || *c == '_' || *c == ':')
            .last()
            .map(|(from, _)| &code[from..bang])
            .unwrap_or("");
        let delimiter = code[bang + 1..]
            .trim_start()
            .chars()
            .next()
            .expect("the delimiter the search just matched");
        Some(format!("{head}!{delimiter}"))
    }

    #[test]
    fn a_macro_invocation_the_trait_body_spreads_over_lines_is_still_caught() {
        // The hole this closes: the guard used to recognise a macro only when
        // its `!` and closing delimiter shared a line, so `transport_methods!(`
        // with the `);` on the next line was invisible to it AND to the `fn`
        // parser — a fifth callable method could appear with every assertion
        // still green (review). The guard is only worth having if pressing
        // Enter is not a way around it.
        for spread in [
            "    transport_methods!(\n        expose_evidence,\n    );\n",
            "    transport_methods!\n    (\n    );\n",
            "    transport_methods! {\n        expose_evidence,\n    }\n",
            "    transport_methods![\n        expose_evidence,\n    ];\n",
        ] {
            let found = macro_invocation_in(&trait_body_code(spread));
            assert!(
                found.is_some(),
                "a macro invocation spread over lines went unseen, which is exactly the shape \
                 that hides a method from the surface guard: {spread:?}"
            );
        }
        // The single-line shapes the old check did catch must stay caught.
        assert!(
            macro_invocation_in(&trait_body_code("    transport_methods!();\n")).is_some(),
            "the layout-independent check must still catch what the line-based one did"
        );
        // And it must not fire on the trait as it is actually written: a guard
        // that cried wolf over documentation is a guard someone deletes, and
        // then nothing is watching the surface at all.
        assert_eq!(
            macro_invocation_in(&trait_body_code(
                "    /// Careful! (prose, not a macro invocation)\n    fn name(&self) -> &str;\n"
            )),
            None,
            "a `!` in documentation is not a macro; only the token shape of an invocation is"
        );
    }

    #[test]
    fn a_transport_has_no_route_to_the_evidence_it_would_be_tempted_to_forge() {
        // A fast transport is the component most tempted to report a pass from
        // its own acknowledgements, and #479 placed the seam BELOW
        // UploadBackend so it cannot CALL verification, see its results, or
        // report an outcome. That is what this pins: the trait's surface.
        //
        // It is NOT an adversarial boundary, and the security model says so
        // (review): `install` returns the builder the ONE S3 client is built
        // from, and that client also serves head_object and the stored-body
        // read-back, so a transport written to deceive could influence the
        // requests verification travels over without touching verification
        // code. A registered transport is TRUSTED code — in-tree, reviewed,
        // and listed in BUILTIN_TRANSPORTS, with no plug-in mechanism. This
        // test bounds what a transport can do by ACCIDENT.
        //
        // A source-level check, deliberately: Rust has no runtime reflection
        // over a trait's methods, and the property being defended is exactly
        // "no fifth method appeared". Adding one now fails here, which is the
        // review this is standing in for.
        let source = include_str!("transport.rs");
        let start = source
            .find("pub trait EgressTransport: Send + Sync {")
            .expect("the trait's declaration must be findable, or this test guards nothing");
        let body = &source[start..start + source[start..].find("\n}").expect("trait body ends")];
        let code = trait_body_code(body);
        // A MACRO in the trait body can expand to a method this parser will
        // never see, because there is no `fn` token to find (review). Rather
        // than pretend to parse Rust, the check refuses the construct: a trait
        // item macro here is the one thing that would make this guard a
        // decoration, so its presence fails the test and forces the review
        // this whole assertion exists to force.
        if let Some(invocation) = macro_invocation_in(&code) {
            panic!(
                "the EgressTransport body contains a macro invocation ({invocation}). A macro \
                 can expand to a callable method with no `fn` token for this guard — or the \
                 parser below — to find, which would leave the surface unchecked while every \
                 assertion stayed green. Either write the method out, or replace this guard \
                 with something that expands macros — do not leave a trait whose surface \
                 nothing verifies"
            );
        }
        // Qualifiers count (review). `strip_prefix("fn ")` alone misses a
        // perfectly valid `unsafe fn expose_evidence(&self, ..)`, and the test
        // whose whole purpose is to force review when the surface grows would
        // have stayed green while it grew. Any line declaring a method is a
        // method, however it is spelled.
        let methods: Vec<&str> = code
            .lines()
            .map(str::trim)
            .filter_map(|line| {
                let mut rest = line;
                for qualifier in [
                    "pub(crate) ",
                    "pub ",
                    "default ",
                    "const ",
                    "async ",
                    "unsafe ",
                    "extern \"C\" ",
                    "extern ",
                ] {
                    rest = rest.strip_prefix(qualifier).unwrap_or(rest);
                }
                rest.strip_prefix("fn ")
            })
            .filter_map(|rest| rest.split(['(', '<']).next())
            .collect();
        // And a declaration this parser did NOT recognise must not pass
        // silently: `fn` appearing on a line the parser above skipped means the
        // spelling moved and the check no longer sees the whole surface.
        // Count DECLARATIONS, not occupied lines (review). Two methods written
        // on one physical line — `fn name(&self) -> &str; fn expose(&self);` —
        // make the parser keep the first and this counter see one line, so the
        // totals agree at four and the exact-name assertion passes over a
        // surface that has five. Counting the `fn ` tokens themselves cannot
        // be fooled by layout, which is the same lesson the macro check just
        // learned one round earlier.
        let declarations = code.matches("fn ").count();
        // Proof this counter is not fooled by layout: two declarations on one
        // line count as two, where a line count sees one.
        assert_eq!(
            "fn name(&self) -> &str; fn expose_evidence(&self);"
                .matches("fn ")
                .count(),
            2,
            "the counter must see both declarations on a shared line, or a fifth method \
             written beside an existing one leaves the totals agreeing at four"
        );
        assert_eq!(
            declarations,
            methods.len(),
            "a method is declared in this trait in a form this test cannot parse, so it is \
             not checking the whole surface. Teach it the spelling rather than leaving a \
             method it cannot see"
        );
        assert_eq!(
            methods,
            vec!["name", "install", "permits_scheme", "tuning_support"],
            "the EgressTransport surface changed. A transport is handed the S3 client config \
             builder and nothing else — no route to CALL verify_object, head_object or the \
             manifest read-back, and no way to see their results. If a method belongs here, \
             add it deliberately and update this list; if it hands the transport anything from \
             the verification path, or any way to report an outcome, it does not belong here \
             at all. Note this bounds ACCIDENT, not malice: the client a transport configures \
             is the client verification reads through, so a transport is trusted code — see \
             SECURITY_MODEL.md"
        );
        for forbidden in ["verify_object", "head_object", "manifest"] {
            assert!(
                !body.contains(forbidden),
                "the trait must not mention {forbidden}: the evidence boundary is that a \
                 transport cannot reach it"
            );
        }
    }

    #[test]
    fn a_tuned_part_size_outside_s3s_range_is_refused() {
        // BOTH ends of the range, because they fail differently (#480, review).
        // Below the floor the size is accepted here and then bypassed by
        // should_multipart into a whole-object PUT, so the recorded tuning
        // never applies. Above the ceiling nothing downstream refuses it
        // either: read_part_bytes ALLOCATES a buffer of that size and
        // upload_part then sends a request S3 cannot accept, turning a
        // configuration mistake into an out-of-memory or a runtime failure.
        use vtop_core::config::EgressTuning;
        let floor = super::S3NativeBackend::part_size_floor();
        let ceiling = super::S3NativeBackend::part_size_ceiling();
        let tuned = |size: u64| EgressTuning {
            part_size_bytes: Some(size),
            ..Default::default()
        };

        let err = reject_out_of_range_part_size(floor, ceiling, &tuned(floor - 1), "tcp_tls")
            .expect_err("a sub-floor part size must be refused");
        assert!(err.to_string().contains("part_size_bytes"), "{err}");
        assert!(
            err.to_string().contains("below"),
            "the message must say which end: {err}"
        );

        let err = reject_out_of_range_part_size(floor, ceiling, &tuned(ceiling + 1), "tcp_tls")
            .expect_err("a part size above S3's 5 GiB maximum must be refused, not allocated");
        assert!(err.to_string().contains("part_size_bytes"), "{err}");
        assert!(
            err.to_string().contains("above"),
            "the message must say which end: {err}"
        );

        // Both boundaries themselves are legal.
        assert!(reject_out_of_range_part_size(floor, ceiling, &tuned(floor), "tcp_tls").is_ok());
        assert!(reject_out_of_range_part_size(floor, ceiling, &tuned(ceiling), "tcp_tls").is_ok());
        // Absent is always fine.
        assert!(
            reject_out_of_range_part_size(floor, ceiling, &EgressTuning::default(), "tcp_tls")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn the_constructor_itself_refuses_a_plaintext_endpoint() {
        // The cross-product test above proves the VALIDATOR refuses; it does
        // not prove the constructor still calls it (review). Deleting the
        // validate_endpoint_scheme call from `new` would leave that test
        // green, and the property everyone actually depends on is that
        // building a backend against a plaintext endpoint fails.
        //
        // This drives the real constructor. It refuses before the SDK loader
        // runs, so the test needs no network and no credentials — which is
        // also why it can only cover the explicit door: the environment doors
        // read fixed variable names that a test cannot set without racing
        // every other test in the process, and the SDK-resolved door needs the
        // loader. Those two are covered through the injected reader above and
        // by the call sites, and that difference is stated rather than papered
        // over.
        let cfg = S3NativeConfig {
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://minio:9000".to_string()),
            force_path_style: true,
            verify_tls: true,
            transport: "tcp_tls".to_string(),
            tuning: Default::default(),
        };
        let err = match S3NativeBackend::new(&cfg).await {
            Err(err) => err,
            Ok(_) => panic!(
                "the constructor built a backend against a plaintext endpoint with \
                 verify_tls: true — the scheme policy is no longer wired into `new`, and \
                 every caller that trusts it is now unprotected"
            ),
        };
        assert!(
            err.to_string().contains("plaintext"),
            "the constructor must refuse for the scheme, not incidentally for something \
             else: {err}"
        );
    }

    #[tokio::test]
    async fn a_tier_style_upload_config_has_its_tuning_map_validated() {
        // `vtopctl tier` deserializes a bare UploadConfig and goes straight to
        // build_backend, never constructing a VtopConfig — so while the map
        // checks lived only in VtopConfig::validate, a typo'd transport name
        // was accepted and the copy ran on the legacy multipart defaults, with
        // the requested tuning applied by nothing (review). build_backend now
        // runs the same checks the engine's config load does.
        let cfg: vtop_core::config::UploadConfig = serde_json::from_str(
            r#"{"bucket":"b","backend":"s3_native",
                "transports":{"tcp_tsl":{"part_size_bytes":8388608}}}"#,
        )
        .expect("a typo in a transport NAME is still valid yaml/json");
        let err = match crate::build_backend(&cfg).await {
            Err(err) => err,
            Ok(_) => panic!(
                "an unknown transport in the tuning map must be refused at construction; \
                 a tier copy accepted it and ran on the legacy multipart defaults"
            ),
        };
        assert!(
            err.to_string().contains("tcp_tsl") && err.to_string().contains("tcp_tls"),
            "the refusal must name the typo AND the valid set, or the operator cannot see \
             the difference between them: {err}"
        );
    }

    #[test]
    fn an_unsupported_tuning_knob_fails_at_construction_over_every_transport() {
        // For every registered transport, set each EgressTuning field in turn:
        // it is either honoured (the transport's tuning_support says so) or
        // rejected with an error naming BOTH the field and the transport — no
        // field is ever silently ignored (#480).
        use vtop_core::config::{EgressTuning, RedundancyPolicy};
        for name in transport::TransportRegistry::with_builtins().names() {
            let t = transport::TransportRegistry::with_builtins()
                .resolve(&name)
                .unwrap();
            let support = t.tuning_support();
            let cases: [(&str, EgressTuning, bool); 5] = [
                (
                    "target_rate_bytes_per_second",
                    EgressTuning {
                        target_rate_bytes_per_second: Some(1000),
                        ..Default::default()
                    },
                    support.rate_control,
                ),
                (
                    "max_concurrency",
                    EgressTuning {
                        max_concurrency: Some(4),
                        ..Default::default()
                    },
                    support.parallelism,
                ),
                (
                    "part_size_bytes",
                    EgressTuning {
                        part_size_bytes: Some(8 << 20),
                        ..Default::default()
                    },
                    support.parallelism,
                ),
                (
                    "parts_in_flight",
                    EgressTuning {
                        parts_in_flight: Some(4),
                        ..Default::default()
                    },
                    support.parallelism,
                ),
                (
                    "redundancy",
                    EgressTuning {
                        redundancy: RedundancyPolicy::ReedSolomon,
                        ..Default::default()
                    },
                    support.redundancy,
                ),
            ];
            for (field, tuning, supported) in cases {
                let result = reject_unsupported_tuning(t.as_ref(), &tuning);
                if supported {
                    assert!(
                        result.is_ok(),
                        "[{name}] honours {field}, so it must not be rejected"
                    );
                } else {
                    let err = result.expect_err(&format!(
                        "[{name}] does not support {field}; it must be refused"
                    ));
                    let msg = err.to_string();
                    assert!(
                        msg.contains(field),
                        "[{name}] refusal names the field: {msg}"
                    );
                    assert!(
                        msg.contains(&name),
                        "[{name}] refusal names the transport: {msg}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod throttle_classification {
    use super::*;
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::put_object::PutObjectError;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    fn service_error(status: u16, code: Option<&str>) -> SdkError<PutObjectError, HttpResponse> {
        let mut metadata = ErrorMetadata::builder().message("as the store said it");
        if let Some(code) = code {
            metadata = metadata.code(code);
        }
        SdkError::service_error(
            PutObjectError::generic(metadata.build()),
            HttpResponse::new(StatusCode::try_from(status).unwrap(), SdkBody::empty()),
        )
    }

    /// A `SlowDown`, a bare 503, and a bare 429 are throttles; a missing
    /// key and a refused credential are not (#102).
    #[test]
    fn a_throttle_is_told_apart_by_status_or_code_and_nothing_else() {
        for (status, code) in [
            (503, Some("SlowDown")),
            (503, None),
            (429, None),
            (400, Some("Throttling")),
        ] {
            let error = sdk_failure("put_object", "s3://b/k", service_error(status, code));
            assert!(
                error.is_upload_throttle(),
                "{status} {code:?} must classify as a throttle: {error}"
            );
            assert!(
                error.to_string().contains(&format!("http {status}")),
                "{error}"
            );
        }
        for (status, code) in [
            (404, Some("NoSuchKey")),
            (403, Some("AccessDenied")),
            (500, Some("InternalError")),
        ] {
            let error = sdk_failure("put_object", "s3://b/k", service_error(status, code));
            assert!(!error.is_upload_throttle(), "{status} {code:?}: {error}");
            assert!(matches!(error, VtopError::Upload(_)));
        }
    }

    /// A 503 whose body is not an S3 error document — a proxy's page, an
    /// empty body — reaches the backend as a response error with the raw
    /// status still attached, and is the same throttle (review).
    #[test]
    fn an_unparseable_throttle_response_is_still_a_throttle() {
        fn response_error(status: u16) -> SdkError<PutObjectError, HttpResponse> {
            SdkError::response_error(
                std::io::Error::other("body was HTML, not an S3 error document"),
                HttpResponse::new(
                    StatusCode::try_from(status).unwrap(),
                    SdkBody::from("<html>"),
                ),
            )
        }
        for status in [429, 503] {
            let error = sdk_failure("upload_part", "s3://b/k", response_error(status));
            assert!(error.is_upload_throttle(), "{status}: {error}");
            assert!(
                error.to_string().contains(&format!("http {status}")),
                "{error}"
            );
        }
        let error = sdk_failure("upload_part", "s3://b/k", response_error(502));
        assert!(!error.is_upload_throttle(), "{error}");
    }
}

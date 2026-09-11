//! The egress transport SEAM (#479): the wire an object travels on is a
//! registered choice, not a hardcoded branch in the upload path.
//!
//! The seam sits deliberately BELOW [`crate::base::UploadBackend`], inside
//! [`crate::s3_native::S3NativeBackend::new`], at the single place an HTTP
//! client is installed into the S3 client config. Putting the choice there
//! means a transport never touches `verify_object`, never touches manifests,
//! never changes `backend_name()`, and never changes the `s3://` URIs written
//! into the durable manifest — the evidence rule in [`crate::base`] ("a
//! non-limited success MUST be derived from the stored body or a
//! service-computed digest") is structurally out of a transport's reach rather
//! than merely forbidden to it.
//!
//! Adding a transport is registering an [`EgressTransportFactory`], never
//! editing a `match`. The built-in set of NAMES lives in
//! [`vtop_core::config::BUILTIN_TRANSPORTS`] so the config validator (which
//! runs in `vtop-core`, below the AWS SDK dependency) and this registry read
//! one list — it cannot drift the way `build_backend`'s literal can. This
//! module owns the other half: the client installer each name resolves to.

use aws_sdk_s3::config::Builder;
use std::collections::BTreeMap;
use vtop_core::errors::VtopError;

/// Which of the shared tuning knobs a transport can honour (#479).
///
/// The knobs themselves — a target rate, a width, redundancy — land with the
/// tuning-parity issue; this records, per transport, which ones it will accept,
/// so a tuning shape can later be validated against the wire it will run on
/// rather than silently ignored. The default `tcp_tls` honours the width knob
/// (the AIMD `WidthController` already in the engine) and neither a rate nor
/// forward error correction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TuningSupport {
    /// Honours a target egress rate or ceiling.
    pub rate_control: bool,
    /// Honours a parallelism / width knob.
    pub parallelism: bool,
    /// Applies forward error correction / redundancy.
    pub redundancy: bool,
}

/// A selectable egress transport: how bytes travel, and nothing about what
/// counts as proof.
pub trait EgressTransport: Send + Sync {
    /// The registered name (matches the `upload.transport` config value).
    fn name(&self) -> &str;

    /// Install the transport into the S3 client config builder, returning the
    /// possibly-modified builder. The DEFAULT `tcp_tls` returns it unchanged —
    /// a strict identity — which is what makes "the shipping path is
    /// byte-identical" provable rather than asserted.
    fn install(&self, builder: Builder) -> Result<Builder, VtopError>;

    /// Whether this transport carries an endpoint of the given URL scheme
    /// (already lowercased, e.g. `"https"`). This is consulted by the
    /// endpoint-scheme validator: a transport admitting a new scheme spelling
    /// is the single most likely place a transport change quietly weakens the
    /// `verify_tls` promise, so the admission is explicit here, not implicit.
    fn permits_scheme(&self, scheme: &str) -> bool;

    /// Which shared tuning knobs this transport can honour.
    fn tuning_support(&self) -> TuningSupport;
}

/// Produces an [`EgressTransport`] — the registered unit, so adding a transport
/// is registering a factory rather than editing an upload-path branch.
pub trait EgressTransportFactory: Send + Sync {
    /// The name this factory registers under.
    fn name(&self) -> &str;
    /// Build the transport instance.
    fn build(&self) -> Result<Box<dyn EgressTransport>, VtopError>;
}

/// The default, shipping transport: kernel TCP inside TLS through the AWS SDK.
///
/// Its [`install`](EgressTransport::install) is a STRICT no-op over the S3
/// client config builder — it returns the builder it was handed, unmodified —
/// so a run with the default transport produces exactly the client the engine
/// built before this seam existed.
pub struct TcpTlsTransport;

impl EgressTransport for TcpTlsTransport {
    fn name(&self) -> &str {
        "tcp_tls"
    }

    fn install(&self, builder: Builder) -> Result<Builder, VtopError> {
        // Strict identity. Changing this is changing the shipping path.
        Ok(builder)
    }

    fn permits_scheme(&self, scheme: &str) -> bool {
        // The AWS SDK's HTTP client speaks http and https and nothing else; the
        // plaintext-vs-encrypted decision is the separate `verify_tls` gate.
        scheme == "http" || scheme == "https"
    }

    fn tuning_support(&self) -> TuningSupport {
        TuningSupport {
            rate_control: false,
            parallelism: true,
            redundancy: false,
        }
    }
}

/// Factory for [`TcpTlsTransport`].
struct TcpTlsFactory;

impl EgressTransportFactory for TcpTlsFactory {
    fn name(&self) -> &str {
        "tcp_tls"
    }
    fn build(&self) -> Result<Box<dyn EgressTransport>, VtopError> {
        Ok(Box::new(TcpTlsTransport))
    }
}

/// Name → factory. Seeded with the built-ins; `resolve` fails closed on an
/// unknown name with an error that names the registered set FROM the registry
/// itself, so the message can never drift from the factories it lists.
pub struct TransportRegistry {
    factories: BTreeMap<String, Box<dyn EgressTransportFactory>>,
}

impl TransportRegistry {
    /// A registry seeded with every built-in transport.
    ///
    /// The built-ins are exactly [`vtop_core::config::BUILTIN_TRANSPORTS`] — the
    /// same list the config validator refuses unknown names against — so a name
    /// that validates always resolves here. The `debug_assert` pins that
    /// coupling: a built-in name added to core without a factory here trips it
    /// in tests rather than surfacing as a run-time "unknown transport" for a
    /// value the validator already accepted.
    pub fn with_builtins() -> Self {
        let mut registry = Self {
            factories: BTreeMap::new(),
        };
        registry.register(Box::new(TcpTlsFactory));
        // The built-in factories and BUILTIN_TRANSPORTS must name the SAME set,
        // checked BOTH ways (review): a name in core without a factory here
        // surfaces as a run-time "unknown transport" for a value the validator
        // accepted; a factory here without the core name is rejected by config
        // validation even though the registry and conformance battery include
        // it. Equal length plus one-way containment proves set equality (the
        // map keys are unique).
        debug_assert!(
            registry.factories.len() == vtop_core::config::BUILTIN_TRANSPORTS.len()
                && vtop_core::config::BUILTIN_TRANSPORTS
                    .iter()
                    .all(|name| registry.factories.contains_key(*name)),
            "the built-in registry and BUILTIN_TRANSPORTS must name the same set"
        );
        registry
    }

    /// Register (or replace) a transport factory under its own name.
    pub fn register(&mut self, factory: Box<dyn EgressTransportFactory>) {
        self.factories.insert(factory.name().to_string(), factory);
    }

    /// The registered transport names, sorted (the map is ordered).
    pub fn names(&self) -> Vec<String> {
        self.factories.keys().cloned().collect()
    }

    /// Resolve a transport by name, or fail closed naming the registered set.
    pub fn resolve(&self, name: &str) -> Result<Box<dyn EgressTransport>, VtopError> {
        match self.factories.get(name) {
            Some(factory) => factory.build(),
            None => Err(VtopError::Config(format!(
                "unknown egress transport {name:?}; registered transports are: {}",
                self.names().join(", ")
            ))),
        }
    }
}

impl Default for TransportRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_tls_install_returns_the_builder_unchanged() {
        // The shipping path must not drift: tcp_tls.install is identity, so the
        // client the engine builds with the default transport is the client it
        // built before the seam existed. We prove identity structurally — the
        // same builder value flows out — by building both and comparing the
        // debug rendering of the resulting config, which reflects every field.
        let shared = aws_sdk_s3::config::Builder::new().force_path_style(true);
        let plain = shared.clone().build();
        let through = TcpTlsTransport
            .install(aws_sdk_s3::config::Builder::new().force_path_style(true))
            .expect("tcp_tls install never fails")
            .build();
        assert_eq!(
            format!("{plain:?}"),
            format!("{through:?}"),
            "tcp_tls install must leave the S3 client config byte-identical"
        );
    }

    #[test]
    fn the_builtins_all_resolve() {
        let registry = TransportRegistry::with_builtins();
        for name in vtop_core::config::BUILTIN_TRANSPORTS {
            registry
                .resolve(name)
                .unwrap_or_else(|_| panic!("built-in transport {name} must resolve"));
        }
    }

    #[test]
    fn an_unknown_transport_names_the_registered_set_from_the_registry() {
        // The message is sourced from the live registry, not a literal: register
        // an extra transport and it appears in the refusal, so the list cannot
        // drift from the registered factories the way a hardcoded string can.
        struct FiberFactory;
        impl EgressTransportFactory for FiberFactory {
            fn name(&self) -> &str {
                "fiber_test"
            }
            fn build(&self) -> Result<Box<dyn EgressTransport>, VtopError> {
                Ok(Box::new(TcpTlsTransport))
            }
        }
        let mut registry = TransportRegistry::with_builtins();
        registry.register(Box::new(FiberFactory));
        // A trait object is not Debug, so `expect_err` cannot render the Ok arm;
        // match instead to pull the refusal out on the unknown name.
        let msg = match registry.resolve("nope") {
            Ok(_) => panic!("an unknown transport must fail closed"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("tcp_tls"),
            "refusal must list built-ins: {msg}"
        );
        assert!(
            msg.contains("fiber_test"),
            "refusal must list the registered extra, from the registry: {msg}"
        );
    }

    #[test]
    fn tcp_tls_permits_only_http_and_https() {
        let t = TcpTlsTransport;
        assert!(t.permits_scheme("https"));
        assert!(t.permits_scheme("http"));
        assert!(!t.permits_scheme("quic"));
        assert!(!t.permits_scheme("udp"));
    }
}

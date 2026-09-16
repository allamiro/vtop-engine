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
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
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
    /// Honours the operator's process-wide `upload.max_egress_bytes_per_second`
    /// (#481). Distinct from `rate_control`, which is the per-transport
    /// `target_rate_bytes_per_second` knob: a transport may be unable to aim
    /// at a rate and still be ABLE to be held under a ceiling.
    ///
    /// The [`EgressShaper`] wraps request bodies in `S3NativeBackend`, ABOVE
    /// this seam, so any transport that hands the SDK's body to the wire as the
    /// SDK polls it is shaped without doing anything. A transport that could
    /// not — one that buffered the whole body before sending, say, so the
    /// pacing would happen in front of a queue it drains at line rate —
    /// declares `false`, and a configured cap is then refused at construction
    /// naming the cap and the transport, never accepted and ignored.
    pub egress_ceiling: bool,
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
            // The hyper client polls the body only when its write buffer has
            // room, so pacing the poll paces what reaches the socket — to
            // within hyper's and the kernel's send buffers, which smooth the
            // wire but cannot add bytes the shaper did not admit.
            egress_ceiling: true,
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

// ---------------------------------------------------------------------------
// The egress shaper (#481)
// ---------------------------------------------------------------------------
//
// APPLICATION-LAYER RATE POLICY OVER KERNEL TCP. This paces how fast request
// body bytes are handed to the HTTP client; it infers nothing about the path,
// reacts to no delay or loss, targets no queue, and must not be described as
// anything else. The kernel's TCP stack still decides how those bytes travel.
//
// WHERE IT SITS. Around the `SdkBody` of every object, manifest and part PUT,
// in `S3NativeBackend` — above the transport seam, so no transport has to
// implement it and none can forget to (a transport that cannot be paced this
// way says so through `TuningSupport::egress_ceiling`). Wrapping the body
// rather than adding a trait method keeps the `EgressTransport` surface
// exactly the four reviewed methods (#488).
//
// RETRIES PASS THE SHAPER AGAIN. The orchestrator re-sends a body by calling
// `SdkBody::try_clone`, which invokes the body's rebuild closure. `shape_body`
// wraps through `SdkBody::map_preserve_contents`, whose rebuild closure wraps
// the freshly rebuilt inner body in a NEW `ShapedBody` — so every attempt's
// bytes are admitted, and counted, as they are polled. No attempt can reuse
// an already-admitted buffer, because admission happens at poll time, not at
// wrap time.
//
// BYTES AND LENGTHS ARE UNTOUCHED. The shaper splits a polled chunk into
// zero-copy `Bytes` slices and yields them in order; it never alters,
// reorders or drops a byte. `size_hint` is forwarded exactly, so
// Content-Length is what it was, and `map_preserve_contents` keeps the
// in-memory contents visible through `SdkBody::bytes()` — which is what the
// SDK's flexible-checksum interceptor and SigV4 payload hashing read, so a
// part's checksum header and signature are computed exactly as before. The
// SDK's aws-chunked encoder re-frames the payload into its own fixed-size
// chunks, so our slice boundaries never reach the wire's framing either.

/// Tokens are held in nanobyte units (bytes × 10⁹) so refill is exact integer
/// arithmetic over nanosecond elapsed time — no float drift over a long run.
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// The burst allowance as a fraction of the cap: the bucket holds 1/32 of a
/// second of `R`.
const BUCKET_DEPTH_DIVISOR: u64 = 32;

/// A process-wide token bucket over request-body bytes (#481).
///
/// ONE per constructed backend, shared by every object, manifest and part the
/// backend sends; each `vtopctl` process builds exactly one backend, so this
/// is the process's total upload egress, not a per-object budget.
///
/// THE BOUND, AND WHY THE BUCKET IS THIS DEEP. For a cap `R`:
///
/// * capacity `C = R / 32` bytes — the most that can leave in a burst;
/// * refill rate `R - C` bytes per second;
/// * admission in pieces of at most `C / 2` bytes; a larger chunk is split.
///
/// A token bucket admits at most `capacity + rate × W` in any window of length
/// `W`, because the window can start with a full bucket and gains `rate × W`.
/// For `W` = 1 s that is `C + (R - C) = R`: NO one-second window, aligned or
/// sliding, sees more than `R` bytes admitted. Refilling at the full `R` would
/// allow `R + C`, which is why the refill is `R - C` rather than `R`. The cost
/// is a sustained ceiling of `31/32 R` (≈ 96.9 %), inside the issue's 10 %
/// tolerance on a 60-second total.
///
/// Why half a bucket per piece: a waiter sleeps exactly its token deficit, and
/// a timer that fires late lets tokens keep accruing until the bucket is full.
/// With pieces of `C / 2`, a wake-up can be `C / 2 ÷ (R - C)` ≈ 16 ms late
/// before any refill is lost to the cap, so ordinary timer slop does not
/// silently lower throughput below the ceiling.
///
/// FAIRNESS. The bucket sits behind a `tokio::sync::Mutex`, whose waiters are
/// served first-in, first-out, and the lock is held by exactly one admitter
/// while it sleeps out its deficit. A body therefore takes its turn behind
/// every body that asked before it, and a large body gets one piece per turn,
/// so it cannot starve a small one indefinitely. The fast path (`try_lock`)
/// only succeeds when nobody is queued, so it cannot jump the line.
///
/// CANCELLATION. Tokens are deducted, the counter is bumped and the piece is
/// released in one step with no await in between. Dropping a body while it
/// waits — a request timeout, an aborted upload — drops its queue position or
/// its sleep and takes nothing; there is no reservation to leak.
///
/// NO BUSY-WAIT. A waiter sleeps (tokio time) for exactly the nanoseconds its
/// deficit needs at the refill rate, and is woken only by that timer or by
/// the lock being handed to it.
pub struct EgressShaper {
    rate_bytes_per_second: u64,
    capacity_bytes: u64,
    refill_bytes_per_second: u64,
    max_piece_bytes: u64,
    bucket: tokio::sync::Mutex<Bucket>,
    admitted: AtomicU64,
    /// The exported counter's child, created on first use — so the family
    /// appears only once a shaper exists (see
    /// `vtop_core::telemetry::Metrics::upload_egress_bytes_total`).
    exported: OnceLock<prometheus::IntCounter>,
}

struct Bucket {
    nanotokens: u128,
    last_refill: tokio::time::Instant,
}

impl std::fmt::Debug for EgressShaper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressShaper")
            .field("rate_bytes_per_second", &self.rate_bytes_per_second)
            .field("capacity_bytes", &self.capacity_bytes)
            .field("admitted_bytes", &self.admitted_bytes())
            .finish()
    }
}

impl EgressShaper {
    /// Build a shaper for a cap of `rate_bytes_per_second`, refusing a cap
    /// below [`vtop_core::config::MIN_EGRESS_BYTES_PER_SECOND`] here too:
    /// `S3NativeConfig` can be built without an `UploadConfig` (tests, the
    /// conformance battery), and a zero cap must not become a shaper that
    /// never admits a byte.
    pub fn new(rate_bytes_per_second: u64) -> Result<Arc<Self>, VtopError> {
        let floor = vtop_core::config::MIN_EGRESS_BYTES_PER_SECOND;
        if rate_bytes_per_second < floor {
            return Err(VtopError::Config(format!(
                "upload.max_egress_bytes_per_second = {rate_bytes_per_second} is below the \
                 {floor} bytes/second minimum; omit it to leave egress uncapped"
            )));
        }
        let capacity_bytes = rate_bytes_per_second / BUCKET_DEPTH_DIVISOR;
        let shaper = Arc::new(Self {
            rate_bytes_per_second,
            capacity_bytes,
            refill_bytes_per_second: rate_bytes_per_second - capacity_bytes,
            max_piece_bytes: (capacity_bytes / 2).max(1),
            bucket: tokio::sync::Mutex::new(Bucket {
                nanotokens: u128::from(capacity_bytes) * NANOS_PER_SECOND,
                last_refill: tokio::time::Instant::now(),
            }),
            admitted: AtomicU64::new(0),
            exported: OnceLock::new(),
        });
        // Present from startup when the registry already exists, so a scrape
        // before the first upload reads 0 rather than "no such series".
        shaper.export(0);
        Ok(shaper)
    }

    /// The configured cap.
    pub fn rate_bytes_per_second(&self) -> u64 {
        self.rate_bytes_per_second
    }

    /// The bucket depth: the largest burst, in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// The largest piece a single admission releases.
    pub fn max_piece_bytes(&self) -> u64 {
        self.max_piece_bytes
    }

    /// Bytes admitted so far — the same number `vtop_upload_egress_bytes_total`
    /// exports, readable without a metrics registry.
    pub fn admitted_bytes(&self) -> u64 {
        self.admitted.load(Ordering::Relaxed)
    }

    fn export(&self, bytes: u64) {
        if let Some(counter) = self.exported.get() {
            counter.inc_by(bytes);
        } else if let Some(metrics) = vtop_core::telemetry::metrics() {
            self.exported
                .get_or_init(|| {
                    metrics
                        .upload_egress_bytes_total
                        .with_label_values::<&str>(&[])
                })
                .inc_by(bytes);
        }
    }

    /// Refill, then take `bytes` if the bucket holds them — recording the
    /// admission in the SAME critical section, so the counter can never lead
    /// or lag the tokens. Otherwise the time until it will.
    fn take_or_deficit(&self, bucket: &mut Bucket, bytes: u64) -> Result<(), Duration> {
        let now = tokio::time::Instant::now();
        let elapsed = now.saturating_duration_since(bucket.last_refill).as_nanos();
        let capacity = u128::from(self.capacity_bytes) * NANOS_PER_SECOND;
        let refill = u128::from(self.refill_bytes_per_second);
        bucket.nanotokens = bucket
            .nanotokens
            .saturating_add(elapsed.saturating_mul(refill))
            .min(capacity);
        bucket.last_refill = now;
        let needed = u128::from(bytes) * NANOS_PER_SECOND;
        if bucket.nanotokens >= needed {
            bucket.nanotokens -= needed;
            self.admitted.fetch_add(bytes, Ordering::Relaxed);
            self.export(bytes);
            Ok(())
        } else {
            // Round UP: waking a nanosecond early would only loop once more,
            // but it is a loop, and this path promises not to spin.
            let deficit = needed - bucket.nanotokens;
            let nanos = deficit.div_ceil(refill);
            Err(Duration::from_nanos(
                u64::try_from(nanos).unwrap_or(u64::MAX),
            ))
        }
    }

    /// Admit without waiting, only if nobody is queued and the tokens are
    /// there. Never jumps the queue: `try_lock` fails while a waiter holds or
    /// is being handed the lock.
    fn try_admit(&self, bytes: u64) -> bool {
        match self.bucket.try_lock() {
            Ok(mut bucket) => self.take_or_deficit(&mut bucket, bytes).is_ok(),
            Err(_) => false,
        }
    }

    /// Wait in line, then wait out the deficit, then admit. Cancellation-safe:
    /// nothing is taken until the final, await-free step.
    async fn admit(self: Arc<Self>, bytes: u64) {
        let mut bucket = self.bucket.lock().await;
        loop {
            match self.take_or_deficit(&mut bucket, bytes) {
                Ok(()) => return,
                Err(wait) => tokio::time::sleep(wait).await,
            }
        }
    }
}

/// A pending admission, held so the body stays `Sync` (the SDK requires it):
/// the future is only ever reached through `&mut self`, via `get_mut`, so the
/// mutex is never contended — it exists to lend the future `Sync`.
struct Admission(std::sync::Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>);

/// An `SdkBody` whose data frames are released through an [`EgressShaper`].
struct ShapedBody {
    inner: SdkBody,
    shaper: Arc<EgressShaper>,
    /// Polled from `inner`, not yet admitted.
    unreleased: Bytes,
    /// A piece waiting for its admission.
    waiting: Option<(Bytes, Admission)>,
}

impl http_body::Body for ShapedBody {
    type Data = Bytes;
    type Error = aws_smithy_types::body::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        // Every field is Unpin (SdkBody boxes its streaming inner), so the
        // body can be handled by plain `&mut`.
        let this = self.get_mut();
        loop {
            if let Some((_, admission)) = this.waiting.as_mut() {
                let future = admission
                    .0
                    .get_mut()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if future.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                let (piece, _) = this.waiting.take().expect("checked just above");
                return Poll::Ready(Some(Ok(http_body::Frame::data(piece))));
            }
            if !this.unreleased.is_empty() {
                let len = this
                    .unreleased
                    .len()
                    .min(usize::try_from(this.shaper.max_piece_bytes).unwrap_or(usize::MAX));
                let piece = this.unreleased.split_to(len);
                if this.shaper.try_admit(len as u64) {
                    return Poll::Ready(Some(Ok(http_body::Frame::data(piece))));
                }
                let admission = Box::pin(Arc::clone(&this.shaper).admit(len as u64));
                this.waiting = Some((piece, Admission(std::sync::Mutex::new(admission))));
                continue;
            }
            match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => this.unreleased = data,
                    // Trailers carry no body bytes; pass them through untouched.
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Poll::Ready(other) => return Poll::Ready(other),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.unreleased.is_empty()
            && self.waiting.is_none()
            && http_body::Body::is_end_stream(&self.inner)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        // Exactly the inner body's remaining length plus what we hold: the
        // total is unchanged by shaping, so Content-Length is too.
        let held = (self.unreleased.len()
            + self.waiting.as_ref().map_or(0, |(piece, _)| piece.len())) as u64;
        let inner = http_body::Body::size_hint(&self.inner);
        let mut hint = http_body::SizeHint::new();
        hint.set_lower(inner.lower() + held);
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper + held);
        }
        hint
    }
}

/// Wrap a request body so its bytes pass `shaper` as they are sent (#481).
///
/// Retry-safe and content-preserving — see the module notes above: every
/// rebuild of the body for a re-send is wrapped anew, and the in-memory
/// contents the SDK signs and checksums stay visible.
pub fn shape_body(shaper: &Arc<EgressShaper>, body: SdkBody) -> SdkBody {
    let shaper = Arc::clone(shaper);
    body.map_preserve_contents(move |inner| {
        SdkBody::from_body_1_x(ShapedBody {
            inner,
            shaper: Arc::clone(&shaper),
            unreleased: Bytes::new(),
            waiting: None,
        })
    })
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

#[cfg(test)]
mod shaper_tests {
    use super::*;
    use aws_sdk_s3::primitives::ByteStream;
    use std::sync::Mutex;
    use tokio::time::Instant;

    const MIB: u64 = 1024 * 1024;

    /// A body that yields one shared chunk `count` times — gigabytes of
    /// virtual payload without allocating them, with an exact size hint.
    struct RepeatBody {
        chunk: Bytes,
        remaining: usize,
    }

    impl http_body::Body for RepeatBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(None);
            }
            this.remaining -= 1;
            Poll::Ready(Some(Ok(http_body::Frame::data(this.chunk.clone()))))
        }
        fn size_hint(&self) -> http_body::SizeHint {
            http_body::SizeHint::with_exact((self.chunk.len() * self.remaining) as u64)
        }
    }

    fn repeat_body(chunk_bytes: usize, count: usize) -> SdkBody {
        SdkBody::from_body_1_x(RepeatBody {
            chunk: Bytes::from(vec![0xA5; chunk_bytes]),
            remaining: count,
        })
    }

    type ReleaseLog = Arc<Mutex<Vec<(Instant, u64)>>>;

    /// Drain a body the way the HTTP client does, logging each release.
    async fn drain_logged(mut body: SdkBody, log: ReleaseLog) {
        while let Some(frame) =
            std::future::poll_fn(|cx| http_body::Body::poll_frame(Pin::new(&mut body), cx)).await
        {
            let data = frame
                .expect("the test body does not fail")
                .into_data()
                .expect("data only");
            log.lock()
                .unwrap()
                .push((Instant::now(), data.len() as u64));
        }
    }

    /// The acceptance criterion's shape, in virtual time (#481): for R in
    /// {1, 4, 16} MiB/s, four bodies that together want far more than R keep
    /// the shaper saturated for 60 s. Read from the shaper's OWN account, the
    /// total stays within 10 % of R·60 and no one-second bucket exceeds R —
    /// checked both on the 1 s samples a harness would take AND on every
    /// sliding window ending at a release, which is the stronger property.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_saturated_shaper_holds_every_one_second_window_under_the_cap_for_a_minute() {
        for rate in [MIB, 4 * MIB, 16 * MIB] {
            let shaper = EgressShaper::new(rate).unwrap();
            let log: ReleaseLog = Arc::default();
            let start = Instant::now();
            let mut bodies = Vec::new();
            for _ in 0..4 {
                // 1 MiB chunks: larger than every bucket here, so the split
                // into pieces is exercised, never bypassed.
                let body = shape_body(
                    &shaper,
                    repeat_body(MIB as usize, 20 * (rate / MIB) as usize),
                );
                bodies.push(tokio::spawn(drain_logged(body, Arc::clone(&log))));
            }
            let mut samples = vec![0_u64];
            for second in 1..=60 {
                tokio::time::sleep_until(start + Duration::from_secs(second)).await;
                samples.push(shaper.admitted_bytes());
            }
            for body in &bodies {
                body.abort();
            }

            for (second, pair) in samples.windows(2).enumerate() {
                let in_bucket = pair[1] - pair[0];
                assert!(
                    in_bucket <= rate,
                    "[{rate} B/s] second {second}..{} admitted {in_bucket} bytes, above the cap — \
                     the operator's uplink ceiling was exceeded",
                    second + 1
                );
            }
            let total = samples[60];
            assert!(
                total <= rate * 60 && total * 10 >= rate * 60 * 9,
                "[{rate} B/s] 60 s admitted {total} bytes; the cap must hold within 10 % of \
                 {} or the ceiling is either broken or needlessly throttling",
                rate * 60
            );

            let log = log.lock().unwrap();
            assert!(
                log.iter().all(|(_, len)| *len <= shaper.max_piece_bytes()),
                "[{rate} B/s] a chunk larger than a piece must be split, not admitted whole"
            );
            let mut window_start = 0;
            let mut in_window = 0_u64;
            for (i, (at, len)) in log.iter().enumerate() {
                in_window += len;
                while log[window_start].0 + Duration::from_secs(1) < *at {
                    in_window -= log[window_start].1;
                    window_start += 1;
                }
                assert!(
                    in_window <= rate,
                    "[{rate} B/s] the one-second window ending at release {i} holds {in_window} \
                     bytes, above the cap"
                );
            }
        }
    }

    /// A large body cannot starve a small one (#481): the bucket's lock is
    /// FIFO and a body takes one piece per turn, so a 64 KiB body that starts
    /// two seconds into a 30 MiB upload finishes in a fraction of a second at
    /// 1 MiB/s rather than waiting behind the whole of it.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_small_body_is_not_starved_behind_a_large_one() {
        let shaper = EgressShaper::new(MIB).unwrap();
        let big_log: ReleaseLog = Arc::default();
        let big = tokio::spawn(drain_logged(
            shape_body(&shaper, repeat_body(MIB as usize, 30)),
            Arc::clone(&big_log),
        ));
        tokio::time::sleep(Duration::from_secs(2)).await;
        let started = Instant::now();
        drain_logged(
            shape_body(&shaper, repeat_body(64 * 1024, 1)),
            Arc::default(),
        )
        .await;
        let waited = started.elapsed();
        assert!(
            waited < Duration::from_millis(500),
            "a small body waited {waited:?} behind a large one; a shared cap must share turns, \
             not hand the uplink to whichever body asked first"
        );
        assert!(
            !big.is_finished(),
            "the large body must still be sending, or this proved nothing about sharing"
        );
        big.abort();
    }

    /// Dropping a body while it waits takes nothing and holds nothing (#481):
    /// a timed-out request must not leak tokens, leave the bucket locked, or
    /// count bytes that never left.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_body_dropped_while_waiting_takes_no_tokens_and_counts_nothing() {
        let rate = 64 * 1024; // capacity 2 KiB, pieces of 1 KiB
        let shaper = EgressShaper::new(rate).unwrap();
        assert_eq!(shaper.capacity_bytes(), 2048);
        let mut body = shape_body(&shaper, repeat_body(16 * 1024, 1));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut released = 0;
        loop {
            match http_body::Body::poll_frame(Pin::new(&mut body), &mut cx) {
                Poll::Ready(Some(Ok(frame))) => released += frame.into_data().unwrap().len(),
                Poll::Pending => break,
                other => panic!("the body cannot end before its bucket drains: {other:?}"),
            }
        }
        assert_eq!(released, 2048, "a full bucket releases exactly its depth");
        assert_eq!(shaper.admitted_bytes(), 2048);
        drop(body);

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            shaper.admitted_bytes(),
            2048,
            "the dropped body's waiting piece must not be counted"
        );
        // The next body finds the bucket unlocked and full again: the dropped
        // waiter neither kept the lock nor consumed a reservation.
        let mut next = shape_body(&shaper, repeat_body(16 * 1024, 1));
        let mut immediate = 0;
        while let Poll::Ready(Some(Ok(frame))) =
            http_body::Body::poll_frame(Pin::new(&mut next), &mut cx)
        {
            immediate += frame.into_data().unwrap().len();
        }
        assert_eq!(
            immediate, 2048,
            "a cancelled waiter must leave the bucket exactly as refill left it"
        );
    }

    /// The shaper paces; it never alters (#481). Same bytes, same length, same
    /// digest, and the in-memory contents the SDK signs and checksums still
    /// visible — for an in-memory part body and a file-backed object body.
    #[tokio::test]
    async fn a_shaped_body_carries_exactly_the_bytes_and_length_it_was_given() {
        let payload: Vec<u8> = (0..(3 * MIB as usize + 17))
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object.bin");
        std::fs::write(&path, &payload).unwrap();
        let shaper = EgressShaper::new(64 * MIB).unwrap();

        let in_memory = SdkBody::from(Bytes::from(payload.clone()));
        let from_file = ByteStream::from_path(&path).await.unwrap().into_inner();
        for (label, original) in [("in-memory", in_memory), ("file", from_file)] {
            let expected_len = original.content_length();
            let expected_contents = original.bytes().map(<[u8]>::to_vec);
            let shaped = shape_body(&shaper, original);
            assert_eq!(
                shaped.content_length(),
                expected_len,
                "[{label}] Content-Length is computed from this; shaping must not change it"
            );
            assert_eq!(
                shaped.bytes().map(<[u8]>::to_vec),
                expected_contents,
                "[{label}] the SDK checksums and signs these contents; they must stay visible"
            );
            let sent = ByteStream::new(shaped)
                .collect()
                .await
                .unwrap()
                .into_bytes();
            assert_eq!(
                vtop_core::checksum::sha256_bytes(&sent),
                vtop_core::checksum::sha256_bytes(&payload),
                "[{label}] the bytes sent must be the bytes given"
            );
        }
        assert_eq!(
            shaper.admitted_bytes(),
            2 * payload.len() as u64,
            "every byte of both bodies was admitted, once"
        );
    }

    /// REGRESSION GUARD for the retry path (#481): the SDK re-sends a body by
    /// `try_clone`, and those bytes cross the wire again. They must cross the
    /// shaper again too, or a store answering 503s would let retries run at
    /// line rate outside the cap — and outside the account.
    #[tokio::test]
    async fn a_body_rebuilt_for_a_retry_passes_the_shaper_again() {
        let shaper = EgressShaper::new(64 * MIB).unwrap();
        let shaped = shape_body(&shaper, SdkBody::from(Bytes::from(vec![7_u8; 100_000])));
        let retry = shaped
            .try_clone()
            .expect("an in-memory body stays retryable when shaped");
        ByteStream::new(shaped).collect().await.unwrap();
        assert_eq!(shaper.admitted_bytes(), 100_000);
        ByteStream::new(retry).collect().await.unwrap();
        assert_eq!(
            shaper.admitted_bytes(),
            200_000,
            "the re-sent attempt must be admitted and counted like the first"
        );
    }

    #[test]
    fn the_bucket_depth_is_a_thirty_second_of_the_cap_and_a_cap_below_the_minimum_is_refused() {
        let shaper = EgressShaper::new(16 * MIB).unwrap();
        assert_eq!(shaper.capacity_bytes(), MIB / 2);
        assert_eq!(shaper.max_piece_bytes(), MIB / 4);
        for rate in [0, vtop_core::config::MIN_EGRESS_BYTES_PER_SECOND - 1] {
            let msg = EgressShaper::new(rate)
                .expect_err("a zero or sub-minimum cap must not build a shaper that never admits")
                .to_string();
            assert!(msg.contains(&rate.to_string()), "names the cap: {msg}");
        }
    }
}

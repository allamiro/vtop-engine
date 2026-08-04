//! Prometheus scrape endpoint plus health/readiness probes.
//!
//! Lifted verbatim in behavior from the archive engine's endpoint and
//! generalized over *what* is being observed, so `vtopctl run`, a metadata
//! node, and a data node all expose the same three routes with the same
//! hardening (#224).
//!
//! Endpoints:
//!   GET /metrics  Prometheus text format
//!   GET /healthz  process liveness — the binary is running and its accept loop
//!                 is turning. Never reports the process's opinion of itself,
//!                 so a wedged-but-alive node still answers 200 and the
//!                 difference from /readyz is diagnostic.
//!   GET /readyz   readiness level, with the reason in the body when not ready
//!
//! # Two ways to start
//!
//! [`maybe_start_from_env`] is the engine's contract: opt-in via
//! `VTOP_METRICS_ADDR`, and *never* fatal — a telemetry problem must not stop a
//! process from archiving telemetry. [`start`] is the cluster-node contract: the
//! operator named a listen address in the node config, so a bind failure is
//! returned rather than logged and swallowed. Silently skipping an endpoint the
//! config explicitly asked for is how a node comes up invisible to the health
//! gate and stalls a deployment with no error to read.
//!
//! # Security posture (#78)
//!
//! The endpoint is **unauthenticated** — anyone who can reach the port can
//! scrape it. Bind it to a private interface (e.g. `127.0.0.1:9090` or a
//! management network), never a public one. The server enforces a concurrent
//! connection cap and a per-connection deadline so an unauthenticated client
//! that CAN reach the port can tie up at most [`MAX_CONNECTIONS`] tasks for
//! [`CONNECTION_DEADLINE`] rather than spawning unbounded work.

use crate::readiness::{Readiness, ReadinessGate};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use prometheus::{Encoder, Registry, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Environment variable holding the listen address, e.g. `0.0.0.0:9090`.
pub const ADDR_ENV: &str = "VTOP_METRICS_ADDR";

/// Maximum concurrent connections. A scrape stack is a handful of pollers;
/// far beyond that is either a misconfiguration or an exhaustion attempt, and
/// both are better served by refusing than by queueing unbounded tasks.
pub const MAX_CONNECTIONS: usize = 16;

/// Hard per-connection deadline. Serving the registry takes milliseconds, so
/// anything alive this long is a stuck or hostile peer holding a permit.
/// Keep-alive is disabled (one request per connection), so the deadline can be
/// this blunt without cutting off a healthy poller mid-scrape.
pub const CONNECTION_DEADLINE: Duration = Duration::from_secs(10);

/// Minimum interval between at-capacity WARN lines; rejections in between are
/// counted and reported in the next line.
const REJECTION_WARN_EVERY: Duration = Duration::from_secs(10);

/// How long a connection may hold a permit without sending request headers.
///
/// Distinct from [`CONNECTION_DEADLINE`], and much shorter, because the two
/// bound different things. The deadline bounds a peer that is *doing* something
/// slowly; this bounds one that is doing nothing at all. With only
/// [`MAX_CONNECTIONS`] permits, a handful of silent sockets would otherwise
/// hold the endpoint closed for the full deadline and refuse a legitimate
/// scrape.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Pause after a failed `accept`, so a persistent failure (file-descriptor
/// exhaustion, a transient kernel error) degrades into a slow retry loop
/// instead of a tight one that burns a core and floods the log — turning a
/// local problem into process-wide pressure.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Minimum interval between accept-failure WARN lines.
const ACCEPT_WARN_EVERY: Duration = Duration::from_secs(10);

/// Why a scrape could not be served.
///
/// The two cases map to different HTTP statuses on purpose: a registry that has
/// not been built yet is a transient 503 a scraper should retry, while an
/// encoding failure is a bug and must be a loud 500 rather than an empty body
/// that renders as a flat-zero panel.
#[derive(Debug)]
pub enum MetricsError {
    /// This process has no registry yet.
    Uninitialized,
    /// A registry exists but could not be encoded.
    Encode(String),
}

/// Whatever a process wants scraped, plus its opinion of its own readiness.
pub trait MetricsSource: Send + Sync + 'static {
    /// Prometheus text-format body.
    fn encode(&self) -> Result<Vec<u8>, MetricsError>;

    /// Readiness level served on `/readyz`.
    fn readiness(&self) -> Readiness;
}

/// The common case: one owned registry plus a [`ReadinessGate`].
pub struct RegistrySource {
    registry: Registry,
    gate: ReadinessGate,
}

impl RegistrySource {
    pub fn new(registry: Registry, gate: ReadinessGate) -> Self {
        Self { registry, gate }
    }
}

impl MetricsSource for RegistrySource {
    fn encode(&self) -> Result<Vec<u8>, MetricsError> {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .map_err(|error| MetricsError::Encode(error.to_string()))?;
        Ok(buf)
    }

    fn readiness(&self) -> Readiness {
        self.gate.get()
    }
}

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(body.into()))
        .expect("static response must build")
}

async fn route(
    req: Request<hyper::body::Incoming>,
    source: Arc<dyn MetricsSource>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // GET only. Every route here is a read, and the endpoint is
    // unauthenticated by design (#78), so anything that is not a GET is a
    // client that has misunderstood this server — answering it identically to
    // a scrape invites exactly that misunderstanding to persist. HEAD is
    // refused too rather than special-cased: it would have to build the full
    // body to compute a Content-Length the server then never sends, and no
    // Prometheus-compatible scraper uses it.
    if req.method() != hyper::Method::GET {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("allow", "GET")
            .header("content-type", "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from_static(b"method not allowed")))
            .expect("static response must build"));
    }
    Ok(match req.uri().path() {
        "/metrics" => match source.encode() {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                // The 0.0.4 content-type is what Prometheus/Alloy expect.
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .expect("metrics response must build"),
            Err(MetricsError::Uninitialized) => {
                text(StatusCode::SERVICE_UNAVAILABLE, "metrics not initialized")
            }
            Err(MetricsError::Encode(error)) => {
                // Encoding failure is a bug, not a scrape error; make it loud
                // rather than serving a misleading empty body.
                tracing::error!(%error, "failed to encode metrics");
                text(StatusCode::INTERNAL_SERVER_ERROR, "encode error")
            }
        },
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => {
            let readiness = source.readiness();
            let status = if readiness.is_ready() {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            text(status, readiness.describe())
        }
        _ => text(StatusCode::NOT_FOUND, "not found"),
    })
}

/// Bind and serve the endpoint on an explicitly configured address.
///
/// Returns the bound address. Unlike [`maybe_start_from_env`], a bind failure is
/// an error the caller must handle: the address came from a config file, so
/// swallowing the failure would hide a misconfiguration behind a node that
/// looks healthy but can never be scraped or health-checked.
pub async fn start(
    addr: SocketAddr,
    source: Arc<dyn MetricsSource>,
) -> Result<SocketAddr, std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr().unwrap_or(addr);
    tracing::info!(%bound, "observability endpoint listening (/metrics, /healthz, /readyz)");
    tokio::spawn(accept_loop(listener, source));
    Ok(bound)
}

/// The configured listen address, or `None` when the endpoint is disabled or
/// the value is unusable.
///
/// Public because callers need to know whether the endpoint is wanted *before*
/// doing the setup work it implies — the archive engine builds its metric
/// registry only when there is somewhere to serve it. Sharing this keeps the
/// accepted formats and the error message from drifting between the two checks.
pub fn addr_from_env() -> Option<SocketAddr> {
    let raw = std::env::var(ADDR_ENV).ok()?;
    match raw.parse() {
        Ok(addr) => Some(addr),
        Err(error) => {
            tracing::error!(
                value = %raw, %error,
                "{ADDR_ENV} is not a valid socket address (expected host:port, e.g. 0.0.0.0:9090); \
                 metrics endpoint disabled"
            );
            None
        }
    }
}

/// Start the endpoint if `ADDR_ENV` is set. Returns the bound address, or
/// `None` when disabled or unusable.
///
/// Never returns an error: a telemetry problem must not stop the process.
pub async fn maybe_start_from_env(source: Arc<dyn MetricsSource>) -> Option<SocketAddr> {
    let addr = addr_from_env()?;
    match start(addr, source).await {
        Ok(bound) => Some(bound),
        Err(e) => {
            // Bind failure (port in use, permissions) must not be fatal here:
            // this path is the opt-in env form, not an explicit config request.
            tracing::error!(%addr, error = %e, "could not bind metrics endpoint; continuing without it");
            None
        }
    }
}

async fn accept_loop(listener: TcpListener, source: Arc<dyn MetricsSource>) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    // Rejection logging is rate-limited: at capacity, a flooding client
    // triggers one rejection per accepted connection, and a synchronous
    // WARN per rejection would turn the cap into a log-flood/CPU path —
    // the exhaustion vector this endpoint hardening exists to close.
    let mut rejected_since_warn: u64 = 0;
    let mut last_warn = std::time::Instant::now() - REJECTION_WARN_EVERY;
    let mut accept_errors_since_warn: u64 = 0;
    let mut last_accept_warn = std::time::Instant::now() - ACCEPT_WARN_EVERY;
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                // At capacity: close immediately rather than queue. The
                // permit is taken BEFORE spawning, so an attacker can hold
                // at most MAX_CONNECTIONS tasks, not one per SYN.
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    rejected_since_warn += 1;
                    if last_warn.elapsed() >= REJECTION_WARN_EVERY {
                        tracing::warn!(
                            %peer,
                            rejected = rejected_since_warn,
                            "metrics connections rejected: at capacity \
                             (count since last report; further rejections \
                             are aggregated)"
                        );
                        rejected_since_warn = 0;
                        last_warn = std::time::Instant::now();
                    }
                    drop(stream);
                    continue;
                };
                let source = Arc::clone(&source);
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let source = Arc::clone(&source);
                        async move { route(req, source).await }
                    });
                    let conn = hyper::server::conn::http1::Builder::new()
                        // One request per connection: a keep-alive poller
                        // would otherwise park on a permit between scrapes
                        // and MAX_CONNECTIONS idle pollers would starve the
                        // endpoint. Prometheus reconnects per scrape fine.
                        .keep_alive(false)
                        // hyper needs an explicit timer to enforce its own
                        // timeouts; without one, setting header_read_timeout
                        // panics at the first request rather than failing at
                        // build time.
                        .timer(TokioTimer::new())
                        // A socket that never sends headers releases its
                        // permit in seconds rather than holding it for the
                        // full connection deadline.
                        .header_read_timeout(HEADER_READ_TIMEOUT)
                        .serve_connection(TokioIo::new(stream), service);
                    match tokio::time::timeout(CONNECTION_DEADLINE, conn).await {
                        Err(_) => {
                            tracing::debug!(%peer, "metrics connection hit deadline; closed")
                        }
                        Ok(Err(e)) => tracing::debug!(error = %e, "metrics connection closed"),
                        Ok(Ok(())) => {}
                    }
                    drop(permit);
                });
            }
            Err(e) => {
                // Keep serving: one bad accept must not silence metrics
                // forever. But retry after a pause, because the failures that
                // persist (out of file descriptors) would otherwise spin this
                // loop at full speed and make the shortage worse.
                accept_errors_since_warn += 1;
                if last_accept_warn.elapsed() >= ACCEPT_WARN_EVERY {
                    tracing::warn!(
                        error = %e,
                        failures = accept_errors_since_warn,
                        "metrics accept failed (count since last report; \
                         retrying with backoff)"
                    );
                    accept_errors_since_warn = 0;
                    last_accept_warn = std::time::Instant::now();
                }
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{IntCounter, Opts};

    struct Uninitialized;
    impl MetricsSource for Uninitialized {
        fn encode(&self) -> Result<Vec<u8>, MetricsError> {
            Err(MetricsError::Uninitialized)
        }
        fn readiness(&self) -> Readiness {
            Readiness::not_ready("no registry")
        }
    }

    fn registry_with_a_counter() -> Registry {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        let counter = IntCounter::with_opts(Opts::new("probe_total", "probe")).unwrap();
        counter.inc();
        registry.register(Box::new(counter)).unwrap();
        registry
    }

    #[test]
    fn a_registry_source_encodes_prometheus_text() {
        let source = RegistrySource::new(registry_with_a_counter(), ReadinessGate::ready());
        let body = String::from_utf8(source.encode().unwrap()).unwrap();
        assert!(body.contains("vtop_probe_total"), "{body}");
    }

    #[test]
    fn a_registry_source_reports_the_gate_it_was_given() {
        let gate = ReadinessGate::starting("warming up");
        let source = RegistrySource::new(registry_with_a_counter(), gate.clone());
        assert!(!source.readiness().is_ready());
        gate.mark_ready();
        assert!(source.readiness().is_ready());
    }

    async fn get(addr: SocketAddr, path: &str) -> (StatusCode, String) {
        request(addr, "GET", path).await
    }

    async fn request(addr: SocketAddr, method: &str, path: &str) -> (StatusCode, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("{method} {path} HTTP/1.1\r\nHost: probe\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let response = String::from_utf8_lossy(&raw).to_string();
        let code = response
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (StatusCode::from_u16(code).unwrap(), body)
    }

    #[tokio::test]
    async fn explicit_start_serves_all_three_routes() {
        let gate = ReadinessGate::starting("warming up");
        let source = Arc::new(RegistrySource::new(registry_with_a_counter(), gate.clone()));
        let addr = start("127.0.0.1:0".parse().unwrap(), source)
            .await
            .expect("bind on an ephemeral port must succeed");

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("vtop_probe_total"), "{body}");

        // Liveness is independent of readiness: a starting node is alive.
        assert_eq!(get(addr, "/healthz").await.0, StatusCode::OK);

        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            body.contains("warming up"),
            "the reason must reach the operator: {body}"
        );

        gate.mark_ready();
        assert_eq!(get(addr, "/readyz").await.0, StatusCode::OK);

        assert_eq!(get(addr, "/nope").await.0, StatusCode::NOT_FOUND);
    }

    /// The endpoint is unauthenticated (#78), so it must not answer verbs it
    /// does not implement as though it did.
    #[tokio::test]
    async fn only_get_is_answered() {
        let source = Arc::new(RegistrySource::new(
            registry_with_a_counter(),
            ReadinessGate::ready(),
        ));
        let addr = start("127.0.0.1:0".parse().unwrap(), source).await.unwrap();
        for method in ["POST", "PUT", "DELETE", "HEAD"] {
            let (status, _) = request(addr, method, "/metrics").await;
            assert_eq!(
                status,
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} /metrics must be refused, not served"
            );
        }
        assert_eq!(get(addr, "/metrics").await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn an_uninitialized_registry_is_a_503_not_an_empty_scrape() {
        let addr = start("127.0.0.1:0".parse().unwrap(), Arc::new(Uninitialized))
            .await
            .unwrap();
        let (status, _) = get(addr, "/metrics").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "an empty 200 would render as a healthy flat-zero panel"
        );
    }

    #[tokio::test]
    async fn explicit_start_surfaces_a_bind_failure_instead_of_swallowing_it() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap();
        let source = Arc::new(RegistrySource::new(
            registry_with_a_counter(),
            ReadinessGate::ready(),
        ));
        assert!(
            start(addr, source).await.is_err(),
            "a configured address that cannot bind must be reported, not logged and dropped"
        );
    }
}

//! Prometheus scrape endpoint plus health/readiness probes.
//!
//! Opt-in: nothing listens unless `VTOP_METRICS_ADDR` is set (e.g.
//! `0.0.0.0:9090`). The engine is often run as a single binary in a lab, and it
//! should not open a port nobody asked for.
//!
//! Runs as a detached task and is deliberately defensive: **telemetry must never
//! be able to take down the data path**. If the listener cannot bind, the engine
//! logs and carries on archiving rather than refusing to start — an unobservable
//! engine is bad, an engine that will not run is worse.
//!
//! Endpoints:
//!   GET /metrics  Prometheus text format
//!   GET /healthz  process liveness
//!   GET /readyz   readiness (metrics registry initialized)
//!
//! # Security posture (#78)
//!
//! The endpoint is **unauthenticated** — anyone who can reach the port can
//! scrape it. Bind it to a private interface (e.g. `127.0.0.1:9090` or a
//! management network), never a public one. The server enforces a concurrent
//! connection cap and a per-connection deadline so an unauthenticated client
//! that CAN reach the port can tie up at most [`MAX_CONNECTIONS`] tasks for
//! [`CONNECTION_DEADLINE`] rather than spawning unbounded work.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use vtop_core::telemetry;

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

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(body.into()))
        .expect("static response must build")
}

async fn route(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(match req.uri().path() {
        "/metrics" => match telemetry::metrics() {
            Some(m) => match m.encode() {
                Ok(body) => Response::builder()
                    .status(StatusCode::OK)
                    // The 0.0.4 content-type is what Prometheus/Alloy expect.
                    .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(Full::new(Bytes::from(body)))
                    .expect("metrics response must build"),
                Err(e) => {
                    // Encoding failure is a bug, not a scrape error; make it
                    // loud rather than serving a misleading empty body that
                    // would render as a flat-zero panel.
                    tracing::error!(error = %e, "failed to encode metrics");
                    text(StatusCode::INTERNAL_SERVER_ERROR, "encode error")
                }
            },
            None => text(StatusCode::SERVICE_UNAVAILABLE, "metrics not initialized"),
        },
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => match telemetry::metrics() {
            Some(_) => text(StatusCode::OK, "ready"),
            None => text(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
        },
        _ => text(StatusCode::NOT_FOUND, "not found"),
    })
}

/// Start the endpoint if `VTOP_METRICS_ADDR` is set. Returns the bound address,
/// or `None` when disabled or unusable.
///
/// Never returns an error: a telemetry problem must not stop the engine.
pub async fn maybe_start() -> Option<SocketAddr> {
    let raw = std::env::var(ADDR_ENV).ok()?;
    let addr: SocketAddr = match raw.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(
                value = %raw, error = %e,
                "{ADDR_ENV} is not a valid socket address (expected host:port, e.g. 0.0.0.0:9090); \
                 metrics endpoint disabled"
            );
            return None;
        }
    };

    if let Err(e) = telemetry::init() {
        tracing::error!(error = %e, "metrics registry failed to initialize; endpoint disabled");
        return None;
    }

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            // Bind failure (port in use, permissions) must not be fatal: the
            // engine's job is archiving telemetry, not serving it.
            tracing::error!(%addr, error = %e, "could not bind metrics endpoint; continuing without it");
            return None;
        }
    };
    let bound = listener.local_addr().unwrap_or(addr);
    tracing::info!(%bound, "metrics endpoint listening (/metrics, /healthz, /readyz)");

    tokio::spawn(async move {
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        // Rejection logging is rate-limited: at capacity, a flooding client
        // triggers one rejection per accepted connection, and a synchronous
        // WARN per rejection would turn the cap into a log-flood/CPU path —
        // the exhaustion vector this endpoint hardening exists to close.
        let mut rejected_since_warn: u64 = 0;
        let mut last_warn = std::time::Instant::now() - REJECTION_WARN_EVERY;
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
                    tokio::spawn(async move {
                        let conn = hyper::server::conn::http1::Builder::new()
                            // One request per connection: a keep-alive poller
                            // would otherwise park on a permit between scrapes
                            // and MAX_CONNECTIONS idle pollers would starve the
                            // endpoint. Prometheus reconnects per scrape fine.
                            .keep_alive(false)
                            .serve_connection(TokioIo::new(stream), service_fn(route));
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
                    // Keep serving: one bad accept must not silence metrics forever.
                    tracing::warn!(error = %e, "metrics accept failed");
                }
            }
        }
    });

    Some(bound)
}

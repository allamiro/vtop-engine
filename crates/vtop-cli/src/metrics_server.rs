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

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use vtop_core::telemetry;

/// Environment variable holding the listen address, e.g. `0.0.0.0:9090`.
pub const ADDR_ENV: &str = "VTOP_METRICS_ADDR";

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
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tokio::spawn(async move {
                        if let Err(e) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service_fn(route))
                            .await
                        {
                            tracing::debug!(error = %e, "metrics connection closed");
                        }
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

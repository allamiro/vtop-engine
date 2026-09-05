//! The archive engine's binding of the shared observability endpoint (#224).
//!
//! The server itself — routing, the connection cap, the per-connection deadline
//! (#78) — now lives in `vtop-observe` so the cluster nodes expose exactly the
//! same surface. What stays here is the part that is specific to `vtopctl run`:
//! the engine's metrics live in a process-global `OnceLock` registry
//! ([`vtop_core::telemetry`]) rather than an owned one, and its readiness
//! question is "did that registry initialize".
//!
//! Opt-in: nothing listens unless `VTOP_METRICS_ADDR` is set (e.g.
//! `0.0.0.0:9090`). The engine is often run as a single binary in a lab, and it
//! should not open a port nobody asked for. Failure to start is logged, never
//! fatal — telemetry must never be able to take down the data path.

use std::net::SocketAddr;
use std::sync::Arc;
use vtop_core::telemetry;
use vtop_observe::{MetricsError, MetricsSource, Readiness};

pub use vtop_observe::{ADDR_ENV, CONNECTION_DEADLINE, MAX_CONNECTIONS};

/// Reads the process-global engine registry at scrape time.
///
/// Deliberately not a captured `&'static Metrics`: the endpoint can be started
/// before the engine has initialized telemetry, and the correct answer in that
/// window is a 503 the scraper retries, not a snapshot of a registry that did
/// not exist yet.
struct EngineMetrics;

impl MetricsSource for EngineMetrics {
    fn encode(&self) -> Result<Vec<u8>, MetricsError> {
        match telemetry::metrics() {
            Some(m) => m
                .encode()
                .map(String::into_bytes)
                .map_err(|e| MetricsError::Encode(e.to_string())),
            None => Err(MetricsError::Uninitialized),
        }
    }

    fn readiness(&self) -> Readiness {
        match telemetry::metrics() {
            Some(_) => Readiness::Ready,
            None => Readiness::not_ready("metrics registry not initialized"),
        }
    }
}

/// Start the endpoint if `VTOP_METRICS_ADDR` is set. Returns the bound address,
/// or `None` when disabled or unusable.
pub async fn maybe_start() -> Option<SocketAddr> {
    // Validate the address BEFORE initializing the registry. With the endpoint
    // disabled or misconfigured there is nothing to serve, and building a
    // registry nobody can read would leave the engine paying for guarded
    // counter work on the hot path with no way to observe it — the behavior
    // an unusable `VTOP_METRICS_ADDR` is supposed to switch off entirely.
    //
    // Shared with the starter rather than re-parsed here, so the accepted
    // formats and the error message cannot drift between the two checks.
    let addr = vtop_observe::addr_from_env()?;
    // And the TLS half (review): a misconfigured pair disables the endpoint,
    // which must be known before the registry exists for the same reason.
    let tls = vtop_observe::tls_from_env_or_disabled()?;
    if let Err(e) = telemetry::init() {
        tracing::error!(error = %e, "metrics registry failed to initialize; endpoint disabled");
        return None;
    }
    vtop_observe::start_lenient(addr, Arc::new(EngineMetrics), tls).await
}

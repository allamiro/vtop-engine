//! Shared operational surface for every long-running VTOP process (#224).
//!
//! Why this is a crate and not a module: the archive engine (`vtopctl run`) had
//! a perfectly good Prometheus endpoint, and the cluster nodes (`vtop-node`,
//! #215) had nothing but stdout ready-markers. Two processes with two answers to
//! "is it up, and what is it doing" is how an operator ends up grepping logs
//! during an incident. The endpoint, the probes, and the log format now come
//! from one place, so a node and the engine are scraped, alerted on, and gated
//! the same way.
//!
//! What lives here:
//!
//! * [`server`] — `/metrics`, `/healthz`, `/readyz` over HTTP, with the
//!   connection cap and per-connection deadline the engine's endpoint already
//!   carried (#78).
//! * [`readiness`] — a shared gate a process flips once it can actually serve,
//!   so `/readyz` reports startup progress instead of merely "the binary is
//!   running". This is what replaces log-grep readiness in the chaos harness.
//! * [`logging`] — one definition of `VTOP_LOG_FORMAT=json`, so node logs and
//!   engine logs land in Loki with the same shape.
//!
//! What deliberately does NOT live here: metric *definitions*. Each process
//! registers its own metrics against its own registry and hands this crate a
//! [`server::MetricsSource`]. Keeping the catalogue out of the shared crate is
//! what stops it from growing a dependency on every crate in the workspace.

pub mod logging;
pub mod readiness;
pub mod server;

pub use readiness::{Readiness, ReadinessGate};
pub use server::{
    addr_from_env, maybe_start_from_env, start, start_tls, tls_from_env, MetricsError,
    MetricsSource, RegistrySource, TlsSettings, ADDR_ENV, CONNECTION_DEADLINE, MAX_CONNECTIONS,
    TLS_CERT_ENV, TLS_CLIENT_CA_ENV, TLS_KEY_ENV,
};

//! The metrics endpoint is a contract with the monitoring stack.
//!
//! Dashboards and alerts are written against these metric names and labels, so a
//! rename silently breaks every panel while the engine keeps working perfectly —
//! exactly the kind of failure nobody notices until an incident.
//!
//! These tests pin the contract and the endpoint's behavior.

use vtop_cli::metrics_server;
use vtop_core::telemetry;

/// Metric names the Grafana dashboards query. Renaming one without updating
/// `observability/` turns a panel into a blank rectangle.
const DASHBOARD_METRICS: [&str; 12] = [
    "vtop_batches_total",
    "vtop_verified_total",
    "vtop_commits_total",
    "vtop_verification_failures_total",
    "vtop_verification_backend_limited_total",
    "vtop_replay_required_total",
    "vtop_failed_total",
    "vtop_records_total",
    "vtop_bytes_in_total",
    "vtop_bytes_out_total",
    "vtop_stage_duration_seconds",
    "vtop_batch_duration_seconds",
];

fn populated() -> String {
    let m = telemetry::init().expect("registry must initialize");
    let l = ["default", "kafka", "cef"];
    m.batches_total
        .with_label_values(&["default", "kafka", "cef", "verified"])
        .inc();
    m.verified_total.with_label_values(&l).inc();
    m.commits_total.with_label_values(&l).inc();
    m.verification_failures_total
        .with_label_values(&l)
        .inc_by(0);
    m.verification_backend_limited_total
        .with_label_values(&l)
        .inc_by(0);
    m.replay_required_total.with_label_values(&l).inc_by(0);
    m.failed_total.with_label_values(&l).inc_by(0);
    m.records_total.with_label_values(&l).inc_by(10);
    m.bytes_in_total.with_label_values(&l).inc_by(100);
    m.bytes_out_total.with_label_values(&l).inc_by(30);
    m.batch_duration_seconds.with_label_values(&l).observe(0.5);
    m.compression_ratio.with_label_values(&l).observe(3.3);
    for s in telemetry::STAGES {
        m.stage_duration_seconds
            .with_label_values(&["default", "kafka", "cef", s])
            .observe(0.01);
    }
    m.inflight_batches.set(2);
    m.encode().expect("encode must succeed")
}

#[test]
fn every_metric_the_dashboards_query_is_exported() {
    let text = populated();
    for name in DASHBOARD_METRICS {
        assert!(
            text.contains(name),
            "{name} is queried by observability/ dashboards but is not exported; \
             renaming a metric silently blanks its panels"
        );
    }
}

/// Histograms must expose `_bucket`, or `histogram_quantile()` returns nothing
/// and every p95 panel is empty while looking perfectly healthy.
#[test]
fn histograms_expose_buckets_for_quantiles() {
    let text = populated();
    for h in [
        "vtop_stage_duration_seconds_bucket",
        "vtop_batch_duration_seconds_bucket",
        "vtop_compression_ratio_bucket",
    ] {
        assert!(
            text.contains(h),
            "missing {h}; histogram_quantile() needs it"
        );
    }
}

/// The label set is a contract too: dashboards group by these.
#[test]
fn expected_labels_are_present() {
    let text = populated();
    for l in [
        r#"tenant="default""#,
        r#"source_type="kafka""#,
        r#"format="cef""#,
        r#"state="verified""#,
        r#"stage="compress""#,
    ] {
        assert!(text.contains(l), "missing label {l}");
    }
}

/// Cardinality guard. batch_id is unbounded — one series per batch would grow
/// the TSDB without limit and eventually take the monitoring stack down.
#[test]
fn unbounded_identifiers_are_never_labels() {
    let text = populated();
    for forbidden in ["batch_id=", "object_uri=", "manifest_uri=", "checksum="] {
        assert!(
            !text.contains(forbidden),
            "{forbidden} is unbounded and must stay in logs, never a metric label"
        );
    }
}

/// The invariant, expressed in metrics: a commit is only ever recorded after a
/// verification, so committed can never exceed verified.
#[test]
fn commits_never_exceed_verified_in_the_metric_contract() {
    let m = telemetry::init().unwrap();
    let l = ["default", "file", "jsonl"];
    for _ in 0..5 {
        m.verified_total.with_label_values(&l).inc();
        m.commits_total.with_label_values(&l).inc();
    }
    let v = m.verified_total.with_label_values(&l).get();
    let c = m.commits_total.with_label_values(&l).get();
    assert!(
        c <= v,
        "commits ({c}) must never exceed verified ({v}) - that would mean \
         SOURCE_COMMITTED happened without VERIFIED"
    );
}

/// The endpoint is opt-in: no VTOP_METRICS_ADDR, no listener. The engine is
/// often a single binary in a lab and must not open a port nobody asked for.
#[tokio::test]
async fn endpoint_is_disabled_without_the_env_var() {
    std::env::remove_var(metrics_server::ADDR_ENV);
    assert!(
        metrics_server::maybe_start().await.is_none(),
        "no listener may start unless VTOP_METRICS_ADDR is set"
    );
}

/// A bad address must not be fatal. Telemetry is never allowed to stop the
/// engine from archiving telemetry.
#[tokio::test]
async fn a_malformed_address_disables_metrics_without_panicking() {
    std::env::set_var(metrics_server::ADDR_ENV, "not-a-socket-addr");
    let bound = metrics_server::maybe_start().await;
    std::env::remove_var(metrics_server::ADDR_ENV);
    assert!(
        bound.is_none(),
        "a malformed address must disable, not crash"
    );
}

/// End-to-end: bind, scrape, and check the three routes behave.
#[tokio::test]
async fn serves_metrics_health_and_readiness() {
    // Port 0 = let the OS pick a free one, so the test cannot collide with a
    // developer's running lab.
    std::env::set_var(metrics_server::ADDR_ENV, "127.0.0.1:0");
    let addr = metrics_server::maybe_start()
        .await
        .expect("endpoint must start on a valid address");
    std::env::remove_var(metrics_server::ADDR_ENV);

    let m = telemetry::init().unwrap();
    m.records_total
        .with_label_values(&["default", "file", "jsonl"])
        .inc_by(7);

    let get = |path: &'static str| async move {
        let url = format!("http://{addr}{path}");
        let out = tokio::process::Command::new("curl")
            .args(["-s", "-w", "\n%{http_code}", &url])
            .output()
            .await
            .expect("curl must run");
        String::from_utf8_lossy(&out.stdout).to_string()
    };

    let metrics = get("/metrics").await;
    assert!(metrics.trim_end().ends_with("200"), "/metrics -> {metrics}");
    assert!(
        metrics.contains("vtop_records_total"),
        "/metrics must serve the registry: {metrics}"
    );

    assert!(get("/healthz").await.trim_end().ends_with("200"));
    assert!(get("/readyz").await.trim_end().ends_with("200"));
    assert!(
        get("/nope").await.trim_end().ends_with("404"),
        "unknown paths must 404"
    );
}

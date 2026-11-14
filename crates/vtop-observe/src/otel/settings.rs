//! What the environment asks of OpenTelemetry export, read and checked once.
//!
//! Only the standard `OTEL_*` variables are read. Every value is checked here,
//! before anything is built: a variable that is set but cannot be honoured is
//! an error at startup that names the variable and what it accepts, never a
//! silent fallback. The values of the header variables are never read into an
//! error or a log line, since they usually hold a collector's credentials.

use std::time::Duration;

/// The collector's base URL; each signal's path is appended.
pub const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The full traces URL, used as given. Wins over [`ENDPOINT_ENV`].
pub const TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
/// The full metrics URL, used as given. Wins over [`ENDPOINT_ENV`].
pub const METRICS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
/// `service.name`.
pub const SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
/// `otlp` (the default once an endpoint is set) or `none`.
pub const TRACES_EXPORTER_ENV: &str = "OTEL_TRACES_EXPORTER";
/// `otlp` (the default once an endpoint is set) or `none`.
pub const METRICS_EXPORTER_ENV: &str = "OTEL_METRICS_EXPORTER";
/// The trace sampler.
pub const SAMPLER_ENV: &str = "OTEL_TRACES_SAMPLER";
/// The sampler's argument (the ratio for the ratio samplers).
pub const SAMPLER_ARG_ENV: &str = "OTEL_TRACES_SAMPLER_ARG";
/// `true` turns all export off.
pub const SDK_DISABLED_ENV: &str = "OTEL_SDK_DISABLED";
/// Extra resource attributes, `key=value,...`.
pub const RESOURCE_ATTRIBUTES_ENV: &str = "OTEL_RESOURCE_ATTRIBUTES";
/// The only protocol this build sends.
pub const PROTOCOL: &str = "http/protobuf";

/// The span queue's capacity, in spans (`OTEL_BSP_MAX_QUEUE_SIZE`).
pub const DEFAULT_MAX_QUEUE: usize = 2048;
/// Spans per export request (`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`).
pub const DEFAULT_MAX_BATCH: usize = 512;
/// The most `OTEL_BSP_MAX_QUEUE_SIZE` and `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
/// accept: a typo must not reserve gigabytes (review of #693).
pub const MAX_QUEUE: usize = 1_000_000;
/// The longest a span waits in the queue (`OTEL_BSP_SCHEDULE_DELAY`).
pub const DEFAULT_SCHEDULE_DELAY: Duration = Duration::from_millis(5_000);
/// Per-request timeout (`OTEL_EXPORTER_OTLP_TIMEOUT`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(10_000);
/// Between metric exports (`OTEL_METRIC_EXPORT_INTERVAL`).
pub const DEFAULT_METRIC_INTERVAL: Duration = Duration::from_millis(60_000);
/// Per metric export (`OTEL_METRIC_EXPORT_TIMEOUT`).
pub const DEFAULT_METRIC_TIMEOUT: Duration = Duration::from_millis(30_000);

/// A variable that is set to something this build cannot honour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub variable: &'static str,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.variable, self.message)
    }
}

impl std::error::Error for ConfigError {}

fn invalid(variable: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError {
        variable,
        message: message.into(),
    }
}

/// The trace sampler, as `OTEL_TRACES_SAMPLER` names it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sampler {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio(f64),
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    ParentBasedTraceIdRatio(f64),
}

/// The accepted `OTEL_TRACES_SAMPLER` values, for the error message.
pub const SAMPLERS: &str = "always_on, always_off, traceidratio, parentbased_always_on, \
                            parentbased_always_off, parentbased_traceidratio";

impl Sampler {
    /// The name and, for the ratio samplers, the ratio.
    pub fn describe(self) -> String {
        match self {
            Sampler::AlwaysOn => "always_on".into(),
            Sampler::AlwaysOff => "always_off".into(),
            Sampler::TraceIdRatio(r) => format!("traceidratio({r})"),
            Sampler::ParentBasedAlwaysOn => "parentbased_always_on".into(),
            Sampler::ParentBasedAlwaysOff => "parentbased_always_off".into(),
            Sampler::ParentBasedTraceIdRatio(r) => format!("parentbased_traceidratio({r})"),
        }
    }

    pub(crate) fn to_sdk(self) -> opentelemetry_sdk::trace::Sampler {
        use opentelemetry_sdk::trace::Sampler as S;
        match self {
            Sampler::AlwaysOn => S::AlwaysOn,
            Sampler::AlwaysOff => S::AlwaysOff,
            Sampler::TraceIdRatio(r) => S::TraceIdRatioBased(r),
            Sampler::ParentBasedAlwaysOn => S::ParentBased(Box::new(S::AlwaysOn)),
            Sampler::ParentBasedAlwaysOff => S::ParentBased(Box::new(S::AlwaysOff)),
            Sampler::ParentBasedTraceIdRatio(r) => {
                S::ParentBased(Box::new(S::TraceIdRatioBased(r)))
            }
        }
    }
}

/// Where and how one signal is sent.
#[derive(Debug, Clone, PartialEq)]
pub struct Signal {
    /// The full URL the signal is POSTed to. Never logged whole; see
    /// [`endpoint_display`].
    pub endpoint: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Request headers (`OTEL_EXPORTER_OTLP_*HEADERS`). Secret: never logged.
    pub headers: Vec<(String, String)>,
}

/// The span queue between the batch path and the exporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSettings {
    pub max_queue: usize,
    pub max_batch: usize,
    pub schedule_delay: Duration,
}

/// Export that is on.
#[derive(Debug, Clone, PartialEq)]
pub struct Export {
    /// `None` when `OTEL_TRACES_EXPORTER=none`.
    pub traces: Option<Signal>,
    /// `None` when `OTEL_METRICS_EXPORTER=none`.
    pub metrics: Option<Signal>,
    pub metric_interval: Duration,
    /// `OTEL_SERVICE_NAME`, else `service.name` in `OTEL_RESOURCE_ATTRIBUTES`,
    /// else the process's own default.
    pub service_name: String,
    /// `OTEL_RESOURCE_ATTRIBUTES`, parsed.
    pub resource_attributes: Vec<(String, String)>,
    pub sampler: Sampler,
    pub queue: QueueSettings,
}

/// What the environment asks for.
#[derive(Debug, Clone, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "built once at startup and consumed at once; boxing would only add a deref"
)]
pub enum Settings {
    /// Export is off; the reason names what to set to turn it on.
    Off(String),
    On(Export),
}

impl Export {
    /// The one INFO line that says export is on: each signal's endpoint (host
    /// and path only), the protocol, the sampler and the service name. Never
    /// a header, a credential or a query string.
    pub fn describe(&self) -> String {
        let signal = |s: &Option<Signal>| {
            s.as_ref()
                .map_or_else(|| "off".to_owned(), |s| endpoint_display(&s.endpoint))
        };
        format!(
            "OpenTelemetry export enabled: traces to {}, metrics to {}, protocol {PROTOCOL}, \
             sampler {}, service.name {}",
            signal(&self.traces),
            signal(&self.metrics),
            self.sampler.describe(),
            self.service_name
        )
    }
}

/// The host, port and path of a URL, for a log line. The user-info, the
/// query string and the fragment are dropped: each can hold a secret.
pub fn endpoint_display(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{host}{path}")
}

/// Read the settings through `env`: `std::env::var` in the binaries, a map in
/// the tests (the environment is process-wide, so tests that set it race).
/// `default_service` is the process's own name (`vtop-engine`, `vtop-node`).
pub fn settings_from(
    env: impl Fn(&str) -> Option<String>,
    default_service: &str,
) -> Result<Settings, ConfigError> {
    let set = |name: &str| {
        env(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    // No endpoint, no export, and nothing else is read: a fleet that sets
    // OTEL_* variables for other software must not stop starting because
    // this one would refuse them (review of #693).
    if [ENDPOINT_ENV, TRACES_ENDPOINT_ENV, METRICS_ENDPOINT_ENV]
        .iter()
        .all(|var| set(var).is_none())
    {
        return Ok(Settings::Off(format!("{ENDPOINT_ENV} unset")));
    }
    if let Some(v) = set(SDK_DISABLED_ENV) {
        match v.to_ascii_lowercase().as_str() {
            "true" => return Ok(Settings::Off(format!("{SDK_DISABLED_ENV}=true"))),
            "false" => {}
            _ => return Err(invalid(SDK_DISABLED_ENV, "must be true or false")),
        }
    }
    let enabled = |var: &'static str| -> Result<bool, ConfigError> {
        match set(var).map(|v| v.to_ascii_lowercase()).as_deref() {
            None | Some("otlp") => Ok(true),
            Some("none") => Ok(false),
            Some(_) => Err(invalid(var, "must be otlp or none")),
        }
    };
    let traces_on = enabled(TRACES_EXPORTER_ENV)?;
    let metrics_on = enabled(METRICS_EXPORTER_ENV)?;

    for var in [
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    ] {
        if let Some(protocol) = set(var) {
            if protocol != PROTOCOL {
                return Err(invalid(
                    var,
                    format!("{protocol} is not supported; the accepted value is {PROTOCOL}"),
                ));
            }
        }
    }
    for var in [
        "OTEL_EXPORTER_OTLP_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
        "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
    ] {
        if set(var).is_some_and(|v| v != "none") {
            return Err(invalid(
                var,
                "compression is not supported; unset it or set none",
            ));
        }
    }

    let base = match set(ENDPOINT_ENV) {
        Some(url) => Some(check_url(ENDPOINT_ENV, &url)?),
        None => None,
    };
    let endpoint = |var: &'static str, path: &str| -> Result<Option<String>, ConfigError> {
        match set(var) {
            Some(url) => Ok(Some(check_url(var, &url)?)),
            None => Ok(base
                .as_ref()
                .map(|b| format!("{}{path}", b.trim_end_matches('/')))),
        }
    };
    let traces_url = endpoint(TRACES_ENDPOINT_ENV, "/v1/traces")?;
    let metrics_url = endpoint(METRICS_ENDPOINT_ENV, "/v1/metrics")?;
    if traces_url.is_none() && metrics_url.is_none() {
        return Ok(Settings::Off(format!("{ENDPOINT_ENV} unset")));
    }

    let millis = |var: &'static str| -> Result<Option<Duration>, ConfigError> {
        match set(var) {
            None => Ok(None),
            Some(v) => v
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .map(|ms| Some(Duration::from_millis(ms)))
                .ok_or_else(|| invalid(var, "must be a whole number of milliseconds above 0")),
        }
    };
    let count = |var: &'static str, default: usize| -> Result<usize, ConfigError> {
        match set(var) {
            None => Ok(default),
            Some(v) => v
                .parse::<usize>()
                .ok()
                .filter(|n| (1..=MAX_QUEUE).contains(n))
                .ok_or_else(|| {
                    invalid(var, format!("must be a whole number from 1 to {MAX_QUEUE}"))
                }),
        }
    };
    let shared_timeout = millis("OTEL_EXPORTER_OTLP_TIMEOUT")?;
    let shared_headers = headers("OTEL_EXPORTER_OTLP_HEADERS", &env, true)?;

    let traces = match (traces_on, traces_url) {
        (true, Some(endpoint)) => Some(Signal {
            endpoint,
            timeout: millis("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")?
                .or(shared_timeout)
                .unwrap_or(DEFAULT_TIMEOUT),
            headers: headers("OTEL_EXPORTER_OTLP_TRACES_HEADERS", &env, true)?
                .unwrap_or_else(|| shared_headers.clone().unwrap_or_default()),
        }),
        _ => None,
    };
    let metrics = match (metrics_on, metrics_url) {
        (true, Some(endpoint)) => Some(Signal {
            endpoint,
            timeout: millis("OTEL_METRIC_EXPORT_TIMEOUT")?
                .or(millis("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT")?)
                .or(shared_timeout)
                .unwrap_or(DEFAULT_METRIC_TIMEOUT),
            headers: headers("OTEL_EXPORTER_OTLP_METRICS_HEADERS", &env, true)?
                .unwrap_or_else(|| shared_headers.clone().unwrap_or_default()),
        }),
        _ => None,
    };
    if traces.is_none() && metrics.is_none() {
        return Ok(Settings::Off(format!(
            "{TRACES_EXPORTER_ENV} and {METRICS_EXPORTER_ENV} are none"
        )));
    }

    let max_queue = count("OTEL_BSP_MAX_QUEUE_SIZE", DEFAULT_MAX_QUEUE)?;
    let max_batch = count("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", DEFAULT_MAX_BATCH)?;
    if max_batch > max_queue {
        return Err(invalid(
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
            format!("must not exceed OTEL_BSP_MAX_QUEUE_SIZE ({max_queue})"),
        ));
    }
    let resource_attributes = headers(RESOURCE_ATTRIBUTES_ENV, &env, false)?.unwrap_or_default();
    let service_name = set(SERVICE_NAME_ENV)
        .or_else(|| {
            resource_attributes
                .iter()
                .find(|(k, _)| k == "service.name")
                .map(|(_, v)| v.clone())
        })
        .unwrap_or_else(|| default_service.to_owned());
    Ok(Settings::On(Export {
        traces,
        metrics,
        metric_interval: millis("OTEL_METRIC_EXPORT_INTERVAL")?.unwrap_or(DEFAULT_METRIC_INTERVAL),
        service_name,
        resource_attributes,
        sampler: sampler(set(SAMPLER_ENV), set(SAMPLER_ARG_ENV))?,
        queue: QueueSettings {
            max_queue,
            max_batch,
            schedule_delay: millis("OTEL_BSP_SCHEDULE_DELAY")?.unwrap_or(DEFAULT_SCHEDULE_DELAY),
        },
    }))
}

/// An endpoint must be an http:// or https:// URL with a host. The error
/// names the variable and quotes the host at most, never the URL.
fn check_url(variable: &'static str, url: &str) -> Result<String, ConfigError> {
    let scheme_ok = url.starts_with("http://") || url.starts_with("https://");
    let parsed = url.parse::<hyper::Uri>();
    match parsed {
        Ok(uri) if scheme_ok && uri.host().is_some_and(|h| !h.is_empty()) => Ok(url.to_owned()),
        _ => Err(invalid(
            variable,
            format!(
                "must be an http:// or https:// URL with a host (got one for host {:?})",
                endpoint_display(url).split('/').next().unwrap_or_default()
            ),
        )),
    }
}

/// `key=value,...` with percent-encoded values (the W3C baggage form the
/// spec uses). `None` when the variable is unset. An error names the
/// variable only: the values are credentials. With `http`, each pair must
/// also be a valid HTTP header, which the exporters would otherwise drop or
/// refuse on every request (review of #693).
fn headers(
    variable: &'static str,
    env: &impl Fn(&str) -> Option<String>,
    http: bool,
) -> Result<Option<Vec<(String, String)>>, ConfigError> {
    let Some(raw) = env(variable).filter(|v| !v.trim().is_empty()) else {
        return Ok(None);
    };
    let mut parsed = Vec::new();
    for pair in raw.split(',').filter(|p| !p.trim().is_empty()) {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(invalid(
                variable,
                "must be key=value pairs separated by commas (value not shown)",
            ));
        };
        let key = key.trim();
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_graphic() && b != b':') {
            return Err(invalid(
                variable,
                "a header name is empty or not a valid token (value not shown)",
            ));
        }
        let value = percent_decode(value.trim());
        if http
            && (hyper::header::HeaderName::from_bytes(key.as_bytes()).is_err()
                || hyper::header::HeaderValue::from_str(&value).is_err())
        {
            return Err(invalid(
                variable,
                "a header name or value is not valid in HTTP: names are ASCII tokens, \
                 values visible ASCII (value not shown)",
            ));
        }
        parsed.push((key.to_owned(), value));
    }
    Ok(Some(parsed))
}

fn percent_decode(s: &str) -> String {
    let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(hi * 16 + lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn sampler(name: Option<String>, arg: Option<String>) -> Result<Sampler, ConfigError> {
    let ratio = || -> Result<f64, ConfigError> {
        match &arg {
            None => Ok(1.0),
            Some(a) => a
                .parse::<f64>()
                .ok()
                .filter(|r| (0.0..=1.0).contains(r))
                .ok_or_else(|| invalid(SAMPLER_ARG_ENV, "must be a number from 0 to 1")),
        }
    };
    Ok(match name.map(|n| n.to_ascii_lowercase()).as_deref() {
        None | Some("parentbased_always_on") => Sampler::ParentBasedAlwaysOn,
        Some("always_on") => Sampler::AlwaysOn,
        Some("always_off") => Sampler::AlwaysOff,
        Some("parentbased_always_off") => Sampler::ParentBasedAlwaysOff,
        Some("traceidratio") => Sampler::TraceIdRatio(ratio()?),
        Some("parentbased_traceidratio") => Sampler::ParentBasedTraceIdRatio(ratio()?),
        Some(other) => {
            return Err(invalid(
                SAMPLER_ENV,
                format!("{other} is not supported; the accepted values are {SAMPLERS}"),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn on(vars: &[(&str, &str)]) -> Export {
        match settings_from(env(vars), "vtop-engine") {
            Ok(Settings::On(export)) => export,
            other => panic!("export is not on for {vars:?}: {other:?}"),
        }
    }

    fn error(vars: &[(&str, &str)]) -> ConfigError {
        match settings_from(env(vars), "vtop-engine") {
            Err(e) => e,
            other => panic!("{vars:?} was accepted: {other:?}"),
        }
    }

    #[test]
    fn without_an_endpoint_export_is_off_and_says_what_to_set() {
        assert_eq!(
            settings_from(env(&[]), "vtop-engine"),
            Ok(Settings::Off("OTEL_EXPORTER_OTLP_ENDPOINT unset".into()))
        );
        assert_eq!(
            settings_from(env(&[(ENDPOINT_ENV, "  ")]), "vtop-engine"),
            Ok(Settings::Off("OTEL_EXPORTER_OTLP_ENDPOINT unset".into())),
            "a blank endpoint is no endpoint"
        );
        assert_eq!(
            settings_from(
                env(&[(ENDPOINT_ENV, "http://c:4318"), (SDK_DISABLED_ENV, "TRUE")]),
                "vtop-engine"
            ),
            Ok(Settings::Off("OTEL_SDK_DISABLED=true".into()))
        );
        assert_eq!(
            settings_from(
                env(&[
                    (ENDPOINT_ENV, "http://c:4318"),
                    (TRACES_EXPORTER_ENV, "none"),
                    (METRICS_EXPORTER_ENV, "none")
                ]),
                "vtop-engine"
            ),
            Ok(Settings::Off(
                "OTEL_TRACES_EXPORTER and OTEL_METRICS_EXPORTER are none".into()
            ))
        );
    }

    #[test]
    fn both_endpoint_forms_resolve_per_signal() {
        let base = on(&[(ENDPOINT_ENV, " http://collector:4318/\n")]);
        assert_eq!(
            base.traces.unwrap().endpoint,
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            base.metrics.unwrap().endpoint,
            "http://collector:4318/v1/metrics"
        );
        let specific = on(&[
            (ENDPOINT_ENV, "http://collector:4318"),
            (TRACES_ENDPOINT_ENV, "https://traces.example/custom"),
        ]);
        assert_eq!(
            specific.traces.unwrap().endpoint,
            "https://traces.example/custom",
            "the traces URL is used as given and wins"
        );
        assert_eq!(
            specific.metrics.unwrap().endpoint,
            "http://collector:4318/v1/metrics"
        );
        let traces_only = on(&[(TRACES_ENDPOINT_ENV, "http://t:4318/v1/traces")]);
        assert!(traces_only.metrics.is_none(), "no metrics URL, no metrics");
        let no_metrics = on(&[
            (ENDPOINT_ENV, "http://c:4318"),
            (METRICS_EXPORTER_ENV, "none"),
        ]);
        assert!(no_metrics.metrics.is_none() && no_metrics.traces.is_some());
    }

    #[test]
    fn the_defaults_are_the_specs() {
        let export = on(&[(ENDPOINT_ENV, "http://c:4318")]);
        assert_eq!(export.service_name, "vtop-engine");
        assert_eq!(export.sampler, Sampler::ParentBasedAlwaysOn);
        assert_eq!(
            export.queue,
            QueueSettings {
                max_queue: 2048,
                max_batch: 512,
                schedule_delay: Duration::from_secs(5)
            }
        );
        assert_eq!(export.traces.unwrap().timeout, Duration::from_secs(10));
        assert_eq!(export.metric_interval, Duration::from_secs(60));
        let named = on(&[
            (ENDPOINT_ENV, "http://c:4318"),
            (SERVICE_NAME_ENV, "archive-eu"),
            (SAMPLER_ENV, "parentbased_traceidratio"),
            (SAMPLER_ARG_ENV, "0.25"),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
            ("OTEL_BSP_MAX_QUEUE_SIZE", "100"),
            ("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "10"),
        ]);
        assert_eq!(named.service_name, "archive-eu");
        assert_eq!(named.sampler, Sampler::ParentBasedTraceIdRatio(0.25));
        assert_eq!(named.traces.unwrap().timeout, Duration::from_millis(2500));
        assert_eq!(named.queue.max_queue, 100);
    }

    #[test]
    fn a_setting_this_build_cannot_honour_is_an_error_naming_the_variable() {
        type Case<'a> = (&'a [(&'a str, &'a str)], &'a str, &'a str);
        let cases: &[Case] = &[
            (
                &[(ENDPOINT_ENV, "collector:4318")],
                ENDPOINT_ENV,
                "http:// or https://",
            ),
            (
                &[(TRACES_ENDPOINT_ENV, "ftp://c/x")],
                TRACES_ENDPOINT_ENV,
                "http:// or https://",
            ),
            (
                &[(ENDPOINT_ENV, "http://c:4318"), (SAMPLER_ENV, "sometimes")],
                SAMPLER_ENV,
                "parentbased_always_on",
            ),
            (
                &[
                    (ENDPOINT_ENV, "http://c:4318"),
                    (SAMPLER_ENV, "traceidratio"),
                    (SAMPLER_ARG_ENV, "2"),
                ],
                SAMPLER_ARG_ENV,
                "from 0 to 1",
            ),
            (
                &[
                    (ENDPOINT_ENV, "http://c:4318"),
                    ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
                ],
                "OTEL_EXPORTER_OTLP_PROTOCOL",
                "http/protobuf",
            ),
            (
                &[
                    (ENDPOINT_ENV, "http://c:4318"),
                    ("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip"),
                ],
                "OTEL_EXPORTER_OTLP_COMPRESSION",
                "not supported",
            ),
            (
                &[
                    (ENDPOINT_ENV, "http://c:4318"),
                    (METRICS_EXPORTER_ENV, "prometheus"),
                ],
                METRICS_EXPORTER_ENV,
                "otlp or none",
            ),
            (
                &[
                    (ENDPOINT_ENV, "http://c:4318"),
                    ("OTEL_EXPORTER_OTLP_TIMEOUT", "soon"),
                ],
                "OTEL_EXPORTER_OTLP_TIMEOUT",
                "milliseconds",
            ),
        ];
        for (vars, variable, accepted) in cases {
            let e = error(vars);
            assert_eq!(e.variable, *variable, "{vars:?}");
            assert!(e.to_string().starts_with(variable), "{e}");
            assert!(e.message.contains(accepted), "{vars:?}: {e}");
        }
    }

    #[test]
    fn a_bad_endpoint_or_header_is_reported_without_its_secret() {
        let e = error(&[(ENDPOINT_ENV, "collector:4318/?token=s3cret")]);
        assert!(!e.to_string().contains("s3cret"), "{e}");
        let e = error(&[
            (ENDPOINT_ENV, "http://c:4318"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "authorization Bearer s3cret"),
        ]);
        assert_eq!(e.variable, "OTEL_EXPORTER_OTLP_HEADERS");
        assert!(!e.to_string().contains("s3cret"), "{e}");
    }

    /// Review of #693: with no endpoint, export is off whatever else is
    /// set. A fleet that sets these for other software must not stop
    /// starting because this release reads them.
    #[test]
    fn without_an_endpoint_other_otel_settings_are_not_checked() {
        for vars in [
            &[(METRICS_EXPORTER_ENV, "prometheus")][..],
            &[(TRACES_EXPORTER_ENV, "otlp,console")][..],
            &[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")][..],
            &[("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")][..],
            &[(SAMPLER_ENV, "sometimes")][..],
        ] {
            assert_eq!(
                settings_from(env(vars), "vtop-engine"),
                Ok(Settings::Off("OTEL_EXPORTER_OTLP_ENDPOINT unset".into())),
                "{vars:?}"
            );
        }
    }

    /// Review of #693: a header the HTTP client would drop or refuse is a
    /// startup error, not a 401 on every export.
    #[test]
    fn a_header_that_is_not_valid_http_is_refused_without_its_value() {
        for raw in ["x(y=1", "k=a%0Ab-s3cret", "k=a%7Fb-s3cret"] {
            let e = error(&[
                (ENDPOINT_ENV, "http://c:4318"),
                ("OTEL_EXPORTER_OTLP_HEADERS", raw),
            ]);
            assert_eq!(e.variable, "OTEL_EXPORTER_OTLP_HEADERS", "{raw}");
            assert!(!e.to_string().contains("s3cret"), "{e}");
        }
        // Resource attributes are not headers: any UTF-8 value is fine.
        assert_eq!(
            on(&[
                (ENDPOINT_ENV, "http://c:4318"),
                (RESOURCE_ATTRIBUTES_ENV, "deployment.region=s%C3%A3o-paulo"),
            ])
            .resource_attributes,
            vec![("deployment.region".to_owned(), "são-paulo".to_owned())]
        );
    }

    /// Review of #693: the queue and batch sizes are bounded, so a typo
    /// cannot allocate gigabytes at startup.
    #[test]
    fn an_absurd_queue_size_is_refused() {
        for var in ["OTEL_BSP_MAX_QUEUE_SIZE", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"] {
            let e = error(&[(ENDPOINT_ENV, "http://c:4318"), (var, "100000000")]);
            assert_eq!(e.variable, var);
            assert!(e.message.contains("1000000"), "{e}");
        }
    }

    #[test]
    fn headers_are_parsed_and_percent_decoded() {
        let export = on(&[
            (ENDPOINT_ENV, "http://c:4318"),
            (
                "OTEL_EXPORTER_OTLP_HEADERS",
                "authorization=Bearer%20abc, x-scope=tenant-1",
            ),
            ("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "x-metrics=1"),
        ]);
        assert_eq!(
            export.traces.unwrap().headers,
            vec![
                ("authorization".to_owned(), "Bearer abc".to_owned()),
                ("x-scope".to_owned(), "tenant-1".to_owned())
            ]
        );
        assert_eq!(
            export.metrics.unwrap().headers,
            vec![("x-metrics".to_owned(), "1".to_owned())],
            "the signal's own variable wins"
        );
    }

    #[test]
    fn the_enabled_line_names_host_path_protocol_sampler_and_service_only() {
        let export = on(&[
            (ENDPOINT_ENV, "https://user:pw@otel.example:4318/base"),
            (
                TRACES_ENDPOINT_ENV,
                "https://user:pw@otel.example:4318/base?token=s3cret",
            ),
            (
                "OTEL_EXPORTER_OTLP_HEADERS",
                "authorization=Bearer%20hdr-secret",
            ),
        ]);
        let line = export.describe();
        for secret in [
            "pw@",
            "user",
            "s3cret",
            "token",
            "hdr-secret",
            "authorization",
        ] {
            assert!(!line.contains(secret), "{secret} in: {line}");
        }
        assert_eq!(
            line,
            "OpenTelemetry export enabled: traces to otel.example:4318/base, metrics to \
             otel.example:4318/base/v1/metrics, protocol http/protobuf, sampler \
             parentbased_always_on, \
             service.name vtop-engine"
        );
    }
}

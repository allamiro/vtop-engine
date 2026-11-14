//! OTLP metrics: the process's Prometheus registry, sent as OTLP.
//!
//! A bridge rather than a second set of instruments. `/metrics` stays the
//! source of truth and is unchanged; every interval this reads the same
//! registry (`Registry::gather`) and sends each family as an OTLP metric of
//! the same name, labels and values: a counter as a cumulative monotonic
//! sum, a gauge as a gauge, a histogram as a cumulative histogram with the
//! same bucket bounds. Instrumenting twice would have let the two disagree.
#![expect(
    clippy::field_reassign_with_default,
    reason = "prost messages are built field by field: their field sets change between \
              opentelemetry-proto releases, which struct literals would turn into build breaks"
)]

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, AggregationTemporality, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Registry;
use prost::Message;

use super::http::OnRuntime;
use super::redact::RateLimited;
use super::settings::Signal;
use super::stats::stats;

/// The registry the metrics export reads. Set by the binary once it has one.
static REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);

pub(crate) fn set_registry(registry: &Registry) {
    *REGISTRY.lock().unwrap_or_else(|e| e.into_inner()) = Some(registry.clone());
}

fn gather() -> Vec<MetricFamily> {
    REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map(Registry::gather)
        .unwrap_or_default()
}

fn unix_nanos(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

fn kv(key: &str, value: &str) -> KeyValue {
    let mut any = AnyValue::default();
    any.value = Some(any_value::Value::StringValue(value.to_owned()));
    let mut kv = KeyValue::default();
    kv.key = key.to_owned();
    kv.value = Some(any);
    kv
}

/// One OTLP request for `families`, and how many data points it carries.
pub(crate) fn request(
    families: &[MetricFamily],
    resource: &[(String, String)],
    started: SystemTime,
    now: SystemTime,
) -> (ExportMetricsServiceRequest, usize) {
    let start = unix_nanos(started);
    let time = unix_nanos(now);
    let mut points = 0;
    let mut metrics = Vec::new();
    for family in families {
        let mut sum_points = Vec::new();
        let mut histogram_points = Vec::new();
        for m in family.get_metric() {
            let attributes: Vec<KeyValue> = m
                .get_label()
                .iter()
                .map(|l| kv(l.name(), l.value()))
                .collect();
            let number = |value: f64| {
                let mut p = NumberDataPoint::default();
                p.attributes = attributes.clone();
                p.start_time_unix_nano = start;
                p.time_unix_nano = time;
                p.value = Some(number_data_point::Value::AsDouble(value));
                p
            };
            match family.get_field_type() {
                MetricType::COUNTER => sum_points.push(number(m.get_counter().get_value())),
                MetricType::GAUGE => sum_points.push(number(m.get_gauge().get_value())),
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let mut p = HistogramDataPoint::default();
                    p.attributes = attributes.clone();
                    p.start_time_unix_nano = start;
                    p.time_unix_nano = time;
                    p.count = h.get_sample_count();
                    p.sum = Some(h.get_sample_sum());
                    // Prometheus buckets are cumulative; OTLP's are per bucket,
                    // with one more count than bounds (the +Inf bucket).
                    let mut previous = 0;
                    for b in h.get_bucket() {
                        if b.upper_bound().is_infinite() {
                            continue;
                        }
                        p.explicit_bounds.push(b.upper_bound());
                        p.bucket_counts
                            .push(b.cumulative_count().saturating_sub(previous));
                        previous = b.cumulative_count();
                    }
                    p.bucket_counts
                        .push(h.get_sample_count().saturating_sub(previous));
                    histogram_points.push(p);
                }
                // Nothing in either registry is a summary or untyped; skipped
                // rather than sent as something it is not.
                MetricType::SUMMARY | MetricType::UNTYPED => {}
            }
        }
        let data = match family.get_field_type() {
            MetricType::COUNTER if !sum_points.is_empty() => {
                points += sum_points.len();
                let mut s = Sum::default();
                s.data_points = sum_points;
                s.aggregation_temporality = AggregationTemporality::Cumulative as i32;
                s.is_monotonic = true;
                Some(metric::Data::Sum(s))
            }
            MetricType::GAUGE if !sum_points.is_empty() => {
                points += sum_points.len();
                let mut g = Gauge::default();
                g.data_points = sum_points;
                Some(metric::Data::Gauge(g))
            }
            MetricType::HISTOGRAM if !histogram_points.is_empty() => {
                points += histogram_points.len();
                let mut h = Histogram::default();
                h.data_points = histogram_points;
                h.aggregation_temporality = AggregationTemporality::Cumulative as i32;
                Some(metric::Data::Histogram(h))
            }
            _ => None,
        };
        if let Some(data) = data {
            let mut metric = Metric::default();
            metric.name = family.name().to_owned();
            metric.description = family.help().to_owned();
            metric.data = Some(data);
            metrics.push(metric);
        }
    }
    let mut scope = InstrumentationScope::default();
    scope.name = "vtop".to_owned();
    scope.version = env!("CARGO_PKG_VERSION").to_owned();
    let mut scope_metrics = ScopeMetrics::default();
    scope_metrics.scope = Some(scope);
    scope_metrics.metrics = metrics;
    let mut res = Resource::default();
    res.attributes = resource.iter().map(|(k, v)| kv(k, v)).collect();
    let mut resource_metrics = ResourceMetrics::default();
    resource_metrics.resource = Some(res);
    resource_metrics.scope_metrics = vec![scope_metrics];
    let mut request = ExportMetricsServiceRequest::default();
    request.resource_metrics = vec![resource_metrics];
    (request, points)
}

/// Sends the registry every interval until stopped, then once more.
pub(crate) struct Pusher {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Option<Result<usize, String>>>,
}

struct Sender {
    signal: Signal,
    client: OnRuntime,
    resource: Vec<(String, String)>,
    started: SystemTime,
    failures: RateLimited,
}

impl Sender {
    /// One export. `Ok` is the number of points sent.
    async fn push(&self) -> Result<usize, String> {
        let families = gather();
        let (request, points) = request(&families, &self.resource, self.started, SystemTime::now());
        if points == 0 {
            return Ok(0);
        }
        let s = stats();
        let sent = tokio::time::timeout(
            self.signal.timeout,
            self.client.post(
                &self.signal.endpoint,
                &self.signal.headers,
                request.encode_to_vec(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            Err(format!(
                "no answer within {} ms (OTEL_METRIC_EXPORT_TIMEOUT)",
                self.signal.timeout.as_millis()
            ))
        });
        match sent {
            Ok(()) => {
                s.metric_points_exported.inc_by(points as u64);
                Ok(points)
            }
            Err(error) => {
                s.metric_points_dropped.inc_by(points as u64);
                s.export_failures.with_label_values(&["metrics"]).inc();
                if let Some(dropped) = self.failures.note(points as u64) {
                    tracing::warn!(
                        error = %error,
                        dropped_since_last_warning = dropped,
                        "OpenTelemetry metrics export failed: the collector refused or could \
                         not be reached, so these data points are dropped; batches are \
                         unaffected, and /metrics still serves them. Check the collector and \
                         OTEL_EXPORTER_OTLP_ENDPOINT"
                    );
                }
                Err(error)
            }
        }
    }
}

impl Pusher {
    pub(crate) fn start(
        signal: Signal,
        interval: Duration,
        client: OnRuntime,
        resource: Vec<(String, String)>,
        started: SystemTime,
    ) -> Self {
        let sender = Sender {
            signal,
            client,
            resource,
            started,
            failures: RateLimited::new(Duration::from_secs(60)),
        };
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            loop {
                tokio::select! {
                    _ = &mut stopped => return Some(sender.push().await),
                    _ = ticks.tick() => { let _ = sender.push().await; }
                }
            }
        });
        Self { stop, task }
    }

    /// Stop, sending the registry one last time. Logs the outcome.
    pub(crate) async fn finish(self, deadline: Duration) {
        let _ = self.stop.send(());
        match tokio::time::timeout(deadline, self.task).await {
            Ok(Ok(Some(Ok(points)))) => tracing::info!(
                points,
                "OpenTelemetry metrics flush: exported {points} data points"
            ),
            Ok(Ok(Some(Err(_)))) => tracing::warn!(
                "OpenTelemetry metrics flush: the last export failed (see the warning above); \
                 exiting without it"
            ),
            Ok(_) => {}
            Err(_) => tracing::warn!(
                "OpenTelemetry metrics flush gave up after {}s: the collector did not answer \
                 in time; exiting without it",
                deadline.as_secs()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts};

    #[test]
    fn the_registry_becomes_otlp_with_the_same_names_labels_and_values() {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        let counter =
            IntCounterVec::new(Opts::new("records_total", "records"), &["tenant"]).unwrap();
        let gauge = IntGauge::new("upload_width", "width").unwrap();
        let histogram = HistogramVec::new(
            HistogramOpts::new("batch_duration_seconds", "batch").buckets(vec![0.1, 1.0]),
            &["tenant"],
        )
        .unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        registry.register(Box::new(gauge.clone())).unwrap();
        registry.register(Box::new(histogram.clone())).unwrap();
        counter.with_label_values(&["default"]).inc_by(7);
        gauge.set(4);
        for v in [0.05, 0.5, 0.5, 3.0] {
            histogram.with_label_values(&["default"]).observe(v);
        }

        let (request, points) = request(
            &registry.gather(),
            &[("service.name".into(), "vtop-engine".into())],
            UNIX_EPOCH,
            UNIX_EPOCH + Duration::from_secs(1),
        );
        assert_eq!(points, 3);
        let rm = &request.resource_metrics[0];
        assert_eq!(
            rm.resource.as_ref().unwrap().attributes[0].key,
            "service.name"
        );
        let metrics = &rm.scope_metrics[0].metrics;
        let by_name = |name: &str| metrics.iter().find(|m| m.name == name).unwrap();

        let Some(Data::Sum(sum)) = &by_name("vtop_records_total").data else {
            panic!("a counter is not a sum")
        };
        assert!(sum.is_monotonic);
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Cumulative as i32
        );
        assert_eq!(
            sum.data_points[0].value,
            Some(number_data_point::Value::AsDouble(7.0))
        );
        assert_eq!(sum.data_points[0].attributes[0].key, "tenant");

        let Some(Data::Gauge(g)) = &by_name("vtop_upload_width").data else {
            panic!("a gauge is not a gauge")
        };
        assert_eq!(
            g.data_points[0].value,
            Some(number_data_point::Value::AsDouble(4.0))
        );

        let Some(Data::Histogram(h)) = &by_name("vtop_batch_duration_seconds").data else {
            panic!("a histogram is not a histogram")
        };
        let p = &h.data_points[0];
        assert_eq!(p.count, 4);
        assert_eq!(p.sum, Some(4.05));
        assert_eq!(p.explicit_bounds, vec![0.1, 1.0]);
        assert_eq!(p.bucket_counts, vec![1, 2, 1], "per bucket, +Inf last");
    }
}

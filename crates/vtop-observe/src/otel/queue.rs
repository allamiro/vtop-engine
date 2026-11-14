//! The span queue between the batch path and the collector.
//!
//! The SDK's own batch processor would do the batching, but it says nothing
//! about what it drops or how much it holds, and the operator needs both: a
//! counter of spans lost and why, a gauge of what is waiting, a warning when
//! the collector cannot keep up, and at shutdown the number flushed or given
//! up on. So this is a small processor of its own with the same contract:
//!
//! * `on_end` never blocks and never allocates beyond the capacity: a span
//!   that finds `max_queue` spans already waiting is dropped and counted;
//! * one thread exports, in batches of up to `max_batch`, at the latest
//!   `schedule_delay` after a span arrives;
//! * a failed export is counted and its spans dropped, never retried into a
//!   queue that would then grow without bound (the OTLP exporter retries
//!   transient failures itself, within its timeout).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::Context;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Span, SpanData, SpanExporter, SpanProcessor};
use opentelemetry_sdk::Resource;

use super::redact::{redact, RateLimited};
use super::settings::QueueSettings;
use super::stats::stats;

/// At most one warning a minute for each kind of loss.
const WARN_INTERVAL: Duration = Duration::from_secs(60);

/// What a flush did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// Spans the collector accepted during the flush.
    pub exported: usize,
    /// Spans in exports that failed during the flush.
    pub failed: usize,
}

#[expect(
    clippy::large_enum_variant,
    reason = "a span is the message sent for every span; boxing it would add an allocation \
              per span to move a value the channel moves anyway"
)]
enum Msg {
    Span(SpanData),
    Resource(Resource),
    Flush {
        reply: mpsc::SyncSender<FlushReport>,
        last: bool,
    },
}

struct Shared {
    /// Spans accepted and not yet exported or dropped: in the channel, in
    /// the batch being built, or in flight. This is what the capacity bounds.
    pending: AtomicUsize,
    capacity: usize,
    closed: AtomicBool,
    queue_full: RateLimited,
}

/// The processor. See the module documentation.
pub struct ExportQueue {
    shared: Arc<Shared>,
    sender: Mutex<mpsc::Sender<Msg>>,
}

impl std::fmt::Debug for ExportQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportQueue")
            .field("capacity", &self.shared.capacity)
            .finish()
    }
}

/// A view of a queue's fill level, kept by whoever built the queue (the
/// provider owns the queue itself).
#[derive(Clone)]
pub struct QueueProbe(Arc<Shared>);

impl QueueProbe {
    /// Spans accepted and not yet exported or dropped.
    pub fn pending(&self) -> usize {
        self.0.pending.load(Ordering::SeqCst)
    }
}

impl ExportQueue {
    /// A probe of this queue's fill level.
    pub fn probe(&self) -> QueueProbe {
        QueueProbe(Arc::clone(&self.shared))
    }

    /// Start the export thread.
    pub fn new<E: SpanExporter + 'static>(exporter: E, settings: QueueSettings) -> Self {
        let shared = Arc::new(Shared {
            pending: AtomicUsize::new(0),
            capacity: settings.max_queue,
            closed: AtomicBool::new(false),
            queue_full: RateLimited::new(WARN_INTERVAL),
        });
        let (sender, receiver) = mpsc::channel();
        let worker = Worker {
            exporter,
            shared: Arc::clone(&shared),
            settings,
            failures: RateLimited::new(WARN_INTERVAL),
        };
        // A thread, not a task: the exporter's future is driven with
        // `block_on`, and that must not happen on a runtime worker.
        let spawned = std::thread::Builder::new()
            .name("vtop-otel-export".into())
            .spawn(move || worker.run(receiver));
        if let Err(e) = spawned {
            tracing::error!(
                error = %e,
                "OpenTelemetry trace export thread could not start; spans will be dropped"
            );
            shared.closed.store(true, Ordering::SeqCst);
        }
        Self {
            shared,
            sender: Mutex::new(sender),
        }
    }

    fn send(&self, msg: Msg) -> bool {
        self.sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(msg)
            .is_ok()
    }

    fn flush(&self, timeout: Duration, last: bool) -> Result<FlushReport, usize> {
        let (reply, answer) = mpsc::sync_channel(1);
        if !self.send(Msg::Flush { reply, last }) {
            return Err(self.shared.pending.load(Ordering::SeqCst));
        }
        answer
            .recv_timeout(timeout)
            .map_err(|_| self.shared.pending.load(Ordering::SeqCst))
    }
}

impl SpanProcessor for ExportQueue {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        let s = stats();
        if self.shared.closed.load(Ordering::SeqCst) {
            s.spans_dropped.with_label_values(&["shutdown"]).inc();
            return;
        }
        let before = self.shared.pending.fetch_add(1, Ordering::SeqCst);
        if before >= self.shared.capacity {
            self.shared.pending.fetch_sub(1, Ordering::SeqCst);
            s.spans_dropped.with_label_values(&["queue_full"]).inc();
            if let Some(dropped) = self.shared.queue_full.note(1) {
                tracing::warn!(
                    capacity = self.shared.capacity,
                    dropped_since_last_warning = dropped,
                    "OpenTelemetry export queue full: the collector is not keeping up, so \
                     new spans are dropped; batches are unaffected. Check the collector, or \
                     raise OTEL_BSP_MAX_QUEUE_SIZE"
                );
            }
            return;
        }
        s.queue_length.inc();
        if !self.send(Msg::Span(span)) {
            self.shared.pending.fetch_sub(1, Ordering::SeqCst);
            s.queue_length.dec();
            s.spans_dropped.with_label_values(&["shutdown"]).inc();
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        match self.flush(Duration::from_secs(5), false) {
            Ok(_) => Ok(()),
            Err(_) => Err(OTelSdkError::Timeout(Duration::from_secs(5))),
        }
    }

    /// Flush what is queued, then stop. Logs the outcome: an INFO with the
    /// number flushed, or a WARN with what was given up on.
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        if self.shared.closed.swap(true, Ordering::SeqCst) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let started = Instant::now();
        match self.flush(timeout, true) {
            Ok(report) if report.failed == 0 => {
                tracing::info!(
                    spans = report.exported,
                    "OpenTelemetry trace flush: flushed {} spans",
                    report.exported
                );
                Ok(())
            }
            Ok(report) => {
                tracing::warn!(
                    exported = report.exported,
                    dropped = report.failed,
                    "OpenTelemetry trace flush: flushed {} spans, {} dropped because the \
                     collector refused or could not be reached",
                    report.exported,
                    report.failed
                );
                Err(OTelSdkError::InternalFailure(
                    "spans dropped at shutdown".into(),
                ))
            }
            Err(abandoned) => {
                stats()
                    .spans_dropped
                    .with_label_values(&["shutdown"])
                    .inc_by(abandoned as u64);
                tracing::warn!(
                    dropped = abandoned,
                    "OpenTelemetry trace flush gave up after {}s, {} spans dropped: the \
                     collector did not answer in time; exiting without them",
                    started.elapsed().as_secs(),
                    abandoned
                );
                Err(OTelSdkError::Timeout(timeout))
            }
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.send(Msg::Resource(resource.clone()));
    }
}

struct Worker<E> {
    exporter: E,
    shared: Arc<Shared>,
    settings: QueueSettings,
    failures: RateLimited,
}

impl<E: SpanExporter> Worker<E> {
    fn run(mut self, receiver: mpsc::Receiver<Msg>) {
        let mut batch: Vec<SpanData> = Vec::with_capacity(self.settings.max_batch);
        let mut due: Option<Instant> = None;
        loop {
            let wait = due.map_or(Duration::from_secs(3600), |d| {
                d.saturating_duration_since(Instant::now())
            });
            match receiver.recv_timeout(wait) {
                Ok(Msg::Span(span)) => {
                    batch.push(span);
                    due.get_or_insert_with(|| Instant::now() + self.settings.schedule_delay);
                    if batch.len() >= self.settings.max_batch {
                        self.export(&mut batch);
                        due = None;
                    }
                }
                Ok(Msg::Resource(resource)) => self.exporter.set_resource(&resource),
                Ok(Msg::Flush { reply, last }) => {
                    // Everything sent before the flush is already in `batch`:
                    // the channel is FIFO.
                    let report = self.export(&mut batch);
                    due = None;
                    if last {
                        let _ = self.exporter.shutdown();
                        let _ = reply.send(report);
                        return;
                    }
                    let _ = reply.send(report);
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.export(&mut batch);
                    due = None;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.export(&mut batch);
                    return;
                }
            }
        }
    }

    fn export(&mut self, batch: &mut Vec<SpanData>) -> FlushReport {
        let mut report = FlushReport::default();
        let s = stats();
        let mut rest = std::mem::take(batch);
        while !rest.is_empty() {
            let tail = rest.split_off(rest.len().min(self.settings.max_batch.max(1)));
            let chunk = std::mem::replace(&mut rest, tail);
            let n = chunk.len();
            let result = futures::executor::block_on(self.exporter.export(chunk));
            self.shared.pending.fetch_sub(n, Ordering::SeqCst);
            s.queue_length.sub(n as i64);
            match result {
                Ok(()) => {
                    s.spans_exported.inc_by(n as u64);
                    report.exported += n;
                }
                Err(e) => {
                    s.spans_dropped
                        .with_label_values(&["export_failed"])
                        .inc_by(n as u64);
                    s.export_failures.with_label_values(&["traces"]).inc();
                    report.failed += n;
                    if let Some(dropped) = self.failures.note(n as u64) {
                        tracing::warn!(
                            error = %redact(&e.to_string()),
                            dropped_since_last_warning = dropped,
                            "OpenTelemetry trace export failed: the collector refused or \
                             could not be reached, so these spans are dropped; batches are \
                             unaffected. Check the collector and OTEL_EXPORTER_OTLP_ENDPOINT"
                        );
                    }
                }
            }
        }
        report
    }
}

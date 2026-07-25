//! Fetch I/O research harness (#190).
//!
//! ## What this measures
//!
//! | Engine | Role |
//! |---|---|
//! | `buffered_page_cache` | Current production path baseline (`SegmentReader::fetch`) |
//! | `sendfile` | Linux probe: kernel `sendfile` of sealed segment bytes |
//! | `splice` | Linux probe: `splice` through a pipe (zero-copy pipe path) |
//! | `odirect` | Experimental Linux probe: `O_DIRECT` aligned reads |
//! | `io_uring` | Deferred probe slot (no dependency; status only) |
//!
//! Workloads: hot, cold (best-effort cache drop), sequential catch-up,
//! concurrent same/disjoint consumers, plain vs TLS-proxy transport cost.
//!
//! This is a **research** harness. It does **not** ship three production fetch
//! engines. Epic #93: only pursue O_DIRECT / io_uring after buffered I/O is
//! shown to be the bottleneck.
//!
//! ## How to run
//!
//! ```text
//! # CI suite (also covered by `cargo test --workspace --locked`)
//! cargo test -p vtop-log --test fetch_io_research_harness --locked
//!
//! # Emit machine-readable JSON (directory is created if missing)
//! VTOP_FETCH_IO_JSON=benchmarks/results/native-fetch-io/summary.json \
//!   cargo test -p vtop-log --test fetch_io_research_harness --locked -- --nocapture
//! ```
//!
//! Methodology: `docs/FETCH_IO_RESEARCH.md`. Complements archive matrix
//! issues #92 / #98 / #130 — does not claim Kafka superiority.

use serde::Serialize;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;
use vtop_log::{
    ActiveSegment, Durability, LogRecord, RangeLineage, SegmentConfig, SegmentDescriptor,
    SegmentReader,
};

const SEED: u64 = 0x5eed_0190;
const PRODUCER: Uuid = Uuid::from_u128(0xB190);
const PAYLOAD_BYTES: usize = 1024;
const RECORD_COUNT: u64 = 256;
const FETCH_MAX_BYTES: usize = 64 * 1024;
const FETCH_MAX_RECORDS: usize = 32;
const CONCURRENT_CONSUMERS: usize = 4;
const HARNESS_VERSION: &str = "1";
const ISSUE: &str = "190";
const METHODOLOGY: &str = "docs/FETCH_IO_RESEARCH.md";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Engine {
    BufferedPageCache,
    Sendfile,
    Splice,
    Odirect,
    IoUring,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Workload {
    Hot,
    Cold,
    SequentialCatchup,
    ConcurrentSame,
    ConcurrentDisjoint,
    PlainTransport,
    TlsProxy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbeStatus {
    Measured,
    SkippedUnavailable,
    Deferred,
}

#[derive(Clone, Debug, Serialize)]
struct LatencyMs {
    p50: f64,
    p95: f64,
    p99: f64,
    samples: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioReport {
    name: String,
    engine: Engine,
    workload: Workload,
    status: ProbeStatus,
    skip_reason: Option<String>,
    fetched_logical_bytes: u64,
    fetch_calls: u64,
    elapsed_ms: f64,
    throughput_mib_per_sec: f64,
    latency_ms: Option<LatencyMs>,
    cpu_user_ms: f64,
    cpu_sys_ms: f64,
    /// `(user+sys) ms / GiB fetched`. Proxy when OS CPU accounting is coarse.
    cpu_ms_per_gib: f64,
    disk_read_bytes: Option<u64>,
    disk_read_amp: Option<f64>,
    estimated_buffer_bytes: u64,
    notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct HwmCheck {
    name: String,
    passed: bool,
    detail: String,
}

#[derive(Clone, Debug, Serialize)]
struct Recommendation {
    default_path: String,
    pursue_odirect_io_uring: bool,
    gate: String,
    rationale: String,
    lab_limits: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct HarnessReport {
    harness_version: String,
    issue: String,
    seed: u64,
    host: HostInfo,
    methodology: String,
    caveats: Vec<String>,
    hwm_checks: Vec<HwmCheck>,
    scenarios: Vec<ScenarioReport>,
    recommendation: Recommendation,
}

#[derive(Clone, Debug, Serialize)]
struct HostInfo {
    os: String,
    arch: String,
    /// True when Linux-only probes (sendfile/splice/O_DIRECT,/proc) ran.
    linux_probes_enabled: bool,
}

struct SegmentFixture {
    _dir: TempDir,
    segment_path: PathBuf,
    high_watermark: u64,
    sealed_file_bytes: u64,
}

struct CpuSample {
    user_ms: f64,
    sys_ms: f64,
}

#[derive(Clone, Copy)]
enum TransportMode {
    None,
    PlainCopy,
    TlsProxy,
}

fn descriptor() -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(0x190),
        topic: "events.fetch-io".to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage::root(Uuid::from_u128(0x0190_0001)),
        base_offset: 0,
    }
}

fn config() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 64 * 1024,
        max_group_bytes: 1024 * 1024,
        max_segment_bytes: 32 * 1024 * 1024,
        max_segment_records: 100_000,
        index_stride: 32,
    }
}

fn record(sequence: u64) -> LogRecord {
    let mut value = vec![0_u8; PAYLOAD_BYTES];
    let seq = sequence.to_le_bytes();
    value[..8].copy_from_slice(&seq);
    LogRecord {
        producer_id: PRODUCER,
        producer_epoch: 0,
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        attributes: 0,
        key: b"k".to_vec(),
        value,
    }
}

fn build_sealed_segment() -> SegmentFixture {
    let dir = TempDir::new().expect("tempdir");
    let active = dir.path().join("fetch-io.active");
    let segment_path = dir.path().join("fetch-io.segment");
    let mut segment = ActiveSegment::create(&active, descriptor(), config()).expect("create");
    for seq in 0..RECORD_COUNT {
        segment
            .append(record(seq), Durability::Fsync)
            .expect("append");
    }
    let high_watermark = segment.committed_offset();
    drop(segment.seal().expect("seal"));
    let sealed_file_bytes = fs::metadata(&segment_path).expect("sealed metadata").len();
    SegmentFixture {
        _dir: dir,
        segment_path,
        high_watermark,
        sealed_file_bytes,
    }
}

fn open_reader(path: &Path) -> SegmentReader {
    SegmentReader::open(path).expect("open sealed segment")
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

fn latency_from(mut samples: Vec<f64>) -> LatencyMs {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    LatencyMs {
        p50: percentile(&samples, 0.50),
        p95: percentile(&samples, 0.95),
        p99: percentile(&samples, 0.99),
        samples: samples.len(),
    }
}

fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64().max(1e-9);
    (bytes as f64 / (1024.0 * 1024.0)) / secs
}

fn cpu_ms_per_gib(cpu_ms: f64, bytes: u64) -> f64 {
    if bytes == 0 {
        0.0
    } else {
        cpu_ms / (bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[cfg(unix)]
fn sample_cpu() -> CpuSample {
    // SAFETY: getrusage(RUSAGE_SELF) writes into a local rusage.
    unsafe {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return CpuSample {
                user_ms: 0.0,
                sys_ms: 0.0,
            };
        }
        let usage = usage.assume_init();
        CpuSample {
            user_ms: timeval_ms(usage.ru_utime),
            sys_ms: timeval_ms(usage.ru_stime),
        }
    }
}

#[cfg(not(unix))]
fn sample_cpu() -> CpuSample {
    CpuSample {
        user_ms: 0.0,
        sys_ms: 0.0,
    }
}

#[cfg(unix)]
fn timeval_ms(tv: libc::timeval) -> f64 {
    (tv.tv_sec as f64) * 1000.0 + (tv.tv_usec as f64) / 1000.0
}

fn cpu_delta(before: &CpuSample, after: &CpuSample) -> (f64, f64) {
    (
        (after.user_ms - before.user_ms).max(0.0),
        (after.sys_ms - before.sys_ms).max(0.0),
    )
}

#[cfg(target_os = "linux")]
fn process_read_bytes() -> Option<u64> {
    let text = fs::read_to_string("/proc/self/io").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("read_bytes: ") {
            return rest.trim().parse().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn process_read_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn advise_dontneed(path: &Path) -> io::Result<()> {
    use std::fs::File;
    use std::os::fd::AsRawFd;
    let file = File::open(path)?;
    // SAFETY: posix_fadvise on an open fd; len 0 means the whole file.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn advise_dontneed(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "posix_fadvise DONTNEED unavailable on this host",
    ))
}

/// Best-effort cold-cache preparation. Linux uses `posix_fadvise`; elsewhere
/// we only reopen (page cache likely still warm — noted in the report).
fn prepare_cold(path: &Path) -> Vec<String> {
    let mut notes = Vec::new();
    match advise_dontneed(path) {
        Ok(()) => notes.push("applied posix_fadvise(POSIX_FADV_DONTNEED)".to_owned()),
        Err(err) => notes.push(format!(
            "cold cache best-effort only ({err}); page cache may still be warm"
        )),
    }
    notes
}

struct ScenarioInput<'a> {
    name: &'a str,
    engine: Engine,
    workload: Workload,
    status: ProbeStatus,
    skip_reason: Option<String>,
    fetched: u64,
    fetch_calls: u64,
    elapsed: Duration,
    latencies: Vec<f64>,
    cpu_before: &'a CpuSample,
    cpu_after: &'a CpuSample,
    disk_before: Option<u64>,
    disk_after: Option<u64>,
    estimated_buffer_bytes: u64,
    notes: Vec<String>,
}

fn finish_scenario(input: ScenarioInput<'_>) -> ScenarioReport {
    let ScenarioInput {
        name,
        engine,
        workload,
        status,
        skip_reason,
        fetched,
        fetch_calls,
        elapsed,
        latencies,
        cpu_before,
        cpu_after,
        disk_before,
        disk_after,
        estimated_buffer_bytes,
        mut notes,
    } = input;
    let (user_ms, sys_ms) = cpu_delta(cpu_before, cpu_after);
    let disk_read = match (disk_before, disk_after) {
        (Some(b), Some(a)) if a >= b => Some(a - b),
        _ => None,
    };
    if matches!(engine, Engine::Sendfile | Engine::Splice) {
        notes.push(
            "TLS terminates zero-copy: ciphertext construction needs userspace bytes, so sendfile/splice do not apply end-to-end under TLS"
                .to_owned(),
        );
    }
    ScenarioReport {
        name: name.to_owned(),
        engine,
        workload,
        status,
        skip_reason,
        fetched_logical_bytes: fetched,
        fetch_calls,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        throughput_mib_per_sec: mib_per_sec(fetched, elapsed),
        latency_ms: if latencies.is_empty() {
            None
        } else {
            Some(latency_from(latencies))
        },
        cpu_user_ms: user_ms,
        cpu_sys_ms: sys_ms,
        cpu_ms_per_gib: cpu_ms_per_gib(user_ms + sys_ms, fetched),
        disk_read_bytes: disk_read,
        disk_read_amp: disk_read.map(|d| ratio(d, fetched.max(1))),
        estimated_buffer_bytes,
        notes,
    }
}

fn skipped(
    name: &str,
    engine: Engine,
    workload: Workload,
    status: ProbeStatus,
    reason: impl Into<String>,
) -> ScenarioReport {
    ScenarioReport {
        name: name.to_owned(),
        engine,
        workload,
        status,
        skip_reason: Some(reason.into()),
        fetched_logical_bytes: 0,
        fetch_calls: 0,
        elapsed_ms: 0.0,
        throughput_mib_per_sec: 0.0,
        latency_ms: None,
        cpu_user_ms: 0.0,
        cpu_sys_ms: 0.0,
        cpu_ms_per_gib: 0.0,
        disk_read_bytes: None,
        disk_read_amp: None,
        estimated_buffer_bytes: 0,
        notes: Vec::new(),
    }
}

fn fetch_windowed(
    reader: &mut SegmentReader,
    start: u64,
    hwm: u64,
    transport: TransportMode,
) -> (u64, u64, Vec<f64>, u64) {
    let mut offset = start;
    let mut fetched = 0_u64;
    let mut calls = 0_u64;
    let mut latencies = Vec::new();
    let mut peak_buf = 0_u64;
    while offset < hwm {
        let t0 = Instant::now();
        let batch = reader
            .fetch(offset, FETCH_MAX_BYTES, FETCH_MAX_RECORDS)
            .expect("fetch");
        let mut elapsed = t0.elapsed();
        assert!(
            batch.high_watermark <= hwm,
            "fetch exposed HWM {} above fixture HWM {hwm}",
            batch.high_watermark
        );
        assert!(
            batch
                .records
                .iter()
                .all(|r| r.offset < batch.high_watermark),
            "fetch returned record at/above HWM"
        );
        match transport {
            TransportMode::PlainCopy => {
                let mut sink = vec![0_u8; batch.encoded_bytes.max(1)];
                let sink_len = sink.len();
                let t1 = Instant::now();
                for (i, rec) in batch.records.iter().enumerate() {
                    let b = rec.record.value.first().copied().unwrap_or(0);
                    sink[i % sink_len] ^= b;
                }
                elapsed += t1.elapsed();
                peak_buf = peak_buf.max(sink_len as u64);
            }
            TransportMode::TlsProxy => {
                let t1 = Instant::now();
                let mut hasher = blake3::Hasher::new();
                for rec in &batch.records {
                    hasher.update(&rec.record.value);
                }
                let digest = hasher.finalize();
                elapsed += t1.elapsed();
                peak_buf = peak_buf.max(batch.encoded_bytes as u64 + 32);
                let _ = digest;
            }
            TransportMode::None => {
                peak_buf = peak_buf.max(batch.encoded_bytes as u64);
            }
        }
        latencies.push(elapsed.as_secs_f64() * 1000.0);
        calls += 1;
        fetched += batch.encoded_bytes as u64;
        if batch.records.is_empty() {
            break;
        }
        offset = batch.next_offset;
    }
    (fetched, calls, latencies, peak_buf)
}

fn run_buffered(
    fixture: &SegmentFixture,
    name: &str,
    workload: Workload,
    cold: bool,
    transport: TransportMode,
) -> ScenarioReport {
    let mut notes = Vec::new();
    if cold {
        notes.extend(prepare_cold(&fixture.segment_path));
    }
    let cpu_before = sample_cpu();
    let disk_before = process_read_bytes();
    let start = Instant::now();
    let mut reader = open_reader(&fixture.segment_path);
    let (fetched, calls, latencies, peak_buf) =
        fetch_windowed(&mut reader, 0, fixture.high_watermark, transport);
    let elapsed = start.elapsed();
    let cpu_after = sample_cpu();
    let disk_after = process_read_bytes();
    match transport {
        TransportMode::TlsProxy => notes.push(
            "TLS proxy = blake3 over fetched payloads (CPU stand-in for record encryption; not a real TLS stack)"
                .to_owned(),
        ),
        TransportMode::PlainCopy => notes.push(
            "plain transport proxy = userspace touch/copy of fetched batch bytes".to_owned(),
        ),
        TransportMode::None => {}
    }
    finish_scenario(ScenarioInput {
        name,
        engine: Engine::BufferedPageCache,
        workload,
        status: ProbeStatus::Measured,
        skip_reason: None,
        fetched,
        fetch_calls: calls,
        elapsed,
        latencies,
        cpu_before: &cpu_before,
        cpu_after: &cpu_after,
        disk_before,
        disk_after,
        estimated_buffer_bytes: peak_buf,
        notes,
    })
}

fn run_buffered_concurrent(fixture: &SegmentFixture, disjoint: bool) -> ScenarioReport {
    let name = if disjoint {
        "buffered_concurrent_disjoint"
    } else {
        "buffered_concurrent_same"
    };
    let workload = if disjoint {
        Workload::ConcurrentDisjoint
    } else {
        Workload::ConcurrentSame
    };
    let hwm = fixture.high_watermark;
    let path = fixture.segment_path.clone();
    let chunk = (hwm / CONCURRENT_CONSUMERS as u64).max(1);
    let totals = Arc::new(Mutex::new((0_u64, 0_u64, Vec::<f64>::new(), 0_u64)));
    let cpu_before = sample_cpu();
    let disk_before = process_read_bytes();
    let start = Instant::now();
    thread::scope(|scope| {
        for i in 0..CONCURRENT_CONSUMERS {
            let path = path.clone();
            let totals = Arc::clone(&totals);
            scope.spawn(move || {
                let mut reader = open_reader(&path);
                let (from, to) = if disjoint {
                    let from = i as u64 * chunk;
                    let to = if i + 1 == CONCURRENT_CONSUMERS {
                        hwm
                    } else {
                        ((i as u64 + 1) * chunk).min(hwm)
                    };
                    (from, to)
                } else {
                    (0, hwm)
                };
                let mut offset = from;
                let mut fetched = 0_u64;
                let mut calls = 0_u64;
                let mut latencies = Vec::new();
                let mut peak = 0_u64;
                while offset < to {
                    let t0 = Instant::now();
                    let batch = reader
                        .fetch(offset, FETCH_MAX_BYTES, FETCH_MAX_RECORDS)
                        .expect("fetch");
                    latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
                    assert!(batch
                        .records
                        .iter()
                        .all(|r| r.offset < batch.high_watermark));
                    let visible: Vec<_> = batch
                        .records
                        .into_iter()
                        .filter(|r| r.offset < to)
                        .collect();
                    if visible.is_empty() {
                        break;
                    }
                    let encoded: u64 = visible
                        .iter()
                        .map(|r| (r.record.key.len() + r.record.value.len() + 32) as u64)
                        .sum();
                    peak = peak.max(encoded);
                    fetched += encoded;
                    calls += 1;
                    offset = visible.last().map(|r| r.offset + 1).unwrap_or(to);
                }
                let mut guard = totals.lock().expect("lock");
                guard.0 += fetched;
                guard.1 += calls;
                guard.2.extend(latencies);
                guard.3 = guard.3.max(peak);
            });
        }
    });
    let elapsed = start.elapsed();
    let cpu_after = sample_cpu();
    let disk_after = process_read_bytes();
    let (fetched, calls, latencies, peak) = {
        let g = totals.lock().expect("lock");
        (g.0, g.1, g.2.clone(), g.3)
    };
    let mut notes = vec![format!("{CONCURRENT_CONSUMERS} threads")];
    if disjoint {
        notes.push("each consumer reads a disjoint offset range".to_owned());
    } else {
        notes.push("all consumers read the full committed range".to_owned());
    }
    finish_scenario(ScenarioInput {
        name,
        engine: Engine::BufferedPageCache,
        workload,
        status: ProbeStatus::Measured,
        skip_reason: None,
        fetched,
        fetch_calls: calls,
        elapsed,
        latencies,
        cpu_before: &cpu_before,
        cpu_after: &cpu_after,
        disk_before,
        disk_after,
        estimated_buffer_bytes: peak * CONCURRENT_CONSUMERS as u64,
        notes,
    })
}

#[cfg(target_os = "linux")]
fn run_sendfile_probe(fixture: &SegmentFixture) -> ScenarioReport {
    let mut notes = prepare_cold(&fixture.segment_path);
    notes.push("raw segment bytes via sendfile → /dev/null (not frame-decoded)".to_owned());
    let cpu_before = sample_cpu();
    let disk_before = process_read_bytes();
    let start = Instant::now();
    let mut latencies = Vec::new();
    let mut transferred = 0_u64;
    let mut calls = 0_u64;
    match sendfile_to_null(
        &fixture.segment_path,
        &mut latencies,
        &mut transferred,
        &mut calls,
    ) {
        Ok(()) => {
            let elapsed = start.elapsed();
            let cpu_after = sample_cpu();
            let disk_after = process_read_bytes();
            finish_scenario(ScenarioInput {
                name: "sendfile_cold_raw",
                engine: Engine::Sendfile,
                workload: Workload::Cold,
                status: ProbeStatus::Measured,
                skip_reason: None,
                fetched: transferred,
                fetch_calls: calls,
                elapsed,
                latencies,
                cpu_before: &cpu_before,
                cpu_after: &cpu_after,
                disk_before,
                disk_after,
                estimated_buffer_bytes: 0,
                notes,
            })
        }
        Err(err) => skipped(
            "sendfile_cold_raw",
            Engine::Sendfile,
            Workload::Cold,
            ProbeStatus::SkippedUnavailable,
            err.to_string(),
        ),
    }
}

#[cfg(not(target_os = "linux"))]
fn run_sendfile_probe(_fixture: &SegmentFixture) -> ScenarioReport {
    skipped(
        "sendfile_cold_raw",
        Engine::Sendfile,
        Workload::Cold,
        ProbeStatus::SkippedUnavailable,
        "sendfile probe is Linux-only",
    )
}

#[cfg(target_os = "linux")]
fn run_splice_probe(fixture: &SegmentFixture) -> ScenarioReport {
    let mut notes = prepare_cold(&fixture.segment_path);
    notes.push("raw segment bytes via splice file→pipe→/dev/null".to_owned());
    let cpu_before = sample_cpu();
    let disk_before = process_read_bytes();
    let start = Instant::now();
    let mut latencies = Vec::new();
    let mut transferred = 0_u64;
    let mut calls = 0_u64;
    match splice_to_null(
        &fixture.segment_path,
        &mut latencies,
        &mut transferred,
        &mut calls,
    ) {
        Ok(()) => {
            let elapsed = start.elapsed();
            let cpu_after = sample_cpu();
            let disk_after = process_read_bytes();
            finish_scenario(ScenarioInput {
                name: "splice_cold_raw",
                engine: Engine::Splice,
                workload: Workload::Cold,
                status: ProbeStatus::Measured,
                skip_reason: None,
                fetched: transferred,
                fetch_calls: calls,
                elapsed,
                latencies,
                cpu_before: &cpu_before,
                cpu_after: &cpu_after,
                disk_before,
                disk_after,
                estimated_buffer_bytes: 0,
                notes,
            })
        }
        Err(err) => skipped(
            "splice_cold_raw",
            Engine::Splice,
            Workload::Cold,
            ProbeStatus::SkippedUnavailable,
            err.to_string(),
        ),
    }
}

#[cfg(not(target_os = "linux"))]
fn run_splice_probe(_fixture: &SegmentFixture) -> ScenarioReport {
    skipped(
        "splice_cold_raw",
        Engine::Splice,
        Workload::Cold,
        ProbeStatus::SkippedUnavailable,
        "splice probe is Linux-only",
    )
}

#[cfg(target_os = "linux")]
fn run_odirect_probe(fixture: &SegmentFixture) -> ScenarioReport {
    let mut notes = vec![
        "experimental O_DIRECT aligned reads of sealed segment (not production path)".to_owned(),
    ];
    notes.extend(prepare_cold(&fixture.segment_path));
    let cpu_before = sample_cpu();
    let disk_before = process_read_bytes();
    let start = Instant::now();
    let mut latencies = Vec::new();
    let mut transferred = 0_u64;
    let mut calls = 0_u64;
    match odirect_read_all(
        &fixture.segment_path,
        &mut latencies,
        &mut transferred,
        &mut calls,
    ) {
        Ok(buf_bytes) => {
            let elapsed = start.elapsed();
            let cpu_after = sample_cpu();
            let disk_after = process_read_bytes();
            finish_scenario(ScenarioInput {
                name: "odirect_cold_raw",
                engine: Engine::Odirect,
                workload: Workload::Cold,
                status: ProbeStatus::Measured,
                skip_reason: None,
                fetched: transferred,
                fetch_calls: calls,
                elapsed,
                latencies,
                cpu_before: &cpu_before,
                cpu_after: &cpu_after,
                disk_before,
                disk_after,
                estimated_buffer_bytes: buf_bytes,
                notes,
            })
        }
        Err(err) => skipped(
            "odirect_cold_raw",
            Engine::Odirect,
            Workload::Cold,
            ProbeStatus::SkippedUnavailable,
            err.to_string(),
        ),
    }
}

#[cfg(not(target_os = "linux"))]
fn run_odirect_probe(_fixture: &SegmentFixture) -> ScenarioReport {
    skipped(
        "odirect_cold_raw",
        Engine::Odirect,
        Workload::Cold,
        ProbeStatus::SkippedUnavailable,
        "O_DIRECT probe is Linux-only",
    )
}

fn run_io_uring_placeholder() -> ScenarioReport {
    skipped(
        "io_uring_probe",
        Engine::IoUring,
        Workload::Cold,
        ProbeStatus::Deferred,
        "no io_uring dependency in tree; enable only after the buffered-path gate trips",
    )
}

fn hwm_correctness_checks() -> Vec<HwmCheck> {
    let mut checks = Vec::new();
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("hwm.active");
    let mut segment = ActiveSegment::create(&path, descriptor(), config()).expect("create");

    segment
        .append(record(0), Durability::Fsync)
        .expect("fsync append");
    segment
        .append(record(1), Durability::Buffered)
        .expect("buffered append");
    assert_eq!(segment.committed_offset(), 1);
    assert_eq!(segment.next_offset(), 2);

    let fetch = segment.fetch(0, usize::MAX, 10).expect("fetch");
    let ok = fetch.records.len() == 1
        && fetch.high_watermark == 1
        && fetch
            .records
            .iter()
            .all(|r| r.offset < fetch.high_watermark);
    checks.push(HwmCheck {
        name: "buffered_tail_hidden_until_commit".to_owned(),
        passed: ok,
        detail: format!(
            "records={} hwm={} committed={}",
            fetch.records.len(),
            fetch.high_watermark,
            segment.committed_offset()
        ),
    });

    let clamped = segment
        .fetch_through(0, usize::MAX, 10, 0)
        .expect("fetch_through");
    let ok2 = clamped.records.is_empty() && clamped.high_watermark == 0;
    checks.push(HwmCheck {
        name: "fetch_through_never_above_requested_hwm".to_owned(),
        passed: ok2,
        detail: format!(
            "records={} hwm={}",
            clamped.records.len(),
            clamped.high_watermark
        ),
    });

    segment
        .append(record(1), Durability::Fsync)
        .expect("commit second");
    let local = segment.committed_offset();
    let cluster = local.saturating_sub(1);
    let batch = segment
        .fetch_through(0, usize::MAX, 10, cluster)
        .expect("cluster clamp");
    let ok3 = batch.high_watermark == cluster
        && batch
            .records
            .iter()
            .all(|r| r.offset < batch.high_watermark);
    checks.push(HwmCheck {
        name: "cluster_hwm_clamped_to_min_local_commit".to_owned(),
        passed: ok3,
        detail: format!(
            "local={local} cluster={cluster} batch_hwm={} records={}",
            batch.high_watermark,
            batch.records.len()
        ),
    });

    checks
}

fn recommend(scenarios: &[ScenarioReport], host: &HostInfo) -> Recommendation {
    let gate = "only pursue O_DIRECT/io_uring if the buffered page-cache path is the bottleneck"
        .to_owned();
    let buffered_hot = scenarios.iter().find(|s| s.name == "buffered_hot");
    let buffered_cold = scenarios.iter().find(|s| s.name == "buffered_cold");
    let odirect = scenarios.iter().find(|s| s.name == "odirect_cold_raw");
    let sendfile = scenarios.iter().find(|s| s.name == "sendfile_cold_raw");
    let tls = scenarios.iter().find(|s| s.name == "buffered_tls_proxy");

    let mut lab_limits = vec![
        "CI/local runs are short and noisy; absolute SLA numbers are not claimed.".to_owned(),
        "Disk read amp uses /proc/self/io read_bytes on Linux only.".to_owned(),
        "Cold-cache on non-Linux is best-effort (page cache often remains warm).".to_owned(),
        "sendfile/splice probes transfer raw segment bytes, not decoded fetch frames.".to_owned(),
        "TLS cost is a blake3 CPU proxy, not a full handshake/record stack.".to_owned(),
        "Multi-hour soak and exotic kernel tuning are deferred.".to_owned(),
    ];
    if !host.linux_probes_enabled {
        lab_limits.push(
            "This host did not run Linux sendfile/splice/O_DIRECT probes; recommendation leans on buffered measurements plus the epic #93 gate."
                .to_owned(),
        );
    }

    // Gate trips only when measured O_DIRECT cold wins are large and buffered
    // cold looks disk-bound (read amp >> 1). Never open the gate solely because
    // TLS CPU is high — crypto dominates storage strategy there.
    let mut pursue = false;
    let mut rationale = String::new();

    let tls_cpu = tls.map(|s| s.cpu_ms_per_gib).unwrap_or(0.0);
    let hot_cpu = buffered_hot.map(|s| s.cpu_ms_per_gib).unwrap_or(0.0);
    if tls_cpu > 0.0 && hot_cpu > 0.0 && tls_cpu > hot_cpu * 1.5 {
        rationale.push_str(
            "TLS-proxy CPU exceeds plain buffered fetch CPU, so transport crypto dominates over storage I/O strategy; ",
        );
    }

    if let (Some(cold), Some(od)) = (buffered_cold, odirect) {
        if od.status == ProbeStatus::Measured && cold.status == ProbeStatus::Measured {
            let amp = cold.disk_read_amp.unwrap_or(0.0);
            let cold_thr = cold.throughput_mib_per_sec;
            let od_thr = od.throughput_mib_per_sec;
            if amp > 1.5 && od_thr > cold_thr * 1.35 {
                pursue = true;
                rationale.push_str(&format!(
                    "buffered cold shows disk_read_amp={amp:.2} and O_DIRECT throughput ({od_thr:.2} MiB/s) beat buffered cold ({cold_thr:.2} MiB/s) by >35%; gate opens for a follow-up design spike only. "
                ));
            } else {
                rationale.push_str(&format!(
                    "O_DIRECT did not clearly beat buffered cold (buffered={cold_thr:.2} MiB/s, odirect={od_thr:.2} MiB/s, disk_read_amp={amp:.2}); keep buffered as default. "
                ));
            }
        }
    }

    if let Some(sf) = sendfile {
        if sf.status == ProbeStatus::Measured {
            rationale.push_str(
                "sendfile/splice remain interesting for plaintext fanout, but TLS terminates zero-copy so they are not a default fetch engine. ",
            );
        }
    }

    if rationale.is_empty() {
        rationale = "Buffered page-cache fetch is the measured baseline and no lab signal shows it is the bottleneck; keep it as the default per epic #93.".to_owned();
    }

    Recommendation {
        default_path: "buffered_page_cache".to_owned(),
        pursue_odirect_io_uring: pursue,
        gate,
        rationale: rationale.trim().to_owned(),
        lab_limits,
    }
}

fn build_report() -> HarnessReport {
    let fixture = build_sealed_segment();
    let host = HostInfo {
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        linux_probes_enabled: cfg!(target_os = "linux"),
    };

    let hwm_checks = hwm_correctness_checks();

    // Warm the page cache once so `buffered_hot` is intentionally hot.
    {
        let mut reader = open_reader(&fixture.segment_path);
        let _ = fetch_windowed(&mut reader, 0, fixture.high_watermark, TransportMode::None);
    }

    let mut scenarios = vec![
        run_buffered(
            &fixture,
            "buffered_hot",
            Workload::Hot,
            false,
            TransportMode::None,
        ),
        run_buffered(
            &fixture,
            "buffered_cold",
            Workload::Cold,
            true,
            TransportMode::None,
        ),
        run_buffered(
            &fixture,
            "buffered_sequential_catchup",
            Workload::SequentialCatchup,
            false,
            TransportMode::None,
        ),
        run_buffered_concurrent(&fixture, false),
        run_buffered_concurrent(&fixture, true),
        run_buffered(
            &fixture,
            "buffered_plain_transport",
            Workload::PlainTransport,
            false,
            TransportMode::PlainCopy,
        ),
        run_buffered(
            &fixture,
            "buffered_tls_proxy",
            Workload::TlsProxy,
            false,
            TransportMode::TlsProxy,
        ),
        run_sendfile_probe(&fixture),
        run_splice_probe(&fixture),
        run_odirect_probe(&fixture),
        run_io_uring_placeholder(),
    ];

    if let Some(catchup) = scenarios
        .iter_mut()
        .find(|s| s.name == "buffered_sequential_catchup")
    {
        catchup.notes.push(format!(
            "sealed file bytes≈{}; fetched_logical_bytes={}",
            fixture.sealed_file_bytes, catchup.fetched_logical_bytes
        ));
    }

    let recommendation = recommend(&scenarios, &host);

    HarnessReport {
        harness_version: HARNESS_VERSION.to_owned(),
        issue: ISSUE.to_owned(),
        seed: SEED,
        host,
        methodology: METHODOLOGY.to_owned(),
        caveats: vec![
            "Research harness only — does not ship production sendfile/O_DIRECT/io_uring fetch engines.".to_owned(),
            "Correctness invariant unchanged: fetch never exposes records at/above the committed high-water mark.".to_owned(),
            "Wall-clock and CPU numbers are lab-limited and noisy under CI; use relative comparisons on one host.".to_owned(),
            "Does not claim Kafka superiority; archive/matrix methodology remains #92 / #98 / #130.".to_owned(),
        ],
        hwm_checks,
        scenarios,
        recommendation,
    }
}

fn maybe_write_json(report: &HarnessReport) {
    let Ok(raw) = std::env::var("VTOP_FETCH_IO_JSON") else {
        return;
    };
    let path = PathBuf::from(&raw);
    let path = if path.is_absolute() {
        path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create JSON output directory");
    }
    let body = serde_json::to_string_pretty(report).expect("serialize report");
    fs::write(&path, body).expect("write JSON report");
    eprintln!("wrote fetch-io report to {}", path.display());
}

#[test]
fn fetch_io_research_report_and_gate() {
    let report = build_report();
    maybe_write_json(&report);

    for check in &report.hwm_checks {
        assert!(
            check.passed,
            "HWM check failed: {} ({})",
            check.name, check.detail
        );
    }

    let measured: Vec<_> = report
        .scenarios
        .iter()
        .filter(|s| s.status == ProbeStatus::Measured)
        .collect();
    assert!(
        measured.iter().any(|s| s.name == "buffered_hot"),
        "buffered_hot must be measured"
    );
    assert!(
        measured
            .iter()
            .any(|s| s.name == "buffered_sequential_catchup"),
        "sequential catch-up must be measured"
    );

    for scenario in &measured {
        assert!(
            scenario.fetched_logical_bytes > 0,
            "{} fetched zero bytes",
            scenario.name
        );
        assert!(
            scenario.fetch_calls > 0,
            "{} recorded zero fetch calls",
            scenario.name
        );
    }

    assert_eq!(
        report.recommendation.default_path, "buffered_page_cache",
        "default recommendation must remain buffered unless a future harness version changes policy"
    );
    assert!(
        report
            .recommendation
            .gate
            .contains("only pursue O_DIRECT/io_uring"),
        "explicit gate text missing"
    );

    let iouring = report
        .scenarios
        .iter()
        .find(|s| s.engine == Engine::IoUring)
        .expect("io_uring row");
    assert_eq!(iouring.status, ProbeStatus::Deferred);

    let encoded = serde_json::to_value(&report).expect("json");
    assert_eq!(encoded["issue"], ISSUE);
    assert_eq!(encoded["harness_version"], HARNESS_VERSION);
    assert!(encoded["scenarios"].as_array().unwrap().len() >= 8);
    assert_eq!(
        encoded["recommendation"]["pursue_odirect_io_uring"].as_bool(),
        Some(report.recommendation.pursue_odirect_io_uring)
    );
}

#[test]
#[ignore = "extended local measurement; run with --ignored"]
fn fetch_io_extended_smoke() {
    let report = build_report();
    maybe_write_json(&report);
    assert!(report.hwm_checks.iter().all(|c| c.passed));
}

// --- Linux syscall probes -------------------------------------------------

#[cfg(target_os = "linux")]
fn sendfile_to_null(
    path: &Path,
    latencies: &mut Vec<f64>,
    transferred: &mut u64,
    calls: &mut u64,
) -> io::Result<()> {
    use std::fs::{File, OpenOptions};
    use std::os::fd::AsRawFd;
    let input = File::open(path)?;
    let null = OpenOptions::new().write(true).open("/dev/null")?;
    let len = input.metadata()?.len();
    let mut offset: off64_t = 0;
    while (offset as u64) < len {
        let t0 = Instant::now();
        let chunk = (len - offset as u64).min(1024 * 1024) as usize;
        // SAFETY: both fds are open; offset points to a valid off64_t.
        let n = unsafe { libc::sendfile(null.as_raw_fd(), input.as_raw_fd(), &mut offset, chunk) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n == 0 {
            break;
        }
        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
        *transferred += n as u64;
        *calls += 1;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
type off64_t = i64;

#[cfg(target_os = "linux")]
fn splice_to_null(
    path: &Path,
    latencies: &mut Vec<f64>,
    transferred: &mut u64,
    calls: &mut u64,
) -> io::Result<()> {
    use std::fs::{File, OpenOptions};
    use std::os::fd::AsRawFd;
    let input = File::open(path)?;
    let null = OpenOptions::new().write(true).open("/dev/null")?;
    let len = input.metadata()?.len();
    let mut pipe_fds = [0_i32; 2];
    // SAFETY: pipe2 writes two ints into pipe_fds.
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let (r_fd, w_fd) = (pipe_fds[0], pipe_fds[1]);
    struct PipeGuard(i32, i32);
    impl Drop for PipeGuard {
        fn drop(&mut self) {
            // SAFETY: close pipe fds we own.
            unsafe {
                libc::close(self.0);
                libc::close(self.1);
            }
        }
    }
    let _guard = PipeGuard(r_fd, w_fd);
    let mut offset: off64_t = 0;
    while (offset as u64) < len {
        let t0 = Instant::now();
        let want = (len - offset as u64).min(64 * 1024) as usize;
        // SAFETY: valid fds; splice file→pipe.
        let n_in = unsafe {
            libc::splice(
                input.as_raw_fd(),
                &mut offset,
                w_fd,
                std::ptr::null_mut(),
                want,
                0,
            )
        };
        if n_in < 0 {
            return Err(io::Error::last_os_error());
        }
        if n_in == 0 {
            break;
        }
        let mut remaining = n_in as usize;
        while remaining > 0 {
            let n_out = unsafe {
                libc::splice(
                    r_fd,
                    std::ptr::null_mut(),
                    null.as_raw_fd(),
                    std::ptr::null_mut(),
                    remaining,
                    0,
                )
            };
            if n_out < 0 {
                return Err(io::Error::last_os_error());
            }
            if n_out == 0 {
                break;
            }
            remaining -= n_out as usize;
        }
        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
        *transferred += n_in as u64;
        *calls += 1;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn odirect_read_all(
    path: &Path,
    latencies: &mut Vec<f64>,
    transferred: &mut u64,
    calls: &mut u64,
) -> io::Result<u64> {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::fs::OpenOptionsExt;
    const ALIGN: usize = 4096;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let len = file.metadata()?.len();
    let buf_len = ((len as usize + ALIGN - 1) / ALIGN) * ALIGN;
    let layout = std::alloc::Layout::from_size_align(buf_len.max(ALIGN), ALIGN)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    // SAFETY: aligned allocation for O_DIRECT.
    let ptr = unsafe { std::alloc::alloc(layout) };
    if ptr.is_null() {
        return Err(io::Error::new(io::ErrorKind::OutOfMemory, "alloc failed"));
    }
    struct AlignedBuf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
    }
    impl Drop for AlignedBuf {
        fn drop(&mut self) {
            // SAFETY: allocated with this layout.
            unsafe { std::alloc::dealloc(self.ptr, self.layout) }
        }
    }
    let aligned = AlignedBuf { ptr, layout };
    file.seek(SeekFrom::Start(0))?;
    let mut done = 0_u64;
    while done < len {
        let want = ((len - done) as usize).min(buf_len);
        let want_aligned = ((want + ALIGN - 1) / ALIGN) * ALIGN;
        // SAFETY: ptr is allocated for at least want_aligned bytes.
        let buf = unsafe { std::slice::from_raw_parts_mut(aligned.ptr, want_aligned) };
        let t0 = Instant::now();
        let n = file.read(buf)?;
        latencies.push(t0.elapsed().as_secs_f64() * 1000.0);
        if n == 0 {
            break;
        }
        let take = (n as u64).min(len - done);
        *transferred += take;
        *calls += 1;
        done += take;
    }
    Ok(buf_len as u64)
}

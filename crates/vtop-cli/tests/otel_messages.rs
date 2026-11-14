//! #683: what an operator sees, for every OpenTelemetry condition, from the
//! real `vtopctl` binary: the message, the exit status, and what reached the
//! collector. Each case runs the binary in its own process with its own
//! environment, so the cases do not race over process-wide state.

use std::path::Path;
use std::process::Output;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const RECORD: &str = "do-not-export-me";
const HEADER_SECRET: &str = "s3cret-header-value";

/// A request the collector received: request line, headers, body.
#[derive(Debug)]
struct Received {
    line: String,
    head: String,
    body: Vec<u8>,
}

#[derive(Clone, Copy)]
enum Collector {
    Answers,
    Hangs,
}

async fn collector(mode: Collector) -> (String, mpsc::UnboundedReceiver<Received>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                loop {
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let length = head
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + length {
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    if let Collector::Hangs = mode {
                        std::future::pending::<()>().await;
                    }
                    let _ = tx.send(Received {
                        line: head.lines().next().unwrap_or_default().to_owned(),
                        head: head.to_ascii_lowercase(),
                        body: buf[head_end..head_end + length].to_vec(),
                    });
                    buf.drain(..head_end + length);
                    let reply = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                    if socket.write_all(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (base, rx)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A config archiving one file to a local directory.
fn config(dir: &Path) -> std::path::PathBuf {
    std::fs::create_dir_all(dir.join("in")).unwrap();
    std::fs::write(
        dir.join("in").join("app.log"),
        format!("{RECORD} 1\n{RECORD} 2\n{RECORD} 3\n"),
    )
    .unwrap();
    let path = dir.join("config.yaml");
    std::fs::write(
        &path,
        format!(
            "engine:\n  name: vtop-otel-test\n  tenant: default\n  state_store: \"sqlite://{d}/state.db\"\n  work_dir: {d}/work\n  log_level: INFO\nbatching: {{}}\ncompression: {{}}\nchecksum:\n  algorithm: sha256\nsources:\n  file:\n    enabled: true\n    paths:\n      - {d}/in/*.log\nupload:\n  backend: localfs\n  local_path: {d}/objects\n  bucket: telemetry\n  prefix: \"\"\n  create_bucket: true\n",
            d = dir.display()
        ),
    )
    .unwrap();
    path
}

/// Run `vtopctl` with only the given OTel variables set.
async fn vtopctl(args: &[&str], config: &Path, vars: &[(&str, &str)]) -> (Output, Duration) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_vtopctl"));
    command.args(args).arg("--config").arg(config);
    for (name, _) in std::env::vars() {
        if name.starts_with("OTEL_") || name == "RUST_LOG" || name == "VTOP_LOG_FORMAT" {
            command.env_remove(name);
        }
    }
    command.envs(vars.iter().copied());
    let started = Instant::now();
    let output = tokio::time::timeout(Duration::from_secs(60), command.output())
        .await
        .expect("vtopctl did not exit within 60 s")
        .unwrap();
    (output, started.elapsed())
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn received(requests: &mut mpsc::UnboundedReceiver<Received>) -> Vec<Received> {
    let mut all = Vec::new();
    while let Ok(Some(r)) = tokio::time::timeout(Duration::from_millis(500), requests.recv()).await
    {
        all.push(r);
    }
    all
}

#[tokio::test]
async fn with_no_endpoint_one_info_line_says_export_is_off_and_what_to_set() {
    let dir = tempfile::tempdir().unwrap();
    let (out, _) = vtopctl(&["discover"], &config(dir.path()), &[]).await;
    assert!(out.status.success(), "{}", stderr(&out));
    let log = stderr(&out);
    assert_eq!(
        log.matches("OpenTelemetry export disabled (OTEL_EXPORTER_OTLP_ENDPOINT unset)")
            .count(),
        1,
        "{log}"
    );
    assert!(!log.contains("OpenTelemetry export enabled"), "{log}");
}

#[tokio::test]
async fn a_setting_that_cannot_be_honoured_fails_startup_naming_the_variable() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    for (vars, variable, accepted) in [
        (
            vec![
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1"),
                ("OTEL_TRACES_SAMPLER", "sometimes"),
            ],
            "OTEL_TRACES_SAMPLER",
            "parentbased_always_on",
        ),
        (
            vec![(
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "collector:4318/?token=s3cret",
            )],
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http:// or https://",
        ),
    ] {
        for command in [&["process-once", "--source", "file"][..], &["validate"][..]] {
            let (out, _) = vtopctl(command, &config, &vars).await;
            let log = stderr(&out);
            assert!(
                !out.status.success(),
                "{command:?} {vars:?} succeeded: {log}"
            );
            assert!(log.contains(variable), "{command:?}: {log}");
            assert!(log.contains(accepted), "{command:?}: {log}");
            assert!(!log.contains("s3cret"), "{command:?}: {log}");
        }
    }
}

/// Export on, collector answering: the INFO line names host and path only;
/// a batch's spans and the registry's metrics arrive as protobuf with the
/// configured header; the JSON log lines of the batch carry its trace id; the
/// flush says how many spans it sent. No record content and no header value
/// appears in what was sent or logged, beyond the header itself.
#[tokio::test]
async fn export_on_sends_traces_and_metrics_and_says_so() {
    let (base, mut requests) = collector(Collector::Answers).await;
    let dir = tempfile::tempdir().unwrap();
    let header = format!("authorization=Bearer%20{HEADER_SECRET}");
    let (out, _) = vtopctl(
        &["process-once", "--source", "file"],
        &config(dir.path()),
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", base.as_str()),
            ("OTEL_EXPORTER_OTLP_HEADERS", header.as_str()),
            ("VTOP_LOG_FORMAT", "json"),
        ],
    )
    .await;
    let log = stderr(&out);
    assert!(out.status.success(), "{log}");
    let host = base.trim_start_matches("http://");
    assert!(
        log.contains(&format!(
            "OpenTelemetry export enabled: traces to {host}/v1/traces, metrics to \
             {host}/v1/metrics, protocol http/protobuf, sampler parentbased_always_on, \
             service.name vtop-engine"
        )),
        "{log}"
    );
    assert!(log.contains("OpenTelemetry trace flush: flushed"), "{log}");
    assert!(
        !log.contains(HEADER_SECRET),
        "a header value was logged: {log}"
    );
    let correlated = log
        .lines()
        .filter(|l| l.contains("\"trace_id\":") && l.contains("batch_sealed"))
        .count();
    assert_eq!(
        correlated, 1,
        "the batch's log line carries no trace id: {log}"
    );

    let all = received(&mut requests).await;
    let traces: Vec<_> = all
        .iter()
        .filter(|r| r.line == "POST /v1/traces HTTP/1.1")
        .collect();
    let metrics: Vec<_> = all
        .iter()
        .filter(|r| r.line == "POST /v1/metrics HTTP/1.1")
        .collect();
    assert!(
        !traces.is_empty(),
        "no traces reached the collector: {all:?}"
    );
    assert!(!metrics.is_empty(), "no metrics reached the collector");
    for r in all.iter() {
        assert!(
            r.head.contains("content-type: application/x-protobuf"),
            "{}",
            r.head
        );
        assert!(
            r.head
                .contains(&format!("authorization: bearer {HEADER_SECRET}")),
            "the configured header was not sent: {}",
            r.head
        );
        assert!(
            !contains(&r.body, RECORD.as_bytes()),
            "record content was exported"
        );
    }
    let spans: Vec<u8> = traces.iter().flat_map(|r| r.body.clone()).collect();
    for name in [
        &b"batch"[..],
        b"upload_manifest",
        b"vtop-otel-test",
        b"host.name",
    ] {
        assert!(
            contains(&spans, name),
            "{} not in the traces",
            String::from_utf8_lossy(name)
        );
    }
    let points: Vec<u8> = metrics.iter().flat_map(|r| r.body.clone()).collect();
    assert!(
        contains(&points, b"vtop_commits_total"),
        "the engine's metrics were not sent"
    );
}

/// Export on, nobody listening: the batch commits, a WARN says the export
/// failed and that batches are unaffected, and the flush says what it dropped.
#[tokio::test]
async fn an_unreachable_collector_warns_and_the_batch_still_commits() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let dir = tempfile::tempdir().unwrap();
    let (out, took) = vtopctl(
        &["process-once", "--source", "file"],
        &config(dir.path()),
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", url.as_str()),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2000"),
        ],
    )
    .await;
    let log = stderr(&out);
    assert!(out.status.success(), "{log}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("committed=true"));
    assert!(log.contains("OpenTelemetry trace export failed"), "{log}");
    assert!(log.contains("batches are unaffected"), "{log}");
    assert!(log.contains("dropped"), "{log}");
    assert!(took < Duration::from_secs(20), "took {took:?}");
}

/// Export on, collector hangs: the batch commits and the exit waits for the
/// flush deadline, then says it gave up.
#[tokio::test]
async fn a_hung_collector_holds_the_exit_for_the_flush_deadline_only() {
    let (base, _requests) = collector(Collector::Hangs).await;
    let dir = tempfile::tempdir().unwrap();
    let (out, took) = vtopctl(
        &["process-once", "--source", "file"],
        &config(dir.path()),
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", base.as_str()),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "60000"),
        ],
    )
    .await;
    let log = stderr(&out);
    assert!(out.status.success(), "{log}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("committed=true"));
    assert!(log.contains("gave up after"), "{log}");
    assert!(took < Duration::from_secs(15), "the exit took {took:?}");
}

/// `validate --print-effective` shows the effective OTel settings: host and
/// path, never the user-info, query string or header values.
#[tokio::test]
async fn validate_prints_the_effective_settings_without_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let header = format!("authorization=Bearer%20{HEADER_SECRET}");
    let (out, _) = vtopctl(
        &["validate", "--print-effective"],
        &config(dir.path()),
        &[
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "https://user:pw@otel.example:4318",
            ),
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "https://otel.example:4318/v1/traces?token=s3cret",
            ),
            ("OTEL_EXPORTER_OTLP_HEADERS", header.as_str()),
        ],
    )
    .await;
    assert!(out.status.success(), "{}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("opentelemetry:\n  export: on\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  traces: otel.example:4318/v1/traces\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  metrics: otel.example:4318/v1/metrics\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  traces_headers: set (values not shown)\n"),
        "{stdout}"
    );
    for secret in ["pw@", "user", "s3cret", HEADER_SECRET] {
        assert!(!stdout.contains(secret), "{secret} in: {stdout}");
    }
}

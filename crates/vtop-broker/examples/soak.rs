//! End-to-end soak, crash, and corruption harness for the native broker.
//!
//! Every mode drives the real stack: TLS 1.3 mTLS sessions, the bounded wire
//! protocol, durable `LocalFsync` acknowledgements, and committed-only fetch.
//! Record contents are deterministic functions of the sequence number, so
//! verification checks bytes, not just counts.
//!
//! ```text
//! cargo run --release -p vtop-broker --example soak -- produce --dir /tmp/soak --records 2000000
//! cargo run --release -p vtop-broker --example soak -- verify --dir /tmp/soak --expect 2000000
//! cargo run --release -p vtop-broker --example soak -- crash-test --dir /tmp/crash --records 500000 --abort-after 200000
//! cargo run --release -p vtop-broker --example soak -- corrupt-test --dir /tmp/corrupt
//! ```
//!
//! `produce` refuses to reuse a directory: sequences restart at zero and would
//! be rejected as duplicates. `crash-test` aborts the producer process mid
//! stream (a real SIGABRT, no destructors), recovers the directory, and proves
//! every acknowledged record survived. `corrupt-test` proves a torn tail is
//! truncated safely and that flipping one committed byte is detected loudly.

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsConnector;
use uuid::Uuid;
use vtop_broker::{
    LocalBroker, NativeServer, ProducerEpochJournal, ServerConfig, ServerTlsMaterial,
    SessionAuthorizer,
};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    read_frame, write_frame, ClientHello, Durability, ErrorResponse, FetchRequest, Message,
    ProduceRecord, ProduceRequest, ProtocolLimits, RangeIdentity, Role, WindowUpdate, WireFrame,
    PROTOCOL_MAJOR,
};

const CLUSTER_ID: Uuid = Uuid::from_u128(0xC1);
const NODE_ID: Uuid = Uuid::from_u128(0x0DE);
const PRINCIPAL_ID: Uuid = Uuid::from_u128(0xACE);
const PRODUCER_ID: Uuid = Uuid::from_u128(0xACE);
const RANGE_ID: Uuid = Uuid::from_u128(0x10);
const SEGMENT_ID: Uuid = Uuid::from_u128(0x11);
const FENCING_EPOCH: u64 = 7;
const PRODUCER_EPOCH: u64 = 1;
const LIMITS: ProtocolLimits = ProtocolLimits {
    max_frame_bytes: 32 * 1024 * 1024,
    max_records: 65_536,
};
const WINDOW_BYTES: u64 = 32 * 1024 * 1024;
const FETCH_BYTES: u32 = 4 * 1024 * 1024;

struct Options {
    dir: PathBuf,
    records: u64,
    batch: u32,
    value_bytes: usize,
    abort_after: Option<u64>,
    expect: u64,
}

fn main() {
    let mut args: VecDeque<String> = std::env::args().skip(1).collect();
    let mode = args.pop_front().unwrap_or_default();
    let mut options = Options {
        dir: PathBuf::new(),
        records: 2_000_000,
        batch: 5_000,
        value_bytes: 200,
        abort_after: None,
        expect: 0,
    };
    while let Some(flag) = args.pop_front() {
        let value = args.pop_front().unwrap_or_else(|| fail(&flag, "a value"));
        match flag.as_str() {
            "--dir" => options.dir = PathBuf::from(value),
            "--records" => options.records = parse(&flag, &value),
            "--batch" => options.batch = parse(&flag, &value),
            "--value-bytes" => options.value_bytes = parse(&flag, &value),
            "--abort-after" => options.abort_after = Some(parse(&flag, &value)),
            "--expect" => options.expect = parse(&flag, &value),
            _ => fail(&flag, "a known flag"),
        }
    }
    if options.dir.as_os_str().is_empty() {
        eprintln!("--dir is required");
        std::process::exit(2);
    }
    if options.records == 0 || options.batch == 0 || options.value_bytes == 0 {
        eprintln!("--records, --batch, and --value-bytes must be greater than zero");
        std::process::exit(2);
    }
    match mode.as_str() {
        "produce" => run_async(produce(options)),
        "verify" => run_async(verify(options)),
        "crash-test" => crash_test(options),
        "corrupt-test" => run_async(corrupt_test(options)),
        other => {
            eprintln!("unknown mode {other:?}; use produce | verify | crash-test | corrupt-test");
            std::process::exit(2);
        }
    }
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> T {
    value.parse().unwrap_or_else(|_| fail(flag, "a number"))
}

fn fail(flag: &str, expected: &str) -> ! {
    eprintln!("flag {flag} expects {expected}");
    std::process::exit(2);
}

fn run_async(future: impl std::future::Future<Output = Result<(), String>>) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    if let Err(problem) = runtime.block_on(future) {
        eprintln!("FAILED: {problem}");
        std::process::exit(1);
    }
}

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "soak".to_owned(),
        topic_epoch: 1,
        range_id: RANGE_ID,
        range_generation: 0,
    }
}

fn segment_path(dir: &Path) -> PathBuf {
    dir.join("soak.active")
}

fn open_broker(dir: &Path, fresh: bool) -> Result<Arc<LocalBroker>, String> {
    std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    let path = segment_path(dir);
    let segment = if fresh {
        if path.exists() {
            return Err(format!(
                "{} already exists; produce needs a fresh --dir (sequences restart at zero)",
                path.display()
            ));
        }
        let descriptor = SegmentDescriptor {
            segment_id: SEGMENT_ID,
            topic: "soak".to_owned(),
            topic_epoch: 1,
            lineage: RangeLineage {
                range_id: RANGE_ID,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let config = SegmentConfig {
            max_segment_bytes: 8 * 1024 * 1024 * 1024,
            max_segment_records: 10_000_000,
            ..SegmentConfig::default()
        };
        ActiveSegment::create(&path, descriptor, config).map_err(|error| error.to_string())?
    } else {
        let segment = ActiveSegment::recover(&path).map_err(|error| error.to_string())?;
        println!("recovery_report={:?}", segment.recovery_report());
        segment
    };
    let epochs =
        ProducerEpochJournal::open(dir.join("soak.epochs")).map_err(|error| error.to_string())?;
    LocalBroker::new(segment, epochs, range(), FENCING_EPOCH)
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

struct LeafAuthorizer {
    leaf_der: Vec<u8>,
}

impl SessionAuthorizer for LeafAuthorizer {
    fn authorize(&self, peer_chain_der: &[Vec<u8>], principal_id: Uuid, role: Role) -> bool {
        peer_chain_der.first().map(Vec::as_slice) == Some(self.leaf_der.as_slice())
            && principal_id == PRINCIPAL_ID
            && matches!(role, Role::Producer | Role::Consumer)
    }
}

struct Harness {
    address: SocketAddr,
    connector: TlsConnector,
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<vtop_broker::BrokerResult<()>>,
}

fn private_key(identity: &CertifiedKey<rcgen::KeyPair>) -> PrivateKeyDer<'static> {
    PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into()
}

async fn start(broker: Arc<LocalBroker>) -> Result<Harness, String> {
    let server_identity =
        generate_simple_self_signed(vec!["localhost".to_owned()]).map_err(|e| e.to_string())?;
    let client_identity =
        generate_simple_self_signed(vec!["soak-client".to_owned()]).map_err(|e| e.to_string())?;
    let mut client_roots = rustls::RootCertStore::empty();
    client_roots
        .add(client_identity.cert.der().clone())
        .map_err(|e| e.to_string())?;
    let server = NativeServer::new(
        broker,
        ServerTlsMaterial {
            certificate_chain: vec![server_identity.cert.der().clone()],
            private_key: private_key(&server_identity),
            client_roots,
        },
        Arc::new(LeafAuthorizer {
            leaf_der: client_identity.cert.der().as_ref().to_vec(),
        }),
        ServerConfig {
            cluster_id: CLUSTER_ID,
            node_id: NODE_ID,
            segment_format: vtop_broker::SegmentFormat::V1,
            max_frame_bytes: LIMITS.max_frame_bytes,
            max_records_per_frame: LIMITS.max_records,
            window_bytes: WINDOW_BYTES,
            max_sessions: 8,
            max_inflight_requests: 8,
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
        },
    )
    .map_err(|error| error.to_string())?;

    let mut server_roots = rustls::RootCertStore::empty();
    server_roots
        .add(server_identity.cert.der().clone())
        .map_err(|e| e.to_string())?;
    let client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|e| e.to_string())?
    .with_root_certificates(server_roots)
    .with_client_auth_cert(
        vec![client_identity.cert.der().clone()],
        private_key(&client_identity),
    )
    .map_err(|e| e.to_string())?;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let address = listener.local_addr().map_err(|e| e.to_string())?;
    let (shutdown, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(server.serve(listener, shutdown_rx));
    Ok(Harness {
        address,
        connector: TlsConnector::from(Arc::new(client_tls)),
        shutdown,
        server,
    })
}

impl Harness {
    async fn stop(self) -> Result<(), String> {
        let _ = self.shutdown.send(());
        self.server
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())
    }
}

type Session = tokio_rustls::client::TlsStream<TcpStream>;

async fn connect(harness: &Harness, role: Role) -> Result<Session, String> {
    let socket = TcpStream::connect(harness.address)
        .await
        .map_err(|e| e.to_string())?;
    let mut stream = harness
        .connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .map_err(|e| e.to_string())?;
    write_frame(
        &mut stream,
        &WireFrame {
            request_id: 0,
            stream_id: 0,
            message: Message::ClientHello(ClientHello {
                cluster_id: CLUSTER_ID,
                principal_id: PRINCIPAL_ID,
                role,
                minimum_major: PROTOCOL_MAJOR,
                maximum_major: PROTOCOL_MAJOR,
                requested_max_frame_bytes: LIMITS.max_frame_bytes,
                requested_max_records: LIMITS.max_records,
                requested_max_inflight_requests: 1,
                initial_window_bytes: WINDOW_BYTES,
                session_nonce: [7; 32],
            }),
        },
        LIMITS,
    )
    .await
    .map_err(|e| e.to_string())?;
    match read_frame(&mut stream, LIMITS)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(WireFrame {
            message: Message::ServerHello(_),
            ..
        }) => Ok(stream),
        Some(WireFrame {
            message: Message::Error(ErrorResponse { code, message, .. }),
            ..
        }) => Err(format!("hello rejected: {code:?} {message}")),
        other => Err(format!("unexpected hello reply: {other:?}")),
    }
}

fn record_value(sequence: u64, value_bytes: usize) -> Vec<u8> {
    (0..value_bytes)
        .map(|index| (sequence.wrapping_mul(31).wrapping_add(index as u64) & 0xff) as u8)
        .collect()
}

async fn produce(options: Options) -> Result<(), String> {
    let broker = open_broker(&options.dir, true)?;
    let harness = start(broker).await?;
    let mut session = connect(&harness, Role::Producer).await?;
    let started = Instant::now();
    let mut acked: u64 = 0;
    let mut request_id: u64 = 0;
    let mut batch_latencies = Vec::new();
    while acked < options.records {
        let count = u64::from(options.batch).min(options.records - acked);
        let records = (acked..acked + count)
            .map(|sequence| ProduceRecord {
                timestamp_millis: sequence as i64,
                key: sequence.to_be_bytes().to_vec(),
                value: record_value(sequence, options.value_bytes),
            })
            .collect();
        request_id += 1;
        let batch_started = Instant::now();
        write_frame(
            &mut session,
            &WireFrame {
                request_id,
                stream_id: 1,
                message: Message::ProduceRequest(ProduceRequest {
                    range: range(),
                    fencing_epoch: FENCING_EPOCH,
                    producer_id: PRODUCER_ID,
                    producer_epoch: PRODUCER_EPOCH,
                    first_sequence: acked,
                    durability: Durability::LocalFsync,
                    records,
                }),
            },
            LIMITS,
        )
        .await
        .map_err(|e| e.to_string())?;
        let reply = read_frame(&mut session, LIMITS)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "server closed the session".to_owned())?;
        match reply.message {
            Message::ProduceResponse(response) => {
                if response.committed_next_offset != acked + count {
                    return Err(format!(
                        "committed_next_offset {} after acking {} records",
                        response.committed_next_offset,
                        acked + count
                    ));
                }
            }
            Message::Error(ErrorResponse { code, message, .. }) => {
                return Err(format!("produce rejected: {code:?} {message}"));
            }
            other => return Err(format!("unexpected produce reply: {other:?}")),
        }
        batch_latencies.push(batch_started.elapsed());
        acked += count;
        if let Some(abort_after) = options.abort_after {
            if acked >= abort_after {
                println!("acked_total={acked}");
                println!("aborting_now");
                // The parent parses these markers; make sure they reach the
                // pipe before the process dies without running destructors.
                std::io::stdout().flush().expect("flush abort markers");
                std::process::abort();
            }
        }
        if request_id.is_multiple_of(20) {
            let rate = acked as f64 / started.elapsed().as_secs_f64();
            println!("acked_total={acked} rate={rate:.0}rec/s");
        }
    }
    let elapsed = started.elapsed();
    batch_latencies.sort_unstable();
    let percentile =
        |fraction: f64| batch_latencies[((batch_latencies.len() - 1) as f64 * fraction) as usize];
    let bytes = acked * (options.value_bytes as u64 + 8);
    println!("acked_total={acked}");
    println!(
        "produce_done records={acked} elapsed={:.2}s rate={:.0}rec/s payload={:.1}MB/s fsync_batches={} batch_p50={:?} batch_p95={:?} batch_max={:?}",
        elapsed.as_secs_f64(),
        acked as f64 / elapsed.as_secs_f64(),
        bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0,
        batch_latencies.len(),
        percentile(0.50),
        percentile(0.95),
        percentile(1.0),
    );
    drop(session);
    harness.stop().await
}

async fn fetch_all(
    harness: &Harness,
    batch: u32,
    value_bytes: usize,
) -> Result<(u64, u64), String> {
    let mut session = connect(harness, Role::Consumer).await?;
    let mut request_id: u64 = 0;
    let mut next_offset: u64 = 0;
    let mut verified: u64 = 0;
    let high_watermark;
    loop {
        request_id += 1;
        write_frame(
            &mut session,
            &WireFrame {
                request_id,
                stream_id: 1,
                message: Message::WindowUpdate(WindowUpdate {
                    additional_bytes: WINDOW_BYTES,
                }),
            },
            LIMITS,
        )
        .await
        .map_err(|e| e.to_string())?;
        request_id += 1;
        write_frame(
            &mut session,
            &WireFrame {
                request_id,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range: range(),
                    fencing_epoch: FENCING_EPOCH,
                    start_offset: next_offset,
                    max_bytes: FETCH_BYTES,
                    max_records: batch,
                }),
            },
            LIMITS,
        )
        .await
        .map_err(|e| e.to_string())?;
        let reply = read_frame(&mut session, LIMITS)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "server closed the session".to_owned())?;
        let response = match reply.message {
            Message::FetchResponse(response) => response,
            Message::Error(ErrorResponse { code, message, .. }) => {
                return Err(format!("fetch rejected: {code:?} {message}"));
            }
            other => return Err(format!("unexpected fetch reply: {other:?}")),
        };
        for record in &response.records {
            if record.offset != next_offset {
                return Err(format!(
                    "expected offset {next_offset}, fetched {}",
                    record.offset
                ));
            }
            if record.key != next_offset.to_be_bytes()
                || record.value != record_value(next_offset, value_bytes)
            {
                return Err(format!("record {next_offset} content mismatch"));
            }
            next_offset += 1;
            verified += 1;
        }
        if next_offset >= response.committed_high_watermark {
            high_watermark = response.committed_high_watermark;
            break;
        }
        if response.records.is_empty() {
            return Err(format!(
                "no progress at offset {next_offset} below high watermark {}",
                response.committed_high_watermark
            ));
        }
    }
    Ok((verified, high_watermark))
}

async fn verify(options: Options) -> Result<(), String> {
    let broker = open_broker(&options.dir, false)?;
    let harness = start(broker).await?;
    let started = Instant::now();
    let (verified, high_watermark) =
        fetch_all(&harness, options.batch, options.value_bytes).await?;
    let elapsed = started.elapsed();
    println!(
        "verify_done records={verified} high_watermark={high_watermark} elapsed={:.2}s rate={:.0}rec/s",
        elapsed.as_secs_f64(),
        verified as f64 / elapsed.as_secs_f64(),
    );
    if verified < options.expect {
        return Err(format!(
            "verified {verified} records but {} were acknowledged before the crash",
            options.expect
        ));
    }
    harness.stop().await
}

fn crash_test(options: Options) {
    let abort_after = options.abort_after.unwrap_or(options.records / 2).max(1);
    let exe = std::env::current_exe().expect("current executable");
    println!("crash_test spawning producer, will SIGABRT after {abort_after} acknowledged records");
    let output = Command::new(exe)
        .args([
            "produce",
            "--dir",
            options.dir.to_str().expect("utf-8 dir"),
            "--records",
            &options.records.to_string(),
            "--batch",
            &options.batch.to_string(),
            "--value-bytes",
            &options.value_bytes.to_string(),
            "--abort-after",
            &abort_after.to_string(),
        ])
        .output()
        .expect("spawn producer child");
    if output.status.success() {
        eprintln!("FAILED: producer child exited cleanly instead of crashing");
        std::process::exit(1);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let acked = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("acked_total="))
        .filter_map(|rest| rest.split_whitespace().next())
        .filter_map(|value| value.parse::<u64>().ok())
        .next_back()
        .expect("child reported acknowledged records");
    println!(
        "crash_test child died (status {:?}) after acking {acked} records",
        output.status
    );
    run_async(verify(Options {
        expect: acked,
        abort_after: None,
        ..options
    }));
    println!("crash_test_done acked={acked} all acknowledged records survived the crash");
}

async fn corrupt_test(options: Options) -> Result<(), String> {
    // Produce a modest, fully-acknowledged prefix first.
    let records = options.records.min(50_000);
    produce(Options {
        records,
        abort_after: None,
        expect: 0,
        dir: options.dir.clone(),
        batch: options.batch,
        value_bytes: options.value_bytes,
    })
    .await?;
    let path = segment_path(&options.dir);

    // A torn tail (a partial write that never reached its commit marker) must
    // be truncated away without losing any acknowledged record.
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|e| e.to_string())?;
        file.write_all(&[0xAB; 4096]).map_err(|e| e.to_string())?;
    }
    println!("corrupt_test appended 4096 garbage bytes to the active tail");
    {
        let segment = ActiveSegment::recover(&path).map_err(|e| e.to_string())?;
        let report = segment.recovery_report();
        if report.truncated_bytes != 4096 || report.records != records {
            return Err(format!(
                "expected recovery to truncate exactly the 4096 garbage bytes and keep {records} records, got {report:?}"
            ));
        }
    }
    verify(Options {
        expect: records,
        abort_after: None,
        dir: options.dir.clone(),
        batch: options.batch,
        value_bytes: options.value_bytes,
        records,
    })
    .await?;
    println!("corrupt_test torn tail truncated safely; all {records} records intact");

    // Flipping one byte inside the committed prefix must be detected loudly,
    // never served as data.
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| e.to_string())?;
        let length = file.metadata().map_err(|e| e.to_string())?.len();
        let target = length / 2;
        let mut byte = [0_u8; 1];
        file.seek(SeekFrom::Start(target))
            .map_err(|e| e.to_string())?;
        file.read_exact(&mut byte).map_err(|e| e.to_string())?;
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(target))
            .map_err(|e| e.to_string())?;
        file.write_all(&byte).map_err(|e| e.to_string())?;
        println!("corrupt_test flipped one committed byte at position {target}");
    }
    let detected = match ActiveSegment::recover(&path) {
        Err(problem) => format!("recovery refused the segment: {problem}"),
        Ok(segment) => {
            // Recovery may legitimately pass if the flipped byte sits in
            // already-scanned slack; the fetch path must then catch it.
            let epochs = ProducerEpochJournal::open(options.dir.join("soak.epochs"))
                .map_err(|e| e.to_string())?;
            let broker = LocalBroker::new(segment, epochs, range(), FENCING_EPOCH)
                .map(Arc::new)
                .map_err(|e| e.to_string())?;
            let harness = start(broker).await?;
            let outcome = fetch_all(&harness, options.batch, options.value_bytes).await;
            harness.stop().await?;
            match outcome {
                Err(problem) => format!("fetch detected the corruption: {problem}"),
                Ok(_) => return Err("corruption was silently served back as valid data".to_owned()),
            }
        }
    };
    println!("corrupt_test_done {detected}");
    Ok(())
}

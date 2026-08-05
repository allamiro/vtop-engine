//! Network produce/verify/status client for the chaos harness.
//!
//! Record contents are deterministic functions of the sequence number (same
//! scheme as the soak example), so `verify` checks bytes, not counts. The
//! producer persists its acknowledged floor to `--acked-file` after every
//! batch: after a `kill -9` of the server, that file is the minimum the
//! surviving cluster must still serve.

use crate::config::{load, RangeConfig, TlsPaths};
use crate::data_node::{MAX_FRAME_BYTES, MAX_RECORDS, WINDOW_BYTES};
use crate::tls;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use uuid::Uuid;
use vtop_protocol::{
    read_frame, write_frame, ClientHello, Durability, ErrorResponse, FetchRequest, Message,
    ProduceRecord, ProduceRequest, ProtocolLimits, ReplicaStatusRequest, Role, WindowUpdate,
    WireFrame, PROTOCOL_MAJOR,
};

const LIMITS: ProtocolLimits = ProtocolLimits {
    max_frame_bytes: MAX_FRAME_BYTES,
    max_records: MAX_RECORDS,
};
const FETCH_BYTES: u32 = 4 * 1024 * 1024;

/// Shared client-side session parameters, loaded from a YAML file so every
/// scenario script passes one `--client-config`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub cluster_id: Uuid,
    pub principal_id: Uuid,
    pub producer_id: Uuid,
    pub producer_epoch: u64,
    pub fencing_epoch: u64,
    pub range: RangeConfig,
    pub server_name: String,
    pub tls: TlsPaths,
}

impl ClientConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        load(path)
    }
}

type Session = tokio_rustls::client::TlsStream<TcpStream>;

async fn connect(config: &ClientConfig, addr: &str, role: Role) -> Result<Session, String> {
    let connector = TlsConnector::from(tls::client_config(&config.tls)?);
    let socket = TcpStream::connect(addr)
        .await
        .map_err(|error| format!("connect {addr}: {error}"))?;
    let name = rustls::pki_types::ServerName::try_from(config.server_name.clone())
        .map_err(|error| error.to_string())?;
    let mut stream = connector
        .connect(name, socket)
        .await
        .map_err(|error| format!("tls {addr}: {error}"))?;
    write_frame(
        &mut stream,
        &WireFrame {
            request_id: 0,
            stream_id: 0,
            message: Message::ClientHello(ClientHello {
                cluster_id: config.cluster_id,
                principal_id: config.principal_id,
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
    .map_err(|error| error.to_string())?;
    match read_frame(&mut stream, LIMITS)
        .await
        .map_err(|error| error.to_string())?
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

pub fn record_value(sequence: u64, value_bytes: usize) -> Vec<u8> {
    (0..value_bytes)
        .map(|index| (sequence.wrapping_mul(31).wrapping_add(index as u64) & 0xff) as u8)
        .collect()
}

fn persist_acked(path: Option<&PathBuf>, acked: u64) -> Result<(), String> {
    let Some(path) = path else { return Ok(()) };
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp).map_err(|error| error.to_string())?;
        writeln!(file, "{acked}").map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
    }
    std::fs::rename(&tmp, path).map_err(|error| error.to_string())
}

pub struct ProduceArgs {
    pub addr: String,
    pub records: u64,
    pub batch: u32,
    pub value_bytes: usize,
    pub first_sequence: u64,
    pub durability: Durability,
    pub acked_file: Option<PathBuf>,
}

enum ProduceBatchError {
    Interrupted(String),
    Failed(String),
}

/// Produce with per-batch acks. Exit contract for scripts:
/// - success: prints `produce_done`, exit 0
/// - server vanished mid-stream (chaos): prints `produce_interrupted
///   acked_total=N`, exit 3 — N is the durable floor to verify against.
pub async fn produce(config: &ClientConfig, args: ProduceArgs) -> Result<i32, String> {
    let mut session = connect(config, &args.addr, Role::Producer).await?;
    let started = Instant::now();
    let mut acked: u64 = 0;
    let mut request_id: u64 = 0;
    // The log offset this producer expects next, learned from the first ack
    // rather than derived from the producer sequence.
    //
    // Sequence and offset are different coordinates: a sequence is per-producer
    // and per-producer-epoch, an offset is the range's. They coincide only when
    // this producer wrote every record in the range, contiguously, from
    // sequence 0. A producer resuming after a failover satisfies none of that —
    // the range already holds records, and a bumped producer epoch restarts
    // sequences at 0 — so asserting `offset == first_sequence + acked` failed a
    // correct broker. Anchoring on the first observed offset keeps the invariant
    // that matters (each ack advances the range by exactly the records acked)
    // without assuming the range started empty or that this producer owns it.
    let mut expected_next_offset: Option<u64> = None;
    persist_acked(args.acked_file.as_ref(), args.first_sequence)?;
    while acked < args.records {
        let count = u64::from(args.batch).min(args.records - acked);
        let first = args.first_sequence + acked;
        let records = (first..first + count)
            .map(|sequence| ProduceRecord {
                timestamp_millis: sequence as i64,
                key: sequence.to_be_bytes().to_vec(),
                value: record_value(sequence, args.value_bytes),
            })
            .collect();
        request_id += 1;
        let expected_offset = expected_next_offset.map(|next| next + count);
        let outcome: Result<u64, ProduceBatchError> = async {
            write_frame(
                &mut session,
                &WireFrame {
                    request_id,
                    stream_id: 1,
                    message: Message::ProduceRequest(ProduceRequest {
                        range: config.range.identity(),
                        fencing_epoch: config.fencing_epoch,
                        producer_id: config.producer_id,
                        producer_epoch: config.producer_epoch,
                        first_sequence: first,
                        durability: args.durability,
                        records,
                    }),
                },
                LIMITS,
            )
            .await
            .map_err(|error| ProduceBatchError::Interrupted(error.to_string()))?;
            let reply = read_frame(&mut session, LIMITS)
                .await
                .map_err(|error| ProduceBatchError::Interrupted(error.to_string()))?
                .ok_or_else(|| {
                    ProduceBatchError::Interrupted("server closed the session".to_owned())
                })?;
            match reply.message {
                Message::ProduceResponse(response) => {
                    if let Some(expected) = expected_offset {
                        if response.committed_next_offset != expected {
                            return Err(ProduceBatchError::Failed(format!(
                                "committed_next_offset {} after acking {count} records; \
                                 expected {expected}",
                                response.committed_next_offset
                            )));
                        }
                    }
                    Ok(response.committed_next_offset)
                }
                Message::Error(ErrorResponse { code, message, .. }) => Err(
                    ProduceBatchError::Failed(format!("produce rejected: {code:?} {message}")),
                ),
                other => Err(ProduceBatchError::Failed(format!(
                    "unexpected produce reply: {other:?}"
                ))),
            }
        }
        .await;
        match outcome {
            Ok(committed_next_offset) => {
                expected_next_offset = Some(committed_next_offset);
                acked += count;
                persist_acked(args.acked_file.as_ref(), args.first_sequence + acked)?;
                if request_id.is_multiple_of(20) {
                    println!(
                        "acked_total={} rate={:.0}rec/s",
                        args.first_sequence + acked,
                        acked as f64 / started.elapsed().as_secs_f64()
                    );
                    std::io::stdout().flush().ok();
                }
            }
            Err(ProduceBatchError::Interrupted(error)) => {
                println!(
                    "produce_interrupted acked_total={}",
                    args.first_sequence + acked
                );
                std::io::stdout().flush().ok();
                eprintln!("produce interrupted: {error}");
                return Ok(3);
            }
            Err(ProduceBatchError::Failed(error)) => return Err(error),
        }
    }
    println!(
        "produce_done acked_total={} elapsed={:.2}s",
        args.first_sequence + acked,
        started.elapsed().as_secs_f64()
    );
    Ok(0)
}

pub struct VerifyArgs {
    pub addr: String,
    pub expect_at_least: u64,
    /// Byte-verify content below this offset; above it, check structure only.
    ///
    /// Expected content is derived from the offset, which is predictable only
    /// for records written contiguously from sequence 0 by this producer. A
    /// range holding anything else — records from another producer, or from
    /// this one after a producer-epoch bump restarts its sequences — has a
    /// suffix no reader can reconstruct. `u64::MAX` verifies everything, which
    /// is the right default for a range this producer wholly owns.
    pub verify_content_through: u64,
    pub batch: u32,
    pub value_bytes: usize,
}

/// Fetch offset 0 → committed HWM, checking every record's offset contiguity
/// and byte-verifying content below `verify_content_through`. Fails if the HWM
/// is below the acknowledged floor or a verified record's content mismatches.
pub async fn verify(config: &ClientConfig, args: VerifyArgs) -> Result<i32, String> {
    let mut session = connect(config, &args.addr, Role::Consumer).await?;
    let mut request_id: u64 = 0;
    let mut next_offset: u64 = 0;
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
        .map_err(|error| error.to_string())?;
        request_id += 1;
        write_frame(
            &mut session,
            &WireFrame {
                request_id,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range: config.range.identity(),
                    fencing_epoch: config.fencing_epoch,
                    start_offset: next_offset,
                    max_bytes: FETCH_BYTES,
                    max_records: args.batch,
                }),
            },
            LIMITS,
        )
        .await
        .map_err(|error| error.to_string())?;
        let reply = read_frame(&mut session, LIMITS)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "server closed the session".to_owned())?;
        let response = match reply.message {
            Message::FetchResponse(response) => response,
            Message::Error(ErrorResponse { code, message, .. }) => {
                return Err(format!("fetch rejected: {code:?} {message}"));
            }
            other => return Err(format!("unexpected fetch reply: {other:?}")),
        };
        if response.committed_high_watermark < next_offset {
            return Err(format!(
                "committed high watermark regressed from at least {next_offset} to {}",
                response.committed_high_watermark
            ));
        }
        for record in &response.records {
            if record.offset >= response.committed_high_watermark {
                return Err(format!(
                    "record {} was exposed at or above committed high watermark {}",
                    record.offset, response.committed_high_watermark
                ));
            }
            if record.offset != next_offset {
                return Err(format!(
                    "expected offset {next_offset}, fetched {}",
                    record.offset
                ));
            }
            if next_offset < args.verify_content_through
                && (record.key != next_offset.to_be_bytes()
                    || record.value != record_value(next_offset, args.value_bytes))
            {
                return Err(format!("record {next_offset} content mismatch"));
            }
            next_offset += 1;
        }
        if next_offset == response.committed_high_watermark {
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
    println!("verify_done records={next_offset} high_watermark={high_watermark}");
    if high_watermark < args.expect_at_least {
        return Err(format!(
            "high watermark {high_watermark} is below the acknowledged floor {}",
            args.expect_at_least
        ));
    }
    Ok(0)
}

/// Query a follower's replication status (local committed offset / next
/// offset) over the replica plane, authenticating with the leader identity.
pub async fn replica_status(
    tls_paths: &TlsPaths,
    server_name: &str,
    addr: &str,
    range: &RangeConfig,
) -> Result<i32, String> {
    let connector = TlsConnector::from(tls::client_config(tls_paths)?);
    let socket = TcpStream::connect(addr)
        .await
        .map_err(|error| format!("connect {addr}: {error}"))?;
    let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|error| error.to_string())?;
    let mut stream = connector
        .connect(name, socket)
        .await
        .map_err(|error| format!("tls {addr}: {error}"))?;
    write_frame(
        &mut stream,
        &WireFrame {
            request_id: 1,
            stream_id: 0,
            message: Message::ReplicaStatusRequest(ReplicaStatusRequest {
                range: range.identity(),
            }),
        },
        LIMITS,
    )
    .await
    .map_err(|error| error.to_string())?;
    let reply = read_frame(&mut stream, LIMITS)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "follower closed the session".to_owned())?;
    match reply.message {
        Message::ReplicaStatusResponse(status) => {
            println!(
                "replica_status local_committed_offset={} next_offset={}",
                status.local_committed_offset, status.next_offset
            );
            Ok(0)
        }
        Message::Error(ErrorResponse { code, message, .. }) => {
            Err(format!("status rejected: {code:?} {message}"))
        }
        other => Err(format!("unexpected status reply: {other:?}")),
    }
}

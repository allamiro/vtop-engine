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
    read_frame, write_frame, ClientHello, Durability, ErrorCode, ErrorResponse, FetchRequest,
    Message, ProduceRecord, ProduceRequest, ProtocolLimits, ReplicaStatusRequest, Role,
    WindowUpdate, WireFrame, PROTOCOL_MAJOR,
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
    /// rustls server name the data node's certificate carries. Unused, and
    /// may be empty, when `tls` is absent.
    #[serde(default)]
    pub server_name: String,
    /// Absent means a PLAINTEXT native session (#294): no certificate on
    /// either side, the declared principal is the whole of the evidence,
    /// and the node admits it only if its authorizer accepts unverified
    /// principals. Meant for a loopback lab; a node refuses to serve
    /// plaintext on anything else unless told so by name.
    #[serde(default)]
    pub tls: Option<TlsPaths>,
    /// How `replica-status` dials the replica plane (#294): unset follows
    /// `tls` (present means TLS with the identity it names, absent means
    /// plaintext), `plaintext` overrides it — for a range whose native
    /// plane is TLS but whose replica plane is not, which the node's two
    /// knobs allow, so one client config can serve both verbs.
    #[serde(default)]
    pub replica_transport: Option<DialTransport>,
    /// How produce/verify dial the native plane (#294): unset follows `tls`
    /// the same way, `plaintext` overrides it — for the inverse combination,
    /// a plaintext native plane behind a TLS replica plane, where the `tls`
    /// block is there for `replica-status` and must not pick the native
    /// transport on its way past.
    #[serde(default)]
    pub native_transport: Option<DialTransport>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DialTransport {
    Tls,
    Plaintext,
}

impl ClientConfig {
    /// The TLS paths to dial the replica plane with, or `None` for plaintext.
    pub fn replica_tls(&self) -> Result<Option<&TlsPaths>, String> {
        dial_tls(
            self.replica_transport,
            self.tls.as_ref(),
            "replica_transport",
        )
    }

    /// The TLS paths to dial the native plane with, or `None` for plaintext.
    pub fn native_tls(&self) -> Result<Option<&TlsPaths>, String> {
        dial_tls(self.native_transport, self.tls.as_ref(), "native_transport")
    }
}

/// One plane's dial decision (#294): the selector when set, the `tls` block's
/// presence when not — and `tls` without a block is refused by the knob's
/// name rather than dialed plaintext by accident.
fn dial_tls<'a>(
    selector: Option<DialTransport>,
    tls: Option<&'a TlsPaths>,
    knob: &str,
) -> Result<Option<&'a TlsPaths>, String> {
    match selector {
        Some(DialTransport::Plaintext) => Ok(None),
        Some(DialTransport::Tls) => tls
            .map(Some)
            .ok_or_else(|| format!("{knob} is tls but the client config has no `tls` block")),
        None => Ok(tls),
    }
}

impl ClientConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        load(path)
    }
}

type Session = vtop_meta::transport::MaybeTls<TcpStream>;

async fn connect(config: &ClientConfig, addr: &str, role: Role) -> Result<Session, String> {
    let socket = TcpStream::connect(addr)
        .await
        .map_err(|error| format!("connect {addr}: {error}"))?;
    let mut stream = match config.native_tls()? {
        Some(paths) => {
            let connector = TlsConnector::from(tls::client_config(paths)?);
            let name = rustls::pki_types::ServerName::try_from(config.server_name.clone())
                .map_err(|error| error.to_string())?;
            Session::Tls(Box::new(
                connector
                    .connect(name, socket)
                    .await
                    .map_err(|error| {
                        format!(
                            "tls {addr}: {error}{}",
                            vtop_meta::transport::tls_handshake_hint(&error)
                        )
                    })?
                    .into(),
            ))
        }
        None => {
            eprintln!(
                "warning: native session to {addr} is PLAINTEXT: no certificate on either side; \
                 the principal is declared, not proven"
            );
            Session::Plain(socket)
        }
    };
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

/// The operation history a live-chaos scenario records for the history
/// checker (#728, `scripts/live-chaos/history/check_history.py`): one JSON
/// object per line, appended and flushed as each event happens, so a process
/// killed mid-run leaves every event it got to.
///
/// Produce: an `invoke` before a batch goes out, then exactly one of `ok`
/// (acknowledged, with the committed next offset after it and the offsets the
/// broker answered with: `first_offset`, plus `offsets` when they are not
/// consecutive),
/// `fail` (refused before anything was appended, see [`refusal_is_definite`]:
/// the records must never be visible) or `info` (no answer, or one that does
/// not settle it: indeterminate, the records may or may not be visible).
/// Read: one line per run of visible records at consecutive offsets whose
/// keys carry consecutive producer sequences, and a final line with the high
/// watermark.
pub struct History {
    file: std::fs::File,
    process: String,
}

impl History {
    pub fn open(path: &std::path::Path, process: &str) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("history {}: {error}", path.display()))?;
        Ok(Self {
            file,
            process: process.to_owned(),
        })
    }

    fn event(
        &mut self,
        mut fields: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), String> {
        fields.insert("process".into(), self.process.clone().into());
        fields.insert("time_us".into(), unix_micros()?.into());
        let mut line = serde_json::Value::Object(fields).to_string();
        line.push('\n');
        self.file
            .write_all(line.as_bytes())
            .and_then(|()| self.file.flush())
            .map_err(|error| format!("history: {error}"))
    }
}

/// A JSON object from `(key, value)` pairs.
fn fields<const N: usize>(
    pairs: [(&str, serde_json::Value); N],
) -> serde_json::Map<String, serde_json::Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
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
    /// Records per second offered, averaged from the start; 0 is unpaced.
    pub rate: u64,
    /// Wall-clock origin of the rate schedule (unix micros); unset is this
    /// invocation's start.
    pub rate_anchor_unix_micros: Option<u64>,
    /// One `<unix_micros> <acked_total> <latency_micros>` line per ack.
    pub ack_log: Option<PathBuf>,
    /// A batch unanswered this long is an interruption; 0 waits forever.
    pub request_timeout_ms: u64,
    /// Record every batch's invocation and outcome here (#728).
    pub history: Option<PathBuf>,
    /// The produce targets a leader the caller has fenced: whatever it
    /// answers short of an acknowledgement, its records must never become
    /// visible, so the history records them as refused (`fail`).
    pub fenced_probe: bool,
}

/// When record number `index` of the schedule is due, at `rate` records per
/// second from `started`; `None` when unpaced. Averaged from the start, as
/// the JVM perf tools' throttle is, so one slow acknowledgement is caught up
/// rather than lowering the offered rate for the rest of the run.
fn paced_send_at(started: Instant, index: u64, rate: u64) -> Option<Instant> {
    if rate == 0 {
        return None;
    }
    Some(started + std::time::Duration::from_secs_f64(index as f64 / rate as f64))
}

/// When a batch of `count` records starting at schedule index `first` may go
/// out: once its LAST record is due. Sending when the first is due would put
/// the rest on the wire before their scheduled time, and a latency measured
/// from each record's schedule (#597) would then come out short by up to
/// `(count - 1) / rate` (19.8 ms for 100 records at 5,000/s), or negative.
fn batch_send_at(started: Instant, first: u64, count: u64, rate: u64) -> Option<Instant> {
    paced_send_at(started, first + count.saturating_sub(1), rate)
}

/// The monotonic instant standing for wall-clock `anchor_micros`, given that
/// `now` is wall-clock `now_micros`: an anchor in the past maps to before
/// `now` (or to `now` itself if the monotonic clock cannot reach back that
/// far, rather than panicking on an underflow).
fn instant_of_wall(now: Instant, now_micros: u64, anchor_micros: u64) -> Instant {
    if anchor_micros >= now_micros {
        now + std::time::Duration::from_micros(anchor_micros - now_micros)
    } else {
        let back = std::time::Duration::from_micros(now_micros - anchor_micros);
        now.checked_sub(back).unwrap_or(now)
    }
}

fn unix_micros() -> Result<u64, String> {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    u64::try_from(since.as_micros()).map_err(|error| error.to_string())
}

/// One acknowledgement-timeline line: wall-clock micros (comparable across
/// processes on one host, which a monotonic clock is not), the acknowledged
/// total after this batch, and the batch's round trip.
fn ack_line(unix_micros: u128, acked_total: u64, latency: std::time::Duration) -> String {
    format!("{unix_micros} {acked_total} {}\n", latency.as_micros())
}

enum ProduceBatchError {
    Interrupted(String),
    Failed(String),
    /// Acknowledged, at an offset other than the one expected: the batch is
    /// committed (recorded as such), and the run fails.
    AckedOutOfPlace {
        committed_next_offset: u64,
        offsets: Vec<u64>,
        message: String,
    },
    /// The broker answered with an error. Whether the records can still
    /// become visible depends on the code: see [`refusal_is_definite`].
    Refused {
        code: ErrorCode,
        message: String,
    },
}

/// Whether a produce refused with `code` was refused before anything was
/// appended, so its records can never become visible (a history's `fail`).
/// Anything else is indeterminate (`info`): `Overloaded` is how a leader
/// reports a quorum wait that expired after it appended and fsynced the batch
/// itself, `Storage` can fail part way through an append, and `Fenced` comes
/// both from the lease check at admission and from the one the broker repeats
/// after appending and replicating, before it publishes the commit, with the
/// same message either way.
fn refusal_is_definite(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::InvalidRequest
            | ErrorCode::UnsupportedVersion
            | ErrorCode::Unauthorized
            | ErrorCode::WrongCluster
            | ErrorCode::WrongRange
            | ErrorCode::SequenceConflict
            | ErrorCode::ProtocolViolation
            | ErrorCode::WrongLineage
    )
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
    let mut history = match args.history.as_ref() {
        Some(path) => Some(History::open(
            path,
            &format!(
                "produce:{}:{}:{}",
                config.producer_id,
                config.producer_epoch,
                std::process::id()
            ),
        )?),
        None => None,
    };
    let mut ack_log = match args.ack_log.as_ref() {
        Some(path) => Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|error| format!("ack log {}: {error}", path.display()))?,
        ),
        None => None,
    };
    // The schedule's origin and how far along it this invocation starts: a
    // run anchored at the shared origin is due at `first_sequence / rate`
    // past it, so a restarted producer picks the schedule up where it is.
    let (schedule_origin, schedule_offset) = match args.rate_anchor_unix_micros {
        Some(anchor) => (
            instant_of_wall(Instant::now(), unix_micros()?, anchor),
            args.first_sequence,
        ),
        None => (started, 0),
    };
    while acked < args.records {
        let count = u64::from(args.batch).min(args.records - acked);
        if let Some(at) = batch_send_at(schedule_origin, schedule_offset + acked, count, args.rate)
        {
            tokio::time::sleep_until(at.into()).await;
        }
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
        if let Some(history) = history.as_mut() {
            history.event(fields([
                ("op", "produce".into()),
                ("type", "invoke".into()),
                ("batch", request_id.into()),
                ("addr", args.addr.clone().into()),
                ("fencing_epoch", config.fencing_epoch.into()),
                ("producer_epoch", config.producer_epoch.into()),
                ("first_sequence", first.into()),
                ("count", count.into()),
                ("value_bytes", args.value_bytes.into()),
            ]))?;
        }
        let sent_at = Instant::now();
        let exchange = async {
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
                    let offsets: Vec<u64> = response.outcomes.iter().map(|o| o.offset).collect();
                    if let Some(expected) = expected_offset {
                        if response.committed_next_offset != expected {
                            return Err(ProduceBatchError::AckedOutOfPlace {
                                committed_next_offset: response.committed_next_offset,
                                offsets,
                                message: format!(
                                    "committed_next_offset {} after acking {count} records; \
                                     expected {expected}",
                                    response.committed_next_offset
                                ),
                            });
                        }
                    }
                    Ok((response.committed_next_offset, offsets))
                }
                Message::Error(ErrorResponse { code, message, .. }) => {
                    Err(ProduceBatchError::Refused { code, message })
                }
                other => Err(ProduceBatchError::Failed(format!(
                    "unexpected produce reply: {other:?}"
                ))),
            }
        };
        let outcome: Result<(u64, Vec<u64>), ProduceBatchError> = if args.request_timeout_ms == 0 {
            exchange.await
        } else {
            let limit = std::time::Duration::from_millis(args.request_timeout_ms);
            match tokio::time::timeout(limit, exchange).await {
                Ok(outcome) => outcome,
                Err(_) => Err(ProduceBatchError::Interrupted(format!(
                    "no answer to a {count}-record batch within {} ms",
                    args.request_timeout_ms
                ))),
            }
        };
        if let Some(history) = history.as_mut() {
            let (mut kind, offset, offsets, error) = match &outcome {
                Ok((next, offsets)) => ("ok", Some(*next), Some(offsets.as_slice()), None),
                Err(ProduceBatchError::AckedOutOfPlace {
                    committed_next_offset,
                    offsets,
                    message,
                }) => (
                    "ok",
                    Some(*committed_next_offset),
                    Some(offsets.as_slice()),
                    Some(message.clone()),
                ),
                Err(ProduceBatchError::Refused { code, message }) => (
                    if refusal_is_definite(*code) {
                        "fail"
                    } else {
                        "info"
                    },
                    None,
                    None,
                    Some(format!("{code:?} {message}")),
                ),
                // A reply this client did not expect: what the broker did
                // with the batch is unknown.
                Err(ProduceBatchError::Failed(error)) => ("info", None, None, Some(error.clone())),
                Err(ProduceBatchError::Interrupted(error)) => {
                    ("info", None, None, Some(error.clone()))
                }
            };
            if args.fenced_probe && kind == "info" {
                kind = "fail";
            }
            let mut event = fields([
                ("op", "produce".into()),
                ("type", kind.into()),
                ("batch", request_id.into()),
                ("first_sequence", first.into()),
                ("count", count.into()),
            ]);
            if let Some(offset) = offset {
                event.insert("committed_next_offset".into(), offset.into());
            }
            // Where the broker placed each record. A resent batch it already
            // holds is answered with its ORIGINAL offsets under the range's
            // current watermark, so the watermark alone would misplace it
            // (review of #878).
            if let Some(offsets) = offsets.filter(|o| o.len() as u64 == count) {
                if let Some(&first_offset) = offsets.first() {
                    event.insert("first_offset".into(), first_offset.into());
                }
                if !offsets_are_consecutive(offsets) {
                    event.insert("offsets".into(), offsets.to_vec().into());
                }
            }
            if let Some(error) = error {
                event.insert("error".into(), error.into());
            }
            history.event(event)?;
        }
        match outcome {
            Ok((committed_next_offset, _)) => {
                let latency = sent_at.elapsed();
                // Stamped on the answer, before the floor's fsync below: the
                // ack log is the benchmark's acknowledgement time, and local
                // checkpoint I/O is not the cluster's latency (#613 review).
                let acked_at = u128::from(unix_micros()?);
                expected_next_offset = Some(committed_next_offset);
                acked += count;
                persist_acked(args.acked_file.as_ref(), args.first_sequence + acked)?;
                if let Some(file) = ack_log.as_mut() {
                    file.write_all(
                        ack_line(acked_at, args.first_sequence + acked, latency).as_bytes(),
                    )
                    .map_err(|error| format!("ack log: {error}"))?;
                }
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
            Err(ProduceBatchError::AckedOutOfPlace { message, .. }) => return Err(message),
            Err(ProduceBatchError::Refused { code, message }) => {
                return Err(format!("produce rejected: {code:?} {message}"))
            }
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
    /// Byte-verify content for this many leading VISIBLE records; beyond
    /// that, check structure only.
    ///
    /// Expected content derives from a record's position among the records
    /// the fetch actually returns, because the producer's records keep
    /// consecutive sequences however the log spells their offsets — the key
    /// carries the sequence, and it must match the position. That is
    /// predictable only for a range whose visible records this producer
    /// wrote from sequence 0; anything else — another producer's records, or
    /// this one's after an epoch bump restarted its sequences — has a suffix
    /// no reader can reconstruct. `u64::MAX` verifies everything, which is
    /// the right default for a range this producer wholly owns.
    pub verify_content_through: u64,
    /// Offsets the consumer's view may legitimately skip before this run
    /// fails. Zero — the default, and every existing caller — is the dense
    /// contract: a skipped offset is a lost record and fails loudly. A
    /// nonzero budget is for logs that legitimately carry consumer-invisible
    /// records (#240's promotion marker, once it exists), where the caller
    /// knows how many skips to expect and anything beyond that is still a
    /// loss.
    pub max_offset_gaps: u64,
    pub batch: u32,
    pub value_bytes: usize,
    /// Append the visible records and the high watermark here, as the read
    /// half of a history (#728). Every visible record is listed, beyond
    /// `verify_content_through` too.
    pub dump: Option<PathBuf>,
    /// The first offset fetched: 0 for a whole range, or the log start of a
    /// range whose retention (#290) has reclaimed a prefix. A history read
    /// records it, so the checker does not expect the reclaimed records in
    /// this read.
    pub from_offset: u64,
}

/// A run of visible records at consecutive offsets whose keys carry
/// consecutive producer sequences, for the read half of a history.
struct ReadRun {
    offset: u64,
    sequence: u64,
    count: u64,
    /// Every value in the run is the producer's value for its key's sequence.
    values_match: bool,
}

/// Whether `offsets` run one after another, so the first names them all.
fn offsets_are_consecutive(offsets: &[u64]) -> bool {
    offsets.windows(2).all(|pair| pair[1] == pair[0] + 1)
}

/// The producer sequence a record's key carries, when it is one.
fn key_sequence(key: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(key).ok().map(u64::from_be_bytes)
}

/// Fetch `from_offset` (0 unless retention reclaimed a prefix) → committed
/// HWM, checking offset monotonicity and
/// byte-verifying content for the first `verify_content_through` visible
/// records. The cursor honors `FetchResponse.next_offset`, which is what
/// makes the wire gap-tolerant; the `max_offset_gaps` budget is what keeps
/// gap tolerance from quietly becoming loss tolerance — at the default of
/// zero, this is exactly the dense contiguity contract this tool has always
/// enforced. Fails if the HWM or the visible record count is below the
/// acknowledged floor, or a verified record's content mismatches.
pub async fn verify(config: &ClientConfig, args: VerifyArgs) -> Result<i32, String> {
    let mut session = connect(config, &args.addr, Role::Consumer).await?;
    let mut request_id: u64 = 0;
    let mut next_offset: u64 = args.from_offset;
    let mut records_seen: u64 = 0;
    let mut gaps: u64 = 0;
    let mut runs: Vec<ReadRun> = Vec::new();
    let mut unkeyed: Vec<u64> = Vec::new();
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
        let before = next_offset;
        for record in &response.records {
            if record.offset >= response.committed_high_watermark {
                return Err(format!(
                    "record {} was exposed at or above committed high watermark {}",
                    record.offset, response.committed_high_watermark
                ));
            }
            if record.offset < next_offset {
                return Err(format!(
                    "offset went backwards: expected at least {next_offset}, fetched {}",
                    record.offset
                ));
            }
            gaps += record.offset - next_offset;
            if args.dump.is_some() {
                match key_sequence(&record.key) {
                    Some(sequence) => {
                        let matches = record.value == record_value(sequence, args.value_bytes);
                        match runs.last_mut() {
                            Some(run)
                                if run.offset + run.count == record.offset
                                    && run.sequence + run.count == sequence
                                    && run.values_match == matches =>
                            {
                                run.count += 1;
                            }
                            _ => runs.push(ReadRun {
                                offset: record.offset,
                                sequence,
                                count: 1,
                                values_match: matches,
                            }),
                        }
                    }
                    None => unkeyed.push(record.offset),
                }
            }
            if records_seen < args.verify_content_through {
                // The expected record is named by its POSITION among the
                // visible records, not by its offset: the producer's records
                // keep consecutive sequences however the log spells their
                // offsets, and the key carries the sequence — so a swapped
                // or misplaced record still fails by name here.
                if record.key != records_seen.to_be_bytes()
                    || record.value != record_value(records_seen, args.value_bytes)
                {
                    return Err(format!(
                        "record at offset {} content mismatch: expected the producer's \
                         record {records_seen}",
                        record.offset
                    ));
                }
            }
            next_offset = record.offset + 1;
            records_seen += 1;
        }
        // The server's cursor may step past trailing offsets it chose not to
        // return; honoring it is the gap-tolerant half of the wire contract
        // that consumers previously ignored.
        if response.next_offset > next_offset {
            gaps += response.next_offset - next_offset;
            next_offset = response.next_offset;
        }
        if gaps > args.max_offset_gaps {
            return Err(format!(
                "{gaps} offset(s) missing from the consumer's view, over the {} this run \
                 tolerates: beyond the declared budget a skipped offset is a lost record",
                args.max_offset_gaps
            ));
        }
        if next_offset > response.committed_high_watermark {
            return Err(format!(
                "cursor {next_offset} passed the committed high watermark {}",
                response.committed_high_watermark
            ));
        }
        if next_offset == response.committed_high_watermark {
            high_watermark = response.committed_high_watermark;
            break;
        }
        if next_offset == before {
            return Err(format!(
                "no progress at offset {next_offset} below high watermark {}",
                response.committed_high_watermark
            ));
        }
    }
    println!(
        "verify_done records={records_seen} next_offset={next_offset} gaps={gaps} \
         high_watermark={high_watermark}"
    );
    if let Some(path) = args.dump.as_ref() {
        let mut history = History::open(path, &format!("read:{}", std::process::id()))?;
        for run in &runs {
            history.event(fields([
                ("op", "read".into()),
                ("type", "records".into()),
                ("offset", run.offset.into()),
                ("first_sequence", run.sequence.into()),
                ("count", run.count.into()),
                ("values_match", run.values_match.into()),
            ]))?;
        }
        for offset in unkeyed {
            history.event(fields([
                ("op", "read".into()),
                ("type", "unkeyed".into()),
                ("offset", offset.into()),
            ]))?;
        }
        history.event(fields([
            ("op", "read".into()),
            ("type", "end".into()),
            ("high_watermark", high_watermark.into()),
            ("records", records_seen.into()),
            ("start_offset", args.from_offset.into()),
        ]))?;
    }
    if high_watermark < args.expect_at_least {
        return Err(format!(
            "high watermark {high_watermark} is below the acknowledged floor {}",
            args.expect_at_least
        ));
    }
    if records_seen < args.expect_at_least {
        return Err(format!(
            "only {records_seen} records visible, below the acknowledged floor {}: a \
             high watermark can clear the floor on invisible offsets alone, and the \
             floor is a promise about RECORDS",
            args.expect_at_least
        ));
    }
    Ok(0)
}

/// Query a follower's replication status (local committed offset / next
/// offset) over the replica plane, authenticating with the leader identity.
pub async fn replica_status(
    tls_paths: Option<&TlsPaths>,
    server_name: &str,
    addr: &str,
    range: &RangeConfig,
) -> Result<i32, String> {
    let socket = TcpStream::connect(addr)
        .await
        .map_err(|error| format!("connect {addr}: {error}"))?;
    // Dialed the way the replica plane is served (#294): with the leader
    // identity under TLS, or plaintext with none — the status verb is one a
    // plaintext replica plane still answers.
    let mut stream = match tls_paths {
        Some(paths) => {
            let connector = TlsConnector::from(tls::client_config(paths)?);
            let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
                .map_err(|error| error.to_string())?;
            Session::Tls(Box::new(
                connector
                    .connect(name, socket)
                    .await
                    .map_err(|error| {
                        format!(
                            "tls {addr}: {error}{}",
                            vtop_meta::transport::tls_handshake_hint(&error)
                        )
                    })?
                    .into(),
            ))
        }
        None => Session::Plain(socket),
    };
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

#[cfg(test)]
mod pacing_tests {
    use super::*;
    use std::time::Duration;

    /// #597: the offered rate is a schedule from the start, so record N is
    /// due N/rate seconds in, and an unpaced run never waits.
    #[test]
    fn a_paced_batch_goes_out_at_its_share_of_the_schedule() {
        let started = Instant::now();
        assert_eq!(paced_send_at(started, 0, 1000), Some(started));
        assert_eq!(
            paced_send_at(started, 500, 1000),
            Some(started + Duration::from_millis(500))
        );
        assert_eq!(
            paced_send_at(started, 30_000, 10_000),
            Some(started + Duration::from_secs(3))
        );
        assert_eq!(paced_send_at(started, 500, 0), None);
    }

    /// #597 review: a batch goes out when its LAST record is due, never
    /// before, so no record is on the wire ahead of its scheduled send time
    /// and a latency measured from the schedule cannot come out negative.
    /// 100 records from index 0 at 5,000/s: the last (index 99) is due at
    /// 19.8 ms, not at 0 as the first-record schedule sent it.
    #[test]
    fn a_paced_batch_waits_until_its_last_record_is_due() {
        let started = Instant::now();
        let at = |first, count, rate| batch_send_at(started, first, count, rate);
        assert_eq!(at(0, 100, 5000), paced_send_at(started, 99, 5000));
        assert_eq!(
            at(0, 100, 5000),
            Some(started + Duration::from_secs_f64(99.0 / 5000.0))
        );
        assert!(at(0, 100, 5000) > paced_send_at(started, 0, 5000));
        // Every record of the batch is due at or before the send.
        for index in 0..100 {
            assert!(paced_send_at(started, index, 5000) <= at(0, 100, 5000));
        }
        // One record is due at its own index; a resumed batch at its own.
        assert_eq!(at(7, 1, 1000), paced_send_at(started, 7, 1000));
        assert_eq!(at(1000, 100, 1000), paced_send_at(started, 1099, 1000));
        assert_eq!(at(0, 100, 0), None);
    }

    /// A restarted producer anchored at the run's origin resumes the one
    /// schedule: the wall-clock anchor maps onto the monotonic clock in
    /// both directions, and a sequence past the anchor is due that much
    /// later.
    #[test]
    fn an_anchored_schedule_maps_wall_clock_onto_the_monotonic_clock() {
        let now = Instant::now() + Duration::from_secs(3600);
        assert_eq!(
            instant_of_wall(now, 10_000_000, 12_500_000),
            now + Duration::from_millis(2500)
        );
        assert_eq!(
            instant_of_wall(now, 10_000_000, 9_000_000),
            now - Duration::from_secs(1)
        );
        let origin = instant_of_wall(now, 10_000_000, 4_000_000);
        // 6 s ago at 1,000 records/s: sequence 6,000 is due right now.
        assert_eq!(paced_send_at(origin, 6000, 1000), Some(now));
    }

    #[test]
    fn an_ack_line_carries_wall_clock_total_and_round_trip() {
        assert_eq!(
            ack_line(1_700_000_000_123_456, 4200, Duration::from_micros(3_517)),
            "1700000000123456 4200 3517\n"
        );
    }
}

#[cfg(test)]
mod dial_tests {
    use super::*;
    use std::path::PathBuf;

    /// #294: the two planes are dialed by their own knobs; the `tls` block's
    /// presence is only the default each falls back to.
    #[test]
    fn each_plane_is_dialed_by_its_own_selector_and_falls_back_to_the_block() {
        let paths = TlsPaths {
            ca: PathBuf::from("ca.pem"),
            cert: PathBuf::from("cert.pem"),
            key: PathBuf::from("key.pem"),
        };
        assert!(dial_tls(None, Some(&paths), "native_transport")
            .unwrap()
            .is_some());
        assert!(dial_tls(None, None, "native_transport").unwrap().is_none());
        // The inverse combination: a TLS block for the replica plane that must
        // not pick TLS for a plaintext native plane.
        assert!(dial_tls(
            Some(DialTransport::Plaintext),
            Some(&paths),
            "native_transport"
        )
        .unwrap()
        .is_none());
        assert!(
            dial_tls(Some(DialTransport::Tls), Some(&paths), "replica_transport")
                .unwrap()
                .is_some()
        );
        let refused = dial_tls(Some(DialTransport::Tls), None, "replica_transport").unwrap_err();
        assert!(
            refused.contains("replica_transport") && refused.contains("`tls`"),
            "{refused}"
        );
    }
}

#[cfg(test)]
mod history_tests {
    use super::*;

    /// #728: a refusal is recorded as definite (`fail`, the records must
    /// never surface) only when the broker refuses before it appends. A
    /// quorum wait that expired is `Overloaded` after the leader's own
    /// append, so calling it definite would make the checker report the
    /// retried batch's records as phantoms.
    #[test]
    fn only_a_refusal_made_before_the_append_is_definite() {
        for code in [
            ErrorCode::WrongRange,
            ErrorCode::WrongCluster,
            ErrorCode::Unauthorized,
            ErrorCode::SequenceConflict,
        ] {
            assert!(refusal_is_definite(code), "{code:?}");
        }
        // `Fenced` too can come after the append: the broker checks the lease
        // again before it publishes a replicated batch's commit.
        for code in [ErrorCode::Overloaded, ErrorCode::Storage, ErrorCode::Fenced] {
            assert!(!refusal_is_definite(code), "{code:?}");
        }
    }

    /// The read dump's runs are keyed by the producer's sequence, which
    /// `produce` writes as the key: eight big-endian bytes, nothing else.
    #[test]
    fn a_batch_is_named_by_its_first_offset_only_when_its_offsets_are_consecutive() {
        assert!(offsets_are_consecutive(&[4, 5, 6]));
        assert!(offsets_are_consecutive(&[9]));
        assert!(offsets_are_consecutive(&[]));
        assert!(!offsets_are_consecutive(&[4, 6, 7]));
        assert!(!offsets_are_consecutive(&[5, 4]));
    }

    #[test]
    fn a_key_is_a_producer_sequence_only_at_eight_bytes() {
        assert_eq!(key_sequence(&42u64.to_be_bytes()), Some(42));
        assert_eq!(key_sequence(b"k"), None);
        assert_eq!(key_sequence(&[0u8; 9]), None);
    }
}

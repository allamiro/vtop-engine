//! An external Kafka cluster as a [`Bridge`] (#458 slice 2).
//!
//! The crate already speaks the protocol, so this backend is a Kafka
//! *client* over those codecs — not `rdkafka`. Pulling librdkafka (and its
//! cmake build) into the data node was the dependency the slice plan asked
//! to make deliberately; speaking ourselves keeps it out of the default
//! build and still produces, fetches, and lists offsets against any broker
//! that serves the versions this gateway serves.
//!
//! A broker that cannot be reached is `BROKER_NOT_AVAILABLE`, never an empty
//! fetch. Partition 0 is what this gateway virtualizes today (one log per
//! Kafka topic name); a produce for another partition is the leader's to
//! refuse.

use crate::api::{TIMESTAMP_EARLIEST, TIMESTAMP_LATEST};
use crate::bridge::{Appended, Bridge, Fetched, Sequenced};
use crate::messages::{frame, ErrorCode};
use crate::records::RecordBatch;
use crate::wire::{Decoder, Encoder};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How the client reaches the cluster and which names it advertises.
#[derive(Debug, Clone)]
pub struct RemoteConfig {
    /// `host:port` bootstrap brokers. The first that accepts a TCP
    /// connection is used; Metadata then names the leader a produce or
    /// fetch actually dials.
    pub bootstrap: Vec<String>,
    /// Kafka topic names this backend serves. Metadata of the gateway lists
    /// these even when the cluster is down; produce and fetch then fail by
    /// name rather than advertising a lie.
    pub topics: Vec<String>,
    pub timeout: Duration,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReplayKey {
    topic: String,
    producer_id: i64,
    producer_epoch: i16,
}

struct ReplaySet {
    first_sequence: i32,
    fingerprint: u64,
    appended: Appended,
}

struct InflightSet {
    first_sequence: i32,
    fingerprint: u64,
}

#[derive(Clone, Copy)]
struct RemoteIdentity {
    producer_id: i64,
    producer_epoch: i16,
    /// Local `first_sequence` at the moment this remote id was minted.
    /// Kafka requires a new producer id to start at sequence 0, so a
    /// gateway restart that mints a replacement still forwards the client's
    /// already-advanced sequence as `first_sequence - local_base`.
    local_base: i32,
}

struct ProducerBook {
    recent: HashMap<ReplayKey, VecDeque<ReplaySet>>,
    inflight: HashMap<ReplayKey, VecDeque<InflightSet>>,
    remote_ids: HashMap<ReplayKey, RemoteIdentity>,
}

/// Whether the Produce bytes can have reached the broker.
enum SendError {
    /// Connect or local setup failed; a retry may send.
    Unsent(ErrorCode),
    /// The request may already have been appended; a retry must not send again
    /// as a new non-idempotent Produce.
    Ambiguous(ErrorCode),
}

impl SendError {
    fn code(self) -> ErrorCode {
        match self {
            Self::Unsent(code) | Self::Ambiguous(code) => code,
        }
    }
}

/// Kafka produce errors that do not prove the leader rejected the append.
fn produce_reply_error(raw: i16) -> SendError {
    match raw {
        7 | 19 => SendError::Ambiguous(ErrorCode::from_i16(raw)),
        // UNKNOWN_PRODUCER_ID (54) / INVALID_PRODUCER_EPOCH (47): the
        // leader rejected the batch; a retry may mint a replacement id.
        47 | 54 => SendError::Unsent(ErrorCode::InvalidProducerEpoch),
        other => SendError::Unsent(ErrorCode::from_i16(other)),
    }
}

fn producer_identity_stale(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::InvalidProducerEpoch)
}

/// A [`Bridge`] over an external Kafka cluster.
pub struct RemoteBridge {
    config: RemoteConfig,
    /// Sequenced produce bookkeeping, one mutex: check-and-mark of in-flight
    /// sets is atomic with replay, and unresolved sets are never evicted.
    book: Mutex<ProducerBook>,
}

impl RemoteBridge {
    pub fn new(config: RemoteConfig) -> Result<Self, String> {
        if config.bootstrap.is_empty() {
            return Err(
                "kafka remote backend has no brokers: set `brokers` to the cluster clients dial"
                    .to_owned(),
            );
        }
        if config.topics.is_empty() {
            return Err(
                "kafka remote backend serves no topics: name the topics this route is for"
                    .to_owned(),
            );
        }
        for name in &config.topics {
            if name.is_empty() {
                return Err("kafka remote backend names a topic with no name".to_owned());
            }
        }
        Ok(Self {
            config,
            book: Mutex::new(ProducerBook {
                recent: HashMap::new(),
                inflight: HashMap::new(),
                remote_ids: HashMap::new(),
            }),
        })
    }

    fn rpc(
        &self,
        addr: SocketAddr,
        key: i16,
        version: i16,
        body: &[u8],
    ) -> Result<Vec<u8>, ErrorCode> {
        self.rpc_tracked(addr, key, version, body, self.config.timeout)
            .map_err(SendError::code)
    }

    fn rpc_tracked(
        &self,
        addr: SocketAddr,
        key: i16,
        version: i16,
        body: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, SendError> {
        if timeout.is_zero() {
            return Err(SendError::Unsent(ErrorCode::RequestTimedOut));
        }
        let mut header = Encoder::new();
        header.i16(key);
        header.i16(version);
        header.i32(1);
        header.nullable_string(Some("vtop-remote"));
        header.raw(body);
        let framed = frame(header.as_slice());
        let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|error| {
            tracing::warn!(%addr, %error, "kafka remote: bootstrap connect failed");
            SendError::Unsent(ErrorCode::BrokerNotAvailable)
        })?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|_| SendError::Unsent(ErrorCode::BrokerNotAvailable))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|_| SendError::Unsent(ErrorCode::BrokerNotAvailable))?;
        stream
            .write_all(&framed)
            .map_err(|_| SendError::Ambiguous(ErrorCode::BrokerNotAvailable))?;
        let mut len_buf = [0_u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|_| SendError::Ambiguous(ErrorCode::BrokerNotAvailable))?;
        let len = i32::from_be_bytes(len_buf);
        if len < 4 || len as usize > 32 * 1024 * 1024 {
            return Err(SendError::Ambiguous(ErrorCode::CorruptMessage));
        }
        let mut body = vec![0_u8; len as usize];
        stream
            .read_exact(&mut body)
            .map_err(|_| SendError::Ambiguous(ErrorCode::BrokerNotAvailable))?;
        let mut d = Decoder::new(&body);
        let correlation = d
            .i32("correlation")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        if correlation != 1 {
            return Err(SendError::Ambiguous(ErrorCode::CorruptMessage));
        }
        Ok(body[4..].to_vec())
    }

    fn host_port(host: &str, port: i32) -> String {
        if host.starts_with('[') || host.ends_with(']') {
            return String::new();
        }
        if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        }
    }

    fn rpc_any(
        &self,
        addrs: impl IntoIterator<Item = SocketAddr>,
        key: i16,
        version: i16,
        body: &[u8],
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut last = ErrorCode::BrokerNotAvailable;
        let mut tried = false;
        let deadline = Instant::now() + self.config.timeout;
        for addr in addrs {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ErrorCode::RequestTimedOut);
            }
            tried = true;
            match self.rpc_tracked(addr, key, version, body, remaining) {
                Ok(reply) => return Ok(reply),
                Err(error) => last = error.code(),
            }
        }
        if !tried {
            return Err(ErrorCode::BrokerNotAvailable);
        }
        Err(last)
    }

    fn bootstrap_addrs(&self) -> Vec<SocketAddr> {
        let mut addrs = Vec::new();
        for broker in &self.config.bootstrap {
            if let Ok(resolved) = broker.to_socket_addrs() {
                addrs.extend(resolved);
            }
        }
        addrs
    }

    fn leader_of(&self, topic: &str) -> Result<SocketAddr, ErrorCode> {
        let mut body = Encoder::new();
        body.array_len(1);
        body.string(topic);
        let reply = self.rpc_any(self.bootstrap_addrs(), 3, 1, body.as_slice())?;
        let mut d = Decoder::new(&reply);
        let brokers = d
            .array_len("brokers")
            .map_err(|_| ErrorCode::CorruptMessage)?
            .unwrap_or(0);
        let mut endpoints = Vec::with_capacity(brokers);
        for _ in 0..brokers {
            let node = d.i32("node").map_err(|_| ErrorCode::CorruptMessage)?;
            let host = d
                .string("host")
                .map_err(|_| ErrorCode::CorruptMessage)?
                .to_owned();
            let port = d.i32("port").map_err(|_| ErrorCode::CorruptMessage)?;
            d.nullable_string("rack").ok();
            endpoints.push((node, host, port));
        }
        d.i32("controller").map_err(|_| ErrorCode::CorruptMessage)?;
        let topics = d
            .array_len("topics")
            .map_err(|_| ErrorCode::CorruptMessage)?
            .unwrap_or(0);
        if topics == 0 {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        let error = d.i16("error").map_err(|_| ErrorCode::CorruptMessage)?;
        if error != 0 {
            return Err(ErrorCode::from_i16(error));
        }
        d.string("name").map_err(|_| ErrorCode::CorruptMessage)?;
        d.bool("internal").ok();
        let partitions = d
            .array_len("partitions")
            .map_err(|_| ErrorCode::CorruptMessage)?
            .unwrap_or(0);
        if partitions == 0 {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        let p_error = d.i16("p_error").map_err(|_| ErrorCode::CorruptMessage)?;
        if p_error != 0 {
            return Err(ErrorCode::from_i16(p_error));
        }
        d.i32("index").map_err(|_| ErrorCode::CorruptMessage)?;
        let leader = d.i32("leader").map_err(|_| ErrorCode::CorruptMessage)?;
        let (_, host, port) = endpoints
            .into_iter()
            .find(|(node, _, _)| *node == leader)
            .ok_or(ErrorCode::NotLeaderOrFollower)?;
        Self::socket_addr(&host, port).ok_or(ErrorCode::BrokerNotAvailable)
    }

    fn socket_addr(host: &str, port: i32) -> Option<SocketAddr> {
        if host.starts_with('[') || host.ends_with(']') {
            return None;
        }
        let encoded = Self::host_port(host, port);
        if encoded.is_empty() {
            return None;
        }
        encoded
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
    }

    fn encode_batches(batches: &[RecordBatch], identity: Option<(RemoteIdentity, i32)>) -> Vec<u8> {
        let mut out = Vec::new();
        match identity {
            None => {
                for batch in batches {
                    out.extend(RecordBatch::encode(
                        batch.base_offset,
                        -1,
                        -1,
                        -1,
                        &batch.records,
                    ));
                }
            }
            Some((remote, mut sequence)) => {
                for batch in batches {
                    out.extend(RecordBatch::encode(
                        batch.base_offset,
                        remote.producer_id,
                        remote.producer_epoch,
                        sequence,
                        &batch.records,
                    ));
                    sequence = sequence.saturating_add(batch.records.len() as i32);
                }
            }
        }
        out
    }

    fn produce_once(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        identity: Option<(RemoteIdentity, i32)>,
    ) -> Result<Appended, SendError> {
        let leader = self.leader_of(topic).map_err(SendError::Unsent)?;
        let records = Self::encode_batches(batches, identity);
        let mut body = Encoder::new();
        body.nullable_string(None);
        body.i16(-1); // acks=all
        body.i32(1_500);
        body.array_len(1);
        body.string(topic);
        body.array_len(1);
        body.i32(0);
        body.nullable_bytes(Some(&records));
        let reply = self.rpc_tracked(leader, 0, 5, body.as_slice(), self.config.timeout)?;
        let mut d = Decoder::new(&reply);
        d.array_len("topics")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        d.string("topic")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        d.array_len("partitions")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        d.i32("index")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        let error = d
            .i16("error")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        if error != 0 {
            return Err(produce_reply_error(error));
        }
        let base_offset = d
            .i64("base")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        let log_append_time_ms = d
            .i64("append")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        let log_start_offset = d
            .i64("start")
            .map_err(|_| SendError::Ambiguous(ErrorCode::CorruptMessage))?;
        Ok(Appended {
            base_offset,
            log_append_time_ms,
            log_start_offset,
        })
    }

    fn lock_book(&self) -> std::sync::MutexGuard<'_, ProducerBook> {
        self.book
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn replay_key(topic: &str, sequenced: Sequenced) -> ReplayKey {
        ReplayKey {
            topic: topic.to_owned(),
            producer_id: sequenced.producer_id,
            producer_epoch: sequenced.producer_epoch,
        }
    }

    fn replay_in(
        book: &ProducerBook,
        topic: &str,
        sequenced: Sequenced,
        fingerprint: u64,
    ) -> Result<Option<Appended>, ErrorCode> {
        let Some(window) = book.recent.get(&Self::replay_key(topic, sequenced)) else {
            return Ok(None);
        };
        match window
            .iter()
            .rev()
            .find(|set| set.first_sequence == sequenced.first_sequence)
        {
            Some(set) if set.fingerprint == fingerprint => Ok(Some(set.appended.clone())),
            Some(_) => Err(ErrorCode::InvalidRecord),
            None => Ok(None),
        }
    }

    fn inflight_in(
        book: &ProducerBook,
        topic: &str,
        sequenced: Sequenced,
        fingerprint: u64,
    ) -> Result<bool, ErrorCode> {
        let Some(window) = book.inflight.get(&Self::replay_key(topic, sequenced)) else {
            return Ok(false);
        };
        match window
            .iter()
            .rev()
            .find(|set| set.first_sequence == sequenced.first_sequence)
        {
            Some(set) if set.fingerprint == fingerprint => Ok(true),
            Some(_) => Err(ErrorCode::InvalidRecord),
            None => Ok(false),
        }
    }

    fn mark_inflight_in(
        book: &mut ProducerBook,
        topic: &str,
        sequenced: Sequenced,
        fingerprint: u64,
    ) {
        let window = book
            .inflight
            .entry(Self::replay_key(topic, sequenced))
            .or_default();
        if window.iter().any(|set| {
            set.first_sequence == sequenced.first_sequence && set.fingerprint == fingerprint
        }) {
            return;
        }
        window.push_back(InflightSet {
            first_sequence: sequenced.first_sequence,
            fingerprint,
        });
    }

    fn remember_in(
        book: &mut ProducerBook,
        topic: &str,
        sequenced: Sequenced,
        fingerprint: u64,
        appended: Appended,
    ) {
        let window = book
            .recent
            .entry(Self::replay_key(topic, sequenced))
            .or_default();
        if window.len() == 5 {
            window.pop_front();
        }
        window.push_back(ReplaySet {
            first_sequence: sequenced.first_sequence,
            fingerprint,
            appended,
        });
    }

    fn clear_inflight_in(book: &mut ProducerBook, topic: &str, sequenced: Sequenced) {
        let key = Self::replay_key(topic, sequenced);
        let Some(window) = book.inflight.get_mut(&key) else {
            return;
        };
        window.retain(|set| set.first_sequence != sequenced.first_sequence);
        if window.is_empty() {
            book.inflight.remove(&key);
        }
    }

    fn init_remote_producer(&self) -> Result<(i64, i16), ErrorCode> {
        let mut body = Encoder::new();
        body.nullable_string(None);
        body.i32(60_000);
        let reply = self.rpc_any(self.bootstrap_addrs(), 22, 1, body.as_slice())?;
        let mut d = Decoder::new(&reply);
        d.i32("throttle").map_err(|_| ErrorCode::CorruptMessage)?;
        let error = d.i16("error").map_err(|_| ErrorCode::CorruptMessage)?;
        if error != 0 {
            return Err(ErrorCode::from_i16(error));
        }
        let producer_id = d
            .i64("producer_id")
            .map_err(|_| ErrorCode::CorruptMessage)?;
        let producer_epoch = d
            .i16("producer_epoch")
            .map_err(|_| ErrorCode::CorruptMessage)?;
        if producer_id < 0 {
            return Err(ErrorCode::KafkaStorageError);
        }
        Ok((producer_id, producer_epoch))
    }

    fn ensure_remote_id(
        &self,
        topic: &str,
        sequenced: Sequenced,
    ) -> Result<RemoteIdentity, ErrorCode> {
        let key = Self::replay_key(topic, sequenced);
        {
            let book = self.lock_book();
            if let Some(id) = book.remote_ids.get(&key) {
                return Ok(*id);
            }
        }
        let (producer_id, producer_epoch) = self.init_remote_producer()?;
        let minted = RemoteIdentity {
            producer_id,
            producer_epoch,
            local_base: sequenced.first_sequence,
        };
        let mut book = self.lock_book();
        Ok(*book.remote_ids.entry(key).or_insert(minted))
    }

    fn forget_remote_id(&self, topic: &str, sequenced: Sequenced) {
        self.lock_book()
            .remote_ids
            .remove(&Self::replay_key(topic, sequenced));
    }

    fn remote_sequence(remote: RemoteIdentity, first_sequence: i32) -> i32 {
        first_sequence.wrapping_sub(remote.local_base)
    }
}

impl Bridge for RemoteBridge {
    fn topics(&self) -> Vec<String> {
        self.config.topics.clone()
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        if !self.config.topics.iter().any(|name| name == topic) {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        if let Some(identity) = sequenced {
            let fingerprint = crate::bridge::set_fingerprint(batches);
            let mut retried_identity = false;
            loop {
                let remote = self.ensure_remote_id(topic, identity)?;
                {
                    let mut book = self.lock_book();
                    if let Some(previous) = Self::replay_in(&book, topic, identity, fingerprint)? {
                        return Ok(previous);
                    }
                    Self::inflight_in(&book, topic, identity, fingerprint)?;
                    Self::mark_inflight_in(&mut book, topic, identity, fingerprint);
                }
                let remote_seq = Self::remote_sequence(remote, identity.first_sequence);
                match self.produce_once(topic, batches, Some((remote, remote_seq))) {
                    Ok(appended) => {
                        let mut book = self.lock_book();
                        Self::clear_inflight_in(&mut book, topic, identity);
                        Self::remember_in(
                            &mut book,
                            topic,
                            identity,
                            fingerprint,
                            appended.clone(),
                        );
                        return Ok(appended);
                    }
                    Err(SendError::Unsent(code))
                        if producer_identity_stale(code) && !retried_identity =>
                    {
                        {
                            let mut book = self.lock_book();
                            Self::clear_inflight_in(&mut book, topic, identity);
                        }
                        self.forget_remote_id(topic, identity);
                        retried_identity = true;
                    }
                    Err(SendError::Unsent(code)) => {
                        let mut book = self.lock_book();
                        Self::clear_inflight_in(&mut book, topic, identity);
                        return Err(code);
                    }
                    Err(SendError::Ambiguous(code)) => return Err(code),
                }
            }
        } else {
            self.produce_once(topic, batches, None)
                .map_err(SendError::code)
        }
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        if !self.config.topics.iter().any(|name| name == topic) {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        let leader = self.leader_of(topic)?;
        let mut body = Encoder::new();
        body.i32(-1);
        body.i32(0);
        body.i32(1);
        body.i32(i32::try_from(max_bytes).unwrap_or(i32::MAX));
        body.i8(1);
        body.array_len(1);
        body.string(topic);
        body.array_len(1);
        body.i32(0);
        body.i64(offset);
        body.i64(-1); // log_start_offset (v5): a client is not a replica
        body.i32(i32::try_from(max_bytes).unwrap_or(i32::MAX));
        let reply = self.rpc(leader, 1, 5, body.as_slice())?;
        let mut d = Decoder::new(&reply);
        d.i32("throttle").map_err(|_| ErrorCode::CorruptMessage)?;
        d.array_len("topics")
            .map_err(|_| ErrorCode::CorruptMessage)?;
        d.string("topic").map_err(|_| ErrorCode::CorruptMessage)?;
        d.array_len("partitions")
            .map_err(|_| ErrorCode::CorruptMessage)?;
        d.i32("index").map_err(|_| ErrorCode::CorruptMessage)?;
        let error = d.i16("error").map_err(|_| ErrorCode::CorruptMessage)?;
        if error != 0 {
            return Err(ErrorCode::from_i16(error));
        }
        let high_watermark = d.i64("hwm").map_err(|_| ErrorCode::CorruptMessage)?;
        d.i64("lso").map_err(|_| ErrorCode::CorruptMessage)?;
        let log_start_offset = d.i64("start").map_err(|_| ErrorCode::CorruptMessage)?;
        let aborted = d
            .array_len("aborted")
            .map_err(|_| ErrorCode::CorruptMessage)?
            .unwrap_or(0);
        for _ in 0..aborted {
            d.i64("aborted_producer")
                .map_err(|_| ErrorCode::CorruptMessage)?;
            d.i64("aborted_first_offset")
                .map_err(|_| ErrorCode::CorruptMessage)?;
        }
        let records = d
            .nullable_bytes("records")
            .map_err(|_| ErrorCode::CorruptMessage)?
            .unwrap_or(&[])
            .to_vec();
        Ok(Fetched {
            records,
            high_watermark,
            log_start_offset,
        })
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        if !self.config.topics.iter().any(|name| name == topic) {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        let leader = self.leader_of(topic)?;
        let lookup = |timestamp: i64| -> Result<i64, ErrorCode> {
            let mut body = Encoder::new();
            body.i32(-1);
            body.array_len(1);
            body.string(topic);
            body.array_len(1);
            body.i32(0);
            body.i64(timestamp);
            let reply = self.rpc(leader, 2, 1, body.as_slice())?;
            let mut d = Decoder::new(&reply);
            d.array_len("topics")
                .map_err(|_| ErrorCode::CorruptMessage)?;
            d.string("topic").map_err(|_| ErrorCode::CorruptMessage)?;
            d.array_len("partitions")
                .map_err(|_| ErrorCode::CorruptMessage)?;
            d.i32("index").map_err(|_| ErrorCode::CorruptMessage)?;
            let error = d.i16("error").map_err(|_| ErrorCode::CorruptMessage)?;
            if error != 0 {
                return Err(ErrorCode::from_i16(error));
            }
            d.i64("timestamp").map_err(|_| ErrorCode::CorruptMessage)?;
            d.i64("offset").map_err(|_| ErrorCode::CorruptMessage)
        };
        Ok((lookup(TIMESTAMP_EARLIEST)?, lookup(TIMESTAMP_LATEST)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::gateway::{Gateway, GatewayConfig};
    use crate::records::Record;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn batch(values: &[&str]) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: values
                .iter()
                .enumerate()
                .map(|(i, v)| Record {
                    offset: i as i64,
                    timestamp_millis: 1,
                    key: None,
                    value: Some(v.as_bytes().to_vec()),
                    headers: Vec::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn a_remote_without_brokers_or_topics_is_refused_before_it_dials() {
        assert!(RemoteBridge::new(RemoteConfig {
            bootstrap: Vec::new(),
            topics: vec!["events".to_owned()],
            timeout: Duration::from_secs(1),
        })
        .err()
        .expect("no brokers")
        .contains("no brokers"));
        assert!(RemoteBridge::new(RemoteConfig {
            bootstrap: vec!["127.0.0.1:9092".to_owned()],
            topics: Vec::new(),
            timeout: Duration::from_secs(1),
        })
        .err()
        .expect("no topics")
        .contains("no topics"));
    }

    #[test]
    fn an_unreachable_cluster_is_broker_not_available_not_an_empty_fetch() {
        let remote = RemoteBridge::new(RemoteConfig {
            bootstrap: vec!["127.0.0.1:1".to_owned()],
            topics: vec!["events".to_owned()],
            timeout: Duration::from_millis(50),
        })
        .unwrap();
        assert_eq!(remote.topics(), vec!["events".to_owned()]);
        assert_eq!(remote.bounds("events"), Err(ErrorCode::BrokerNotAvailable));
        assert_eq!(
            remote.fetch("events", 0, 1024),
            Err(ErrorCode::BrokerNotAvailable)
        );
        assert_eq!(
            remote.produce("nope", &[batch(&["a"])], None),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
    }

    #[tokio::test]
    async fn produce_fetch_and_bounds_round_trip_against_a_gateway() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::new(
            Arc::new(MemoryBridge::with_topics(["events"])),
            GatewayConfig {
                advertised_port: addr.port() as i32,
                ..GatewayConfig::default()
            },
        );
        tokio::spawn(gateway.serve(listener, rx));
        let remote = Arc::new(
            RemoteBridge::new(RemoteConfig {
                bootstrap: vec![format!("127.0.0.1:{}", addr.port())],
                topics: vec!["events".to_owned()],
                timeout: Duration::from_secs(2),
            })
            .unwrap(),
        );
        let produced = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || {
                remote.produce("events", &[batch(&["a", "b"])], None)
            })
            .await
            .unwrap()
            .unwrap()
        };
        assert_eq!(produced.base_offset, 0);
        let fetched = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || remote.fetch("events", 0, 1 << 20))
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(fetched.high_watermark, 2);
        assert_eq!(fetched.log_start_offset, 0);
        assert!(!fetched.records.is_empty());
        let sequenced = Sequenced {
            producer_id: 9,
            producer_epoch: 0,
            first_sequence: 0,
        };
        let first = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || {
                remote.produce("events", &[batch(&["c"])], Some(sequenced))
            })
            .await
            .unwrap()
            .unwrap()
        };
        let retry = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || {
                remote.produce("events", &[batch(&["c"])], Some(sequenced))
            })
            .await
            .unwrap()
            .unwrap()
        };
        assert_eq!(first.base_offset, retry.base_offset);
        let mismatch = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || {
                remote.produce("events", &[batch(&["NO"])], Some(sequenced))
            })
            .await
            .unwrap()
        };
        assert_eq!(
            mismatch,
            Err(ErrorCode::InvalidRecord),
            "the same sequence with other bytes is not a retry"
        );
        let next = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || {
                remote.produce(
                    "events",
                    &[batch(&["d"])],
                    Some(Sequenced {
                        producer_id: 9,
                        producer_epoch: 0,
                        first_sequence: 1,
                    }),
                )
            })
            .await
            .unwrap()
            .unwrap()
        };
        assert_eq!(next.base_offset, 3);
        let bounds = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || remote.bounds("events"))
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            bounds,
            (0, 4),
            "the sequenced retry appended nothing; sequence 1 landed once"
        );
        let _ = tx.send(true);
    }

    #[test]
    fn a_produce_timeout_after_the_leader_answered_is_ambiguous() {
        assert!(matches!(
            produce_reply_error(ErrorCode::RequestTimedOut.as_i16()),
            SendError::Ambiguous(_)
        ));
        assert!(matches!(produce_reply_error(19), SendError::Ambiguous(_)));
        assert!(matches!(
            produce_reply_error(ErrorCode::UnknownTopicOrPartition.as_i16()),
            SendError::Unsent(_)
        ));
        assert!(matches!(
            produce_reply_error(54),
            SendError::Unsent(ErrorCode::InvalidProducerEpoch)
        ));
        assert_eq!(
            RemoteBridge::remote_sequence(
                RemoteIdentity {
                    producer_id: 1,
                    producer_epoch: 0,
                    local_base: 50,
                },
                50
            ),
            0
        );
        assert_eq!(
            RemoteBridge::remote_sequence(
                RemoteIdentity {
                    producer_id: 1,
                    producer_epoch: 0,
                    local_base: 50,
                },
                51
            ),
            1
        );
    }

    #[test]
    fn an_ipv6_leader_host_is_bracketed_before_it_is_dialed() {
        assert_eq!(
            RemoteBridge::host_port("2001:db8::1", 9092),
            "[2001:db8::1]:9092"
        );
        assert!(
            RemoteBridge::host_port("[::1]", 9092).is_empty(),
            "Metadata carries host without URL-authority brackets"
        );
        assert_eq!(RemoteBridge::host_port("127.0.0.1", 9092), "127.0.0.1:9092");
    }
}

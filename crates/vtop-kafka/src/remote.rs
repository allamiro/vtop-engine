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
use std::time::Duration;

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

/// A [`Bridge`] over an external Kafka cluster.
pub struct RemoteBridge {
    config: RemoteConfig,
    /// Idempotent retries this client already sent (#458): the remote
    /// cluster does not share this gateway's producer ids, so a sequenced
    /// retry that the primary already acknowledged must not produce again
    /// on the shadow. Keyed by topic as well as producer identity: one
    /// RemoteBridge may serve several names, and a retry on B must not
    /// ack A's offset. Five sets, matching the memory backend's window.
    recent: Mutex<HashMap<ReplayKey, VecDeque<ReplaySet>>>,
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
            recent: Mutex::new(HashMap::new()),
        })
    }

    fn rpc(
        &self,
        addr: SocketAddr,
        key: i16,
        version: i16,
        body: &[u8],
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut header = Encoder::new();
        header.i16(key);
        header.i16(version);
        header.i32(1);
        header.nullable_string(Some("vtop-remote"));
        header.raw(body);
        let framed = frame(header.as_slice());
        let mut stream =
            TcpStream::connect_timeout(&addr, self.config.timeout).map_err(|error| {
                tracing::warn!(%addr, %error, "kafka remote: bootstrap connect failed");
                ErrorCode::BrokerNotAvailable
            })?;
        stream
            .set_read_timeout(Some(self.config.timeout))
            .map_err(|_| ErrorCode::BrokerNotAvailable)?;
        stream
            .set_write_timeout(Some(self.config.timeout))
            .map_err(|_| ErrorCode::BrokerNotAvailable)?;
        stream
            .write_all(&framed)
            .map_err(|_| ErrorCode::BrokerNotAvailable)?;
        let mut len_buf = [0_u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|_| ErrorCode::BrokerNotAvailable)?;
        let len = i32::from_be_bytes(len_buf);
        if len < 4 || len as usize > 32 * 1024 * 1024 {
            return Err(ErrorCode::CorruptMessage);
        }
        let mut body = vec![0_u8; len as usize];
        stream
            .read_exact(&mut body)
            .map_err(|_| ErrorCode::BrokerNotAvailable)?;
        let mut d = Decoder::new(&body);
        let correlation = d
            .i32("correlation")
            .map_err(|_| ErrorCode::CorruptMessage)?;
        if correlation != 1 {
            return Err(ErrorCode::CorruptMessage);
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
        for addr in addrs {
            tried = true;
            match self.rpc(addr, key, version, body) {
                Ok(reply) => return Ok(reply),
                Err(error) => last = error,
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

    fn encode_batches(batches: &[RecordBatch]) -> Vec<u8> {
        let mut out = Vec::new();
        for batch in batches {
            // The remote cluster did not mint this gateway's producer ids.
            // Forwarding them is UNKNOWN_PRODUCER_ID there; local sequenced
            // retries are remembered in `recent` instead.
            out.extend(RecordBatch::encode(
                batch.base_offset,
                -1,
                -1,
                -1,
                &batch.records,
            ));
        }
        out
    }

    fn produce_once(&self, topic: &str, batches: &[RecordBatch]) -> Result<Appended, ErrorCode> {
        let leader = self.leader_of(topic)?;
        let records = Self::encode_batches(batches);
        let mut body = Encoder::new();
        body.nullable_string(None);
        body.i16(-1); // acks=all
        body.i32(1_500);
        body.array_len(1);
        body.string(topic);
        body.array_len(1);
        body.i32(0);
        body.nullable_bytes(Some(&records));
        let reply = self.rpc(leader, 0, 5, body.as_slice())?;
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
        let base_offset = d.i64("base").map_err(|_| ErrorCode::CorruptMessage)?;
        let log_append_time_ms = d.i64("append").map_err(|_| ErrorCode::CorruptMessage)?;
        let log_start_offset = d.i64("start").map_err(|_| ErrorCode::CorruptMessage)?;
        Ok(Appended {
            base_offset,
            log_append_time_ms,
            log_start_offset,
        })
    }

    fn replay(
        &self,
        topic: &str,
        sequenced: Sequenced,
        fingerprint: u64,
    ) -> Result<Option<Appended>, ErrorCode> {
        let recent = self
            .recent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(window) = recent.get(&ReplayKey {
            topic: topic.to_owned(),
            producer_id: sequenced.producer_id,
            producer_epoch: sequenced.producer_epoch,
        }) else {
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

    fn remember(&self, topic: &str, sequenced: Sequenced, fingerprint: u64, appended: Appended) {
        let mut recent = self
            .recent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let window = recent
            .entry(ReplayKey {
                topic: topic.to_owned(),
                producer_id: sequenced.producer_id,
                producer_epoch: sequenced.producer_epoch,
            })
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
            if let Some(previous) = self.replay(topic, identity, fingerprint)? {
                return Ok(previous);
            }
            let appended = self.produce_once(topic, batches)?;
            self.remember(topic, identity, fingerprint, appended.clone());
            return Ok(appended);
        }
        self.produce_once(topic, batches)
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
        let bounds = {
            let remote = Arc::clone(&remote);
            tokio::task::spawn_blocking(move || remote.bounds("events"))
                .await
                .unwrap()
                .unwrap()
        };
        assert_eq!(bounds, (0, 3), "the sequenced retry appended nothing");
        let _ = tx.send(true);
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

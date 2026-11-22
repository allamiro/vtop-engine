//! A cap on the connections one listener holds open at once (#732: M6, R7).
//!
//! The metadata peer and admin planes, and the replica plane, spawn one task
//! per accepted connection. The frame bounds limit what one connection costs;
//! nothing limited how many there were, so a client holding a certificate the
//! CA signed (or anything that reaches a plaintext plane) could open them
//! until the node ran out of file descriptors or memory.
//!
//! A connection over the cap is closed at accept, before its handshake or
//! first frame: queueing it would only move the unbounded growth into the
//! accept backlog. Connections already open are not touched. Each refusal is
//! counted, and said on stderr at most once per window per plane, with the
//! number suppressed.
//!
//! A slot is held from accept, so a connection that never finishes its
//! handshake must not hold one forever: a client with no certificate could
//! otherwise take every slot by opening sockets and sending nothing, and the
//! cap would turn a flood into a lockout (review). Each connection gets one
//! deadline from accept across the dialect check, the TLS handshake and its
//! first frame ([`DEFAULT_HANDSHAKE_DEADLINE`]); one that misses it is closed
//! and counted the same way.

use super::maybe_tls::warn_rate_limited;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How long a connection on a capped listener may take from accept to its
/// first frame: the dialect check, the TLS handshake and the first request.
/// A healthy peer takes milliseconds; the bound only has to be short enough
/// that silent sockets cannot keep every slot.
pub const DEFAULT_HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// Default for the metadata peer plane (`max_peer_connections`). Raft opens
/// one short-lived connection per RPC, so a healthy group of a few voters
/// holds a handful at once.
pub const DEFAULT_MAX_PEER_CONNECTIONS: usize = 256;

/// Default for the metadata admin plane (`max_admin_connections`). Every data
/// node's lease agent, watcher and gateway dial it, one connection per
/// request, so it gets more room than the peer plane.
pub const DEFAULT_MAX_ADMIN_CONNECTIONS: usize = 1024;

/// Default for the replica plane (`max_replica_connections`). A data node
/// serves one range: its leader's session, status probes and repairs.
pub const DEFAULT_MAX_REPLICA_CONNECTIONS: usize = 256;

/// The slots of one listener, and how many connections it has refused.
#[derive(Debug)]
pub struct ConnectionLimit {
    plane: &'static str,
    label: &'static str,
    setting: &'static str,
    max: usize,
    slots: Arc<Semaphore>,
    refused: AtomicU64,
    handshake_deadline: Duration,
    handshake_timeouts: AtomicU64,
}

impl ConnectionLimit {
    /// `plane` names the listener in messages ("metadata peer"), `label` is
    /// its metric label value ("meta_peer"), `setting` is the config key that
    /// changes `max`. A `max` of zero is raised to one: a listener that admits
    /// nothing is a misconfiguration the node config refuses before this.
    pub fn new(
        plane: &'static str,
        label: &'static str,
        setting: &'static str,
        max: usize,
    ) -> Self {
        let max = max.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            plane,
            label,
            setting,
            max,
            slots: Arc::new(Semaphore::new(max)),
            refused: AtomicU64::new(0),
            handshake_deadline: DEFAULT_HANDSHAKE_DEADLINE,
            handshake_timeouts: AtomicU64::new(0),
        }
    }

    /// Give each connection `deadline` from accept to its first frame
    /// instead of [`DEFAULT_HANDSHAKE_DEADLINE`].
    pub fn with_handshake_deadline(mut self, deadline: Duration) -> Self {
        self.handshake_deadline = deadline;
        self
    }

    /// The deadline from accept to a connection's first frame.
    pub fn handshake_deadline(&self) -> Duration {
        self.handshake_deadline
    }

    /// Count and say (rate-limited) that the connection from `peer` missed
    /// its handshake deadline. The caller closes it, which frees its slot.
    pub fn handshake_expired(&self, peer: SocketAddr) {
        self.handshake_timeouts.fetch_add(1, Ordering::Relaxed);
        warn_rate_limited(
            &format!("connection-handshake:{}", self.label),
            &self.handshake_timeout_message(peer),
        );
    }

    /// What a missed handshake deadline says.
    pub fn handshake_timeout_message(&self, peer: SocketAddr) -> String {
        format!(
            "{plane} endpoint closed a connection from {peer}: it did not finish its handshake \
             and send its first request within {deadline:?} of being accepted, and it was \
             holding one of the `{setting}` slots. The slot is free again; open connections are \
             unaffected. A healthy peer takes milliseconds: if this repeats, find what at {peer} \
             opens connections and sends nothing. \
             vtop_listener_connections_refused_total{{plane=\"{label}\",reason=\"handshake_timeout\"}} \
             counts every one",
            plane = self.plane,
            deadline = self.handshake_deadline,
            setting = self.setting,
            label = self.label,
        )
    }

    /// Connections closed for missing the handshake deadline.
    pub fn handshake_timeouts_total(&self) -> u64 {
        self.handshake_timeouts.load(Ordering::Relaxed)
    }

    /// The metadata peer plane's limit.
    pub fn meta_peer(max: usize) -> Self {
        Self::new("metadata peer", "meta_peer", "max_peer_connections", max)
    }

    /// The metadata admin plane's limit.
    pub fn meta_admin(max: usize) -> Self {
        Self::new("admin", "meta_admin", "max_admin_connections", max)
    }

    /// The replica plane's limit.
    pub fn replica(max: usize) -> Self {
        Self::new("replica", "replica", "max_replica_connections", max)
    }

    /// A slot for a connection just accepted from `peer`, held until the
    /// connection's task ends; `None` when every slot is taken, after
    /// counting the refusal and saying so (rate-limited). The caller closes
    /// the socket.
    pub fn admit(&self, peer: SocketAddr) -> Option<OwnedSemaphorePermit> {
        match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(slot) => Some(slot),
            Err(_) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                warn_rate_limited(
                    &format!("connection-limit:{}", self.label),
                    &self.refusal_message(peer),
                );
                None
            }
        }
    }

    /// What a refusal says: what happened, what the node did, what to do.
    pub fn refusal_message(&self, peer: SocketAddr) -> String {
        format!(
            "{plane} endpoint closed a connection from {peer}: {max} connections are already \
             open, the most `{setting}` allows. The new connection was closed before its \
             handshake; open connections are unaffected. If the load is legitimate, raise \
             `{setting}` in the node config; if not, find what at {peer} keeps opening \
             connections. \
             vtop_listener_connections_refused_total{{plane=\"{label}\",reason=\"capacity\"}} \
             counts every refusal",
            plane = self.plane,
            max = self.max,
            setting = self.setting,
            label = self.label,
        )
    }

    /// Connections refused at accept for the cap since the listener started.
    pub fn refused_total(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// The metric label value naming this listener.
    pub fn label(&self) -> &'static str {
        self.label
    }

    /// The most connections held open at once.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Connections open now.
    pub fn open(&self) -> usize {
        self.max - self.slots.available_permits()
    }
}

/// What a listener test needs to see a cap at work over real sockets.
#[cfg(test)]
pub(crate) mod probe {
    use super::ConnectionLimit;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    /// Wait (bounded) until `done` holds.
    pub async fn eventually(what: &str, done: impl Fn() -> bool) {
        for _ in 0..500 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting until {what}");
    }

    /// Whether the server closed this connection without a word.
    pub async fn closed_by_server(stream: &mut TcpStream) -> bool {
        let mut byte = [0_u8; 1];
        matches!(
            tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await,
            Ok(Ok(0)) | Ok(Err(_))
        )
    }

    /// A cap of one and a short handshake deadline: a connection that sends
    /// nothing is closed at the deadline and counted, and its slot then
    /// serves the next connection. Without the deadline the silent socket
    /// would hold the only slot for as long as it stayed open.
    pub async fn a_silent_connection_loses_its_slot_at_the_deadline(
        addr: SocketAddr,
        limit: &ConnectionLimit,
    ) {
        let mut silent = TcpStream::connect(addr).await.unwrap();
        eventually("the silent connection holds the slot", || limit.open() == 1).await;
        assert!(
            closed_by_server(&mut silent).await,
            "the silent connection was not closed at its deadline"
        );
        assert_eq!(limit.handshake_timeouts_total(), 1);
        eventually("the slot is freed", || limit.open() == 0).await;
        let _next = TcpStream::connect(addr).await.unwrap();
        eventually("a new connection takes the freed slot", || {
            limit.open() == 1
        })
        .await;
        assert_eq!(limit.refused_total(), 0, "nothing was refused for the cap");
    }

    /// The cap of one, end to end: the first connection is served and held,
    /// the second is closed and counted, and once the first ends its slot
    /// serves a new connection.
    pub async fn the_second_of_a_cap_of_one_is_closed(addr: SocketAddr, limit: &ConnectionLimit) {
        let held = TcpStream::connect(addr).await.unwrap();
        eventually("the first connection holds the slot", || limit.open() == 1).await;
        let mut over = TcpStream::connect(addr).await.unwrap();
        assert!(
            closed_by_server(&mut over).await,
            "the connection over the cap was not closed"
        );
        assert_eq!(limit.refused_total(), 1);
        assert_eq!(limit.open(), 1, "the held connection is untouched");
        drop(held);
        eventually("the first connection's slot is freed", || limit.open() == 0).await;
        let _next = TcpStream::connect(addr).await.unwrap();
        eventually("a new connection takes the freed slot", || {
            limit.open() == 1
        })
        .await;
        assert_eq!(
            limit.refused_total(),
            1,
            "the freed slot was served, not refused"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> SocketAddr {
        "192.0.2.7:4100".parse().unwrap()
    }

    #[test]
    fn the_connection_over_the_cap_is_refused_and_counted_and_a_freed_slot_is_reused() {
        let limit = ConnectionLimit::meta_peer(2);
        let first = limit.admit(peer()).expect("first slot");
        let _second = limit.admit(peer()).expect("second slot");
        assert_eq!(limit.open(), 2);
        assert!(limit.admit(peer()).is_none(), "the third is over the cap");
        assert_eq!(limit.refused_total(), 1);
        drop(first);
        assert!(limit.admit(peer()).is_some(), "a freed slot is served");
        assert_eq!(limit.refused_total(), 1);
    }

    #[test]
    fn the_refusal_names_the_plane_the_cap_the_setting_and_the_metric() {
        let message = ConnectionLimit::replica(3).refusal_message(peer());
        for needed in [
            "replica endpoint closed a connection from 192.0.2.7:4100",
            "3 connections are already open",
            "`max_replica_connections`",
            "open connections are unaffected",
            "vtop_listener_connections_refused_total{plane=\"replica\",reason=\"capacity\"}",
        ] {
            assert!(
                message.contains(needed),
                "{needed:?} missing from {message}"
            );
        }
    }

    #[test]
    fn a_zero_cap_is_raised_to_one() {
        let limit = ConnectionLimit::meta_admin(0);
        assert_eq!(limit.max(), 1);
        assert!(limit.admit(peer()).is_some());
    }
}

//! A bridge that may be empty: the seam that lets one gateway outlive its
//! leadership (#751).
//!
//! A candidate node binds its Kafka listener once, at startup, and holds the
//! range only while it holds the lease. The listener therefore cannot be
//! built over the node's broker, which exists only while it leads. It is
//! built over a [`BridgeSlot`] instead: empty while the node follows, holding
//! the leader's bridge between a promotion and the demotion that follows it.
//! The address never moves, so a client that reconnects to it finds a gateway
//! that says, by code, who to ask next.
//!
//! What the slot guarantees, and what each guarantee is for:
//!
//! - **Empty means refused, by name.** A call with nothing installed answers
//!   `NOT_LEADER_OR_FOLLOWER` for a topic the slot was built to serve, and
//!   `UNKNOWN_TOPIC_OR_PARTITION` for one it was not. It never answers with an
//!   empty success, so a client cannot read "this node has nothing" as "the
//!   topic is empty".
//! - **One call, one bridge.** A call resolves the installed bridge once, at
//!   its start, and finishes on that bridge. A call that began under an old
//!   leadership therefore never lands on the next leadership's bridge, and a
//!   call that began after the slot was cleared never touches the old one.
//!   The fencing epoch is the real safety net (the old broker refuses on its
//!   own, and the next one mints a producer epoch above the journal's), but a
//!   call that cannot cross a swap does not need it to.
//! - **Retired means returned.** [`BridgeSlot::clear`] and a replacing
//!   [`BridgeSlot::install`] hand the old bridge back to the caller rather
//!   than dropping it. The caller drops it after the gateway has quiesced
//!   ([`crate::gateway::GatewayHandle::quiesce`]), which is the point at which
//!   no call can still hold it: the directory behind a broker has exactly one
//!   owner while it is reopened.

use crate::bridge::{Appended, Bridge, Fetched, Sequenced};
use crate::messages::ErrorCode;
use crate::records::RecordBatch;
use std::sync::{Arc, RwLock};

/// What is installed and the generation that installed it.
struct Installed {
    generation: u64,
    bridge: Arc<dyn Bridge>,
}

#[derive(Default)]
struct State {
    /// Counts installs and clears, so a witness can tell "the bridge it saw"
    /// from "a bridge installed since". Never reused.
    generation: u64,
    installed: Option<Installed>,
}

/// A [`Bridge`] that forwards to whatever is installed, and refuses while
/// nothing is.
pub struct BridgeSlot {
    /// The topics this slot answers for while empty: the names a Metadata
    /// request lists on a node that does not hold the range. The installed
    /// bridge is authoritative once there is one.
    topics: Vec<String>,
    state: RwLock<State>,
}

impl BridgeSlot {
    /// An empty slot that serves `topics` once something is installed.
    pub fn new<I, S>(topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            topics: topics.into_iter().map(Into::into).collect(),
            state: RwLock::new(State::default()),
        }
    }

    /// Install `bridge`, returning the one it replaced, if any, and the
    /// generation the new one carries. A node installs after it has completed
    /// a promotion and never over a bridge it has not cleared, so a `Some` is
    /// a caller that is replacing one on purpose; it owns the old bridge until
    /// it has quiesced the gateway.
    pub fn install(&self, bridge: Arc<dyn Bridge>) -> (u64, Option<Arc<dyn Bridge>>) {
        let mut state = self.write();
        state.generation += 1;
        let generation = state.generation;
        let retired = state
            .installed
            .replace(Installed { generation, bridge })
            .map(|old| old.bridge);
        (generation, retired)
    }

    /// Empty the slot: every call that starts after this is refused. Returns
    /// the bridge that was installed, still alive, for the caller to drop once
    /// the gateway has quiesced and no call can hold it.
    pub fn clear(&self) -> Option<Arc<dyn Bridge>> {
        let mut state = self.write();
        let retired = state.installed.take().map(|old| old.bridge);
        if retired.is_some() {
            state.generation += 1;
        }
        retired
    }

    /// Whether a bridge is installed now.
    pub fn is_installed(&self) -> bool {
        self.read().installed.is_some()
    }

    /// The generation of the installed bridge, `None` while empty.
    pub fn installed_generation(&self) -> Option<u64> {
        self.read().installed.as_ref().map(|i| i.generation)
    }

    /// The installed bridge, resolved once for one call. The lock is held
    /// only for the clone: a produce holds the bridge through its fsync, and
    /// a demotion must never wait behind it to empty the slot.
    fn current(&self) -> Option<Arc<dyn Bridge>> {
        self.read()
            .installed
            .as_ref()
            .map(|installed| Arc::clone(&installed.bridge))
    }

    fn refusal(&self, topic: &str) -> ErrorCode {
        if self.topics.iter().any(|name| name == topic) {
            ErrorCode::NotLeaderOrFollower
        } else {
            ErrorCode::UnknownTopicOrPartition
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Bridge for BridgeSlot {
    fn topics(&self) -> Vec<String> {
        match self.current() {
            Some(bridge) => bridge.topics(),
            None => self.topics.clone(),
        }
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        match self.current() {
            Some(bridge) => bridge.produce(topic, batches, sequenced),
            None => Err(self.refusal(topic)),
        }
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        match self.current() {
            Some(bridge) => bridge.fetch(topic, offset, max_bytes),
            None => Err(self.refusal(topic)),
        }
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        match self.current() {
            Some(bridge) => bridge.bounds(topic),
            None => Err(self.refusal(topic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::records::Record;

    fn batch(value: &[u8]) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                offset: 0,
                timestamp_millis: 1,
                key: None,
                value: Some(value.to_vec()),
                headers: Vec::new(),
            }],
        }
    }

    #[test]
    fn an_empty_slot_refuses_by_name_and_lists_what_it_was_built_for() {
        let slot = BridgeSlot::new(["events"]);
        assert_eq!(slot.topics(), vec!["events".to_owned()]);
        assert_eq!(
            slot.produce("events", &[batch(b"a")], None),
            Err(ErrorCode::NotLeaderOrFollower)
        );
        assert_eq!(
            slot.fetch("events", 0, 1024).map(|_| ()),
            Err(ErrorCode::NotLeaderOrFollower)
        );
        assert_eq!(slot.bounds("events"), Err(ErrorCode::NotLeaderOrFollower));
        // A name it was not built for is not a topic this range has, whoever
        // holds the range.
        assert_eq!(
            slot.bounds("other"),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert!(!slot.is_installed());
    }

    #[test]
    fn an_installed_bridge_serves_and_clearing_refuses_again() {
        let slot = BridgeSlot::new(["events"]);
        let (generation, retired) = slot.install(Arc::new(MemoryBridge::with_topics(["events"])));
        assert!(retired.is_none());
        assert_eq!(slot.installed_generation(), Some(generation));
        let appended = slot.produce("events", &[batch(b"a")], None).unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(slot.bounds("events"), Ok((0, 1)));

        let retired = slot.clear();
        assert!(retired.is_some(), "the caller gets the bridge back");
        assert_eq!(slot.installed_generation(), None);
        assert_eq!(
            slot.produce("events", &[batch(b"b")], None),
            Err(ErrorCode::NotLeaderOrFollower)
        );
        assert!(slot.clear().is_none(), "clearing an empty slot is a no-op");
    }

    #[test]
    fn a_replacing_install_returns_the_old_bridge_and_a_new_generation() {
        let slot = BridgeSlot::new(["events"]);
        let first = Arc::new(MemoryBridge::with_topics(["events"]));
        let second = Arc::new(MemoryBridge::with_topics(["events"]));
        let (g1, _) = slot.install(Arc::clone(&first) as Arc<dyn Bridge>);
        let (g2, retired) = slot.install(Arc::clone(&second) as Arc<dyn Bridge>);
        assert!(g2 > g1, "a generation is never reused");
        assert!(retired.is_some());
        slot.produce("events", &[batch(b"a")], None).unwrap();
        assert_eq!(first.bounds("events"), Ok((0, 0)), "the old bridge is idle");
        assert_eq!(second.bounds("events"), Ok((0, 1)));
    }

    /// A bridge whose produce parks until released, so a test can hold a call
    /// open across a swap.
    struct Gated {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        appended: std::sync::atomic::AtomicUsize,
    }

    impl Bridge for Gated {
        fn topics(&self) -> Vec<String> {
            vec!["events".to_owned()]
        }
        fn produce(
            &self,
            _topic: &str,
            _batches: &[RecordBatch],
            _sequenced: Option<Sequenced>,
        ) -> Result<Appended, ErrorCode> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            self.appended
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Appended {
                base_offset: 0,
                log_append_time_ms: -1,
                log_start_offset: 0,
            })
        }
        fn fetch(&self, _: &str, _: i64, _: usize) -> Result<Fetched, ErrorCode> {
            Err(ErrorCode::NotLeaderOrFollower)
        }
        fn bounds(&self, _: &str) -> Result<(i64, i64), ErrorCode> {
            Ok((0, 0))
        }
    }

    /// The witness for "one call, one bridge": a produce that began under the
    /// old bridge finishes there while the slot is swapped under it, and the
    /// new bridge sees none of it. Red against a slot that resolved the
    /// bridge after the call began.
    #[test]
    fn a_call_that_began_under_the_old_bridge_never_appends_through_the_new_one() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let old = Arc::new(Gated {
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
            appended: std::sync::atomic::AtomicUsize::new(0),
        });
        let new = Arc::new(MemoryBridge::with_topics(["events"]));
        let slot = Arc::new(BridgeSlot::new(["events"]));
        slot.install(Arc::clone(&old) as Arc<dyn Bridge>);

        let call = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || slot.produce("events", &[batch(b"in flight")], None))
        };
        entered_rx.recv().unwrap(); // the call is inside the old bridge
        let (_, retired) = slot.install(Arc::clone(&new) as Arc<dyn Bridge>);
        assert!(retired.is_some());
        release_tx.send(()).unwrap();
        let outcome = call.join().unwrap();

        assert!(outcome.is_ok());
        assert_eq!(
            old.appended.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the call finished on the bridge it began on"
        );
        assert_eq!(
            new.bounds("events"),
            Ok((0, 0)),
            "nothing reached the bridge installed after the call began"
        );
    }

    /// Dropping the slot's bridge is the caller's act: once cleared, the only
    /// owner left is whoever was handed the retired bridge.
    #[test]
    fn a_cleared_bridge_is_owned_only_by_the_caller_that_was_handed_it() {
        let slot = BridgeSlot::new(["events"]);
        let bridge: Arc<dyn Bridge> = Arc::new(MemoryBridge::with_topics(["events"]));
        let witness = Arc::downgrade(&bridge);
        slot.install(bridge);
        assert!(witness.upgrade().is_some());
        let retired = slot.clear();
        assert!(
            witness.upgrade().is_some(),
            "alive until the caller drops it"
        );
        drop(retired);
        assert!(
            witness.upgrade().is_none(),
            "the slot kept no second owner of a bridge it was told to clear"
        );
    }
}

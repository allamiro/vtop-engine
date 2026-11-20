//! The Kafka gateway on a candidate node (#751).
//!
//! A candidate binds its Kafka listener once, at startup, and holds the range
//! only while it holds the lease. The gateway behind the listener is built
//! once too, over a [`BridgeSlot`] and an [`OffsetSlot`] that are empty while
//! the node follows. What changes with the role is what is installed in them:
//!
//! - **Promotion**, after the promotion has completed and the broker is
//!   installed: the consumer groups are forgotten (a group from an earlier
//!   leadership is never judged against), a fresh offset store over this
//!   broker is installed, and then the native bridge over this broker, which
//!   mints a producer epoch above the journal's. From that call on the
//!   gateway serves.
//! - **Demotion**, in #280 order: the bridge and the store are cleared (every
//!   call that starts after this is refused, and Metadata names the holder),
//!   the groups are forgotten so parked members go to the new coordinator,
//!   and the gateway's in-flight calls are waited for. Only then are the
//!   retired bridge and store dropped, and only after that may the node drop
//!   the broker: the directory behind it must have exactly one owner when it
//!   is reopened.
//!
//! While the node follows, the gateway answers from what the lease agent last
//! read: Metadata lists every member and names the holder as the partition's
//! leader, and FindCoordinator names the holder, so a client that bootstraps
//! from any member follows a failover with no restart and no config change.

use crate::lease_agent::ObservedHolder;
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::LocalBroker;
use vtop_kafka::{
    Bridge, BridgeSlot, Gateway, GatewayConfig, GatewayHandle, HolderView, LeaseState, LeaseView,
    NativeBridge, NativeBridgeConfig, OffsetSlot, OffsetStore, Quiesced, RangeHolder,
};

/// Builds the offset store a leadership commits through, over that
/// leadership's broker.
pub(crate) type OffsetStoreFactory =
    Box<dyn Fn(&Arc<LocalBroker>) -> Arc<dyn OffsetStore> + Send + Sync>;

/// The lease as the candidate's broker slot holds it: the installed broker's
/// view, read as a leader reads its one broker, and `Gone` while no broker is
/// installed (the node follows).
pub(crate) struct SlotLeaseView(pub(crate) Arc<vtop_broker::BrokerSlot>);

impl LeaseView for SlotLeaseView {
    fn lease(&self) -> LeaseState {
        match self.0.current() {
            Some(broker) => crate::data_node::broker_lease(&broker),
            None => LeaseState::Gone,
        }
    }
}

/// The holder the lease agent last read, as a Kafka broker id (#751): mapped
/// through `kafka.brokers`, and `None` for a holder with no entry, which the
/// configuration refuses for every member.
pub(crate) struct WatchedHolder {
    seen: tokio::sync::watch::Receiver<Option<ObservedHolder>>,
    brokers: Vec<(Uuid, i32)>,
}

impl WatchedHolder {
    pub(crate) fn new(
        seen: tokio::sync::watch::Receiver<Option<ObservedHolder>>,
        brokers: Vec<(Uuid, i32)>,
    ) -> Self {
        Self { seen, brokers }
    }
}

impl HolderView for WatchedHolder {
    fn holder(&self) -> Option<RangeHolder> {
        let seen = (*self.seen.borrow())?;
        let broker_id = self
            .brokers
            .iter()
            .find(|(node, _)| *node == seen.node_uuid)
            .map(|(_, id)| *id)?;
        Some(RangeHolder {
            broker_id,
            epoch: seen.fencing_epoch,
        })
    }
}

/// A candidate's gateway, apart from its listener: what a promotion installs
/// and a demotion clears.
pub(crate) struct CandidateKafka {
    range: Uuid,
    advertised: String,
    bridge: Arc<BridgeSlot>,
    offsets: Arc<OffsetSlot>,
    handle: GatewayHandle,
    bridge_config: NativeBridgeConfig,
    store: Option<OffsetStoreFactory>,
}

impl CandidateKafka {
    /// The gateway, built once, and the handle on it a promotion and a
    /// demotion use. The caller serves the gateway on the listener it bound
    /// at startup.
    pub(crate) fn new(
        range: Uuid,
        config: GatewayConfig,
        bridge_config: NativeBridgeConfig,
        lease: Arc<dyn LeaseView>,
        holders: Arc<dyn HolderView>,
        store: Option<OffsetStoreFactory>,
    ) -> (Self, Gateway) {
        let advertised = format!("{}:{}", config.advertised_host, config.advertised_port);
        let bridge = Arc::new(BridgeSlot::new([bridge_config.topic.clone()]));
        let offsets = Arc::new(OffsetSlot::empty());
        let gateway = Gateway::new(Arc::clone(&bridge) as Arc<dyn Bridge>, config)
            .with_lease(lease)
            .with_holder(holders)
            .with_offsets(Arc::clone(&offsets) as Arc<dyn OffsetStore>);
        let handle = gateway.handle();
        (
            Self {
                range,
                advertised,
                bridge,
                offsets,
                handle,
                bridge_config,
                store,
            },
            gateway,
        )
    }

    /// Whether the gateway serves the range now: a bridge is installed.
    pub(crate) fn is_serving(&self) -> bool {
        self.bridge.is_installed()
    }

    /// The promotion's last step: serve the range through `broker`, which
    /// the node has promoted to `epoch` and installed. A bridge that cannot
    /// be built (its producer epochs are exhausted, which no deployment
    /// reaches) leaves the gateway redirecting and says so; the native plane
    /// serves regardless.
    pub(crate) fn promote(&self, broker: &Arc<LocalBroker>, epoch: u64) {
        let bridge = match NativeBridge::new(Arc::clone(broker), self.bridge_config.clone()) {
            Ok(bridge) => Arc::new(bridge) as Arc<dyn Bridge>,
            Err(error) => {
                tracing::error!(
                    range = %self.range,
                    epoch,
                    %error,
                    "kafka gateway for range {} cannot serve at epoch {epoch}: {error}. The node \
                     leads on the native plane and its Kafka listener keeps redirecting; restart \
                     the node to mint a fresh producer epoch",
                    self.range
                );
                return;
            }
        };
        let store = self.store.as_ref().map(|build| build(broker));
        self.install(bridge, store, epoch);
    }

    /// Install a leadership's bridge and store: the groups first, so nothing
    /// from an earlier leadership is judged against, then the store, then the
    /// bridge, which is the moment the gateway serves.
    pub(crate) fn install(
        &self,
        bridge: Arc<dyn Bridge>,
        store: Option<Arc<dyn OffsetStore>>,
        epoch: u64,
    ) {
        self.handle.reset_groups();
        if let Some(store) = store {
            // A promotion never installs over a store it did not clear; one
            // that does is dropped here, after nothing can reach it again.
            drop(self.offsets.install(store));
        }
        let (_, replaced) = self.bridge.install(bridge);
        debug_assert!(replaced.is_none(), "a promotion follows a demotion");
        tracing::info!(
            range = %self.range,
            epoch,
            advertised = %self.advertised,
            "kafka gateway for range {} is now serving at epoch {epoch}, advertised as {}; \
             clients that asked another member are redirected here by metadata",
            self.range,
            self.advertised
        );
    }

    /// Stop serving, in #280 order, and return once nothing the gateway
    /// started can still hold the retired bridge or store. `role` is what the
    /// node becomes (`follower`, or `shutdown`), for the operator's line.
    ///
    /// Refused first: every call that starts after the slots are cleared is
    /// refused without touching a bridge. Then the groups are released, and
    /// the calls in flight are waited for, past the gateway's ceiling if they
    /// must be (a WARN says so). Only then are the retired bridge and store
    /// dropped. The caller drops the broker after this returns.
    pub(crate) async fn demote(&self, role: &str) -> Quiesced {
        let retired_bridge = self.bridge.clear();
        let retired_store = self.offsets.clear();
        self.handle.reset_groups();
        let ceiling = self.handle.ceiling();
        let range = self.range;
        let quiesced = self
            .handle
            .quiesce(|in_flight| {
                tracing::warn!(
                    range = %range,
                    ceiling_ms = ceiling.as_millis() as u64,
                    in_flight,
                    "kafka gateway drain exceeded {} ms with {in_flight} calls in flight; the \
                     broker is already fenced so none of them can append, and the range is \
                     released after they end",
                    ceiling.as_millis()
                );
            })
            .await;
        let was_serving = retired_bridge.is_some();
        drop(retired_bridge);
        drop(retired_store);
        if was_serving {
            tracing::info!(
                range = %self.range,
                role,
                in_flight = quiesced.in_flight,
                drained_ms = quiesced.waited.as_millis() as u64,
                "kafka gateway for range {} stopped serving ({role}): {} in-flight calls drained \
                 in {} ms. Clients are redirected by metadata; nothing is needed.",
                self.range,
                quiesced.in_flight,
                quiesced.waited.as_millis()
            );
        }
        quiesced
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The holder the agent read is named by its Kafka broker id, at the
    /// epoch it holds; a holder with no `kafka.brokers` entry, or none at
    /// all, is named to nobody.
    #[test]
    fn the_watched_holder_is_named_by_its_broker_id() {
        let a = Uuid::from_u128(0xA);
        let b = Uuid::from_u128(0xB);
        let (tx, rx) = tokio::sync::watch::channel(None);
        let view = WatchedHolder::new(rx, vec![(a, 1), (b, 2)]);
        assert_eq!(view.holder(), None);
        tx.send_replace(Some(ObservedHolder {
            node_uuid: b,
            fencing_epoch: 9,
        }));
        assert_eq!(
            view.holder(),
            Some(RangeHolder {
                broker_id: 2,
                epoch: 9
            })
        );
        tx.send_replace(Some(ObservedHolder {
            node_uuid: Uuid::from_u128(0xC),
            fencing_epoch: 10,
        }));
        assert_eq!(view.holder(), None, "no entry, no name");
        tx.send_replace(None);
        assert_eq!(view.holder(), None);
    }

    /// An empty broker slot is a node that does not hold the range.
    #[test]
    fn an_empty_broker_slot_reads_as_a_lease_gone() {
        let view = SlotLeaseView(Arc::new(vtop_broker::BrokerSlot::empty()));
        assert_eq!(view.lease(), LeaseState::Gone);
    }
}

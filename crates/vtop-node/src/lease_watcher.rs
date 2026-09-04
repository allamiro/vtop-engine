//! Followers learn granted epochs without being restarted (#239).
//!
//! A follower validates every replica append against the epoch it serves. That
//! epoch used to come only from static configuration, and nothing on a
//! follower watched metadata — so the first grant whose epoch differed from
//! the followers' config fenced the leader out of its own quorum. The range
//! stopped, and the only way to restart it was to restart every follower with
//! a new config. That is a workable stopgap for a test harness and not a thing
//! anyone should have to do to a running cluster.
//!
//! # Observing, not competing
//!
//! This is deliberately NOT a lease agent. The agent on a leader acquires and
//! renews — it is trying to *hold* the range. A follower has no claim to make:
//! it reads what metadata already decided and moves its own epoch to match.
//! Giving followers an agent would put every replica in the election, which is
//! the opposite of what a follower is for.
//!
//! # Why adopting an epoch is safe
//!
//! Metadata is authoritative about who leads at which epoch, and the epoch is
//! strictly monotonic — a grant always mints `epoch + 1`, so the previous
//! holder is fenced by construction. A follower that adopts epoch `N` is
//! therefore agreeing to serve whoever metadata says holds `N`, and refusing
//! everyone else. Two properties keep that honest:
//!
//! * **Adoption only moves forward.** [`InProcessFollower::adopt_fencing_epoch`]
//!   is a `fetch_max`, so a stale read is a no-op rather than a rewind. A
//!   follower walked backward would start accepting appends from a leader
//!   metadata had already fenced — the split-brain write the epoch exists to
//!   prevent — and no polling loop can be trusted to deliver observations in
//!   order.
//! * **It fails closed while stale.** Until an observation lands, the follower
//!   still holds its previous epoch and rejects the new leader. Rejecting a
//!   legitimate leader for one poll interval costs latency; accepting a fenced
//!   one costs data.
//!
//! So the failure mode of a watcher that is slow, wedged, or entirely dead is
//! a follower that stops accepting appends — never one that accepts the wrong
//! ones.

use crate::lease_agent::LeasePublisher;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::replication::InProcessFollower;
use vtop_meta::AdminClient;
use vtop_observe::ReadinessGate;

/// Drives one follower's epoch from the metadata plane.
pub struct FollowerLeasePublisher {
    follower: Arc<InProcessFollower>,
}

impl FollowerLeasePublisher {
    pub fn new(follower: Arc<InProcessFollower>) -> Self {
        Self { follower }
    }
}

impl LeasePublisher for FollowerLeasePublisher {
    fn promote(&self, fencing_epoch: u64, _committed_offset: Option<u64>) {
        // "Promote" is the trait's word for the leader case; here it means
        // "serve this epoch". The committed offset is deliberately ignored: a
        // follower's boundary is whatever it has durably replicated, and
        // advancing it from a metadata read would claim durability for records
        // this replica may not hold.
        //
        // Both values must end up equal or the follower refuses every append.
        // Order is not a safety question — the check requires equality, so any
        // window between the two writes fails closed — but both must happen.
        self.follower.adopt_fencing_epoch(fencing_epoch);
        self.follower.meta_fencing_epoch().set(fencing_epoch);
    }

    fn demote(&self, fencing_epoch: u64) {
        // Only the metadata view is cleared. The held epoch stays where it is:
        // it records the newest epoch this follower has seen, and rewinding it
        // would let a later stale grant look current.
        self.follower
            .meta_fencing_epoch()
            .clear_lease(fencing_epoch);
    }

    fn suspend(&self, fencing_epoch: u64) {
        // NOT `clear_lease`: that records the epoch in `released_through`,
        // after which observing the same epoch again could never reactivate
        // the view. A suspend means "I lost sight of metadata", not "the lease
        // ended" — and the difference decides whether this follower can rejoin
        // its own leader without a restart.
        self.follower.meta_fencing_epoch().suspend(fencing_epoch);
    }
}

pub struct LeaseWatcherConfig {
    pub poll_interval: Duration,
    /// How long a single admin read may take before it is abandoned.
    ///
    /// Bounded for the same reason the agent bounds its calls: a metadata node
    /// that accepts a connection and then stops answering would otherwise wedge
    /// this loop forever, and a wedged watcher is a follower that never learns
    /// another epoch.
    pub request_timeout: Duration,
}

impl LeaseWatcherConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.poll_interval.is_zero() {
            return Err("lease watcher poll_interval must be non-zero".to_owned());
        }
        if self.request_timeout.is_zero() {
            return Err("lease watcher request_timeout must be non-zero".to_owned());
        }
        Ok(())
    }
}

/// Polls the committed range lease and publishes it into a follower.
pub struct LeaseWatcher {
    admin: AdminClient,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    config: LeaseWatcherConfig,
    publisher: Arc<dyn LeasePublisher>,
    /// Marked once the first observation lands.
    ///
    /// A follower that has not yet read metadata does not know which epoch it
    /// serves, and it fails closed — so it is not ready in the only sense the
    /// word can mean here. Reporting ready before that first read is what made
    /// a freshly started follower refuse its leader's opening appends and sit
    /// at offset 0 while the range moved on without it: a scenario would start
    /// a follower, see /readyz go green, produce, and only discover later that
    /// this replica had never joined.
    ready: Option<ReadinessGate>,
}

impl LeaseWatcher {
    pub fn new(
        admin: AdminClient,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        config: LeaseWatcherConfig,
        publisher: Arc<dyn LeasePublisher>,
        ready: Option<ReadinessGate>,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            admin,
            topic_uuid,
            range_uuid,
            config,
            publisher,
            ready,
        })
    }

    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        // The epoch this watcher has already published, so a steady state does
        // not re-publish the same grant every poll. Purely to keep the logs
        // and the atomics quiet — correctness does not depend on it, because
        // publishing is idempotent.
        let mut published: Option<u64> = None;
        // Consumed on the first observation of ANY outcome that tells us what
        // metadata thinks — a grant or a definite "no lease". Both are answers;
        // only an unreachable metadata plane leaves this follower ignorant.
        let mut ready = self.ready.clone();
        // A watch that CLOSED without ever publishing `true` is "no shutdown
        // will ever be requested", not "shutdown now" — the same rule the
        // oneshot adapter pins (#408). Tracked so the loop stops selecting on
        // a channel that would complete instantly forever.
        let mut watch_closed = false;
        loop {
            // Raced against shutdown, and the abandoned result never
            // publishes (#408): a SIGTERM landing mid-read used to wait out
            // the request timeout before the drain could proceed — on an
            // unresponsive metadata plane, the full five seconds — and a
            // watcher that is stopping has no business acting on whatever
            // that read eventually says.
            let observed = if watch_closed {
                self.observe().await
            } else {
                tokio::select! {
                    observed = self.observe() => observed,
                    changed = shutdown.changed() => {
                        // The VALUE decides before the closure does
                        // (review): a sender that published `true` and was
                        // then dropped is a shutdown whose channel closed,
                        // not a closure to park on.
                        if *shutdown.borrow() {
                            return;
                        }
                        if changed.is_err() {
                            watch_closed = true;
                            self.observe().await
                        } else {
                            continue;
                        }
                    }
                }
            };
            match observed {
                Ok(Some(epoch)) => {
                    if published != Some(epoch) {
                        tracing::info!(
                            range = %self.range_uuid,
                            fencing_epoch = epoch,
                            "follower adopting metadata's fencing epoch"
                        );
                    }
                    self.publisher.promote(epoch, None);
                    published = Some(epoch);
                    if let Some(gate) = ready.take() {
                        gate.mark_ready();
                    }
                }
                Ok(None) => {
                    // Metadata says no live lease. Clear the view so this
                    // follower refuses appends from a leader that no longer
                    // holds the range, but keep the held epoch: it is the
                    // floor a future grant must beat.
                    if let Some(epoch) = published.take() {
                        tracing::info!(
                            range = %self.range_uuid,
                            fencing_epoch = epoch,
                            "range lease released; follower is fenced until a new grant"
                        );
                        self.publisher.demote(epoch);
                    }
                    // A leaderless range is a real answer: this follower now
                    // knows it serves nothing, which is a state it can report.
                    // Withholding readiness here would wedge a range that is
                    // simply between leaders.
                    if let Some(gate) = ready.take() {
                        gate.mark_ready();
                    }
                }
                Err(error) => {
                    // Losing sight of metadata is NOT the same as the lease
                    // ending, and conflating them is what would make a
                    // transient admin outage need an operator. Suspend keeps
                    // the epoch reactivatable; `demote` would not.
                    tracing::warn!(
                        range = %self.range_uuid,
                        %error,
                        "lease watch round failed; follower fails closed until it can read again"
                    );
                    if let Some(epoch) = published {
                        self.publisher.suspend(epoch);
                    }
                }
            }
            if watch_closed {
                tokio::time::sleep(self.config.poll_interval).await;
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {}
                changed = shutdown.changed() => {
                    // A closed watch completes instantly forever; without
                    // the flag this select would never sleep again and the
                    // watcher would hot-poll metadata (#408).
                    if changed.is_err() {
                        watch_closed = true;
                    }
                }
            }
            if *shutdown.borrow() {
                // Nothing to release: a watcher only observes the lease. It
                // just stops observing (#280).
                return;
            }
        }
    }

    /// `Ok(Some(epoch))` when a live lease exists, `Ok(None)` when none does.
    async fn observe(&self) -> Result<Option<u64>, String> {
        let view = tokio::time::timeout(
            self.config.request_timeout,
            self.admin
                .read_range_lease(self.topic_uuid, self.range_uuid),
        )
        .await
        .map_err(|_| "read range lease timed out".to_owned())?
        .map_err(|error| error.to_string())?;

        if !view.found {
            // The range is not in metadata at all. Distinct from "exists with
            // no lease": this is a configuration fault, not a leaderless
            // range, and treating it as a release would quietly fence a
            // follower whose range uuid is simply wrong.
            return Err(format!("range {} is unknown to metadata", self.range_uuid));
        }
        Ok(view.lease.map(|lease| lease.fencing_epoch))
    }
}

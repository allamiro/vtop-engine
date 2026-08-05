//! The loop that makes range leadership live (#223).
//!
//! The metadata plane already knew how to grant, expire, and renew a range
//! lease, and the broker already refused any request whose fencing epoch did
//! not match its own. What was missing was the thing in between: a process that
//! actually asks for the lease, keeps it, notices when it has lost it, and
//! tells its broker. Until this existed, `MetaFencingEpoch` was a value nobody
//! ever published to — which is why #224's readiness probe shipped with a
//! caveat saying its fenced branch could not fire.
//!
//! # What the agent guarantees, and what it does not
//!
//! It guarantees that this process's broker only believes it holds epoch `E`
//! while metadata says so. It does **not** guarantee that exactly one process
//! is trying at a time — that is Raft's job, and acquisition is a single
//! linearizable proposal, so exactly one candidate can win a given round.
//!
//! Safety never rests on the timers here. Acquisition always mints
//! `fencing_epoch + 1`, so a slow, skewed, or paused agent that acquires late
//! still fences whoever held the range before it. The intervals below are a
//! liveness tuning knob: too long and failover is slow, too short and the
//! metadata group carries needless proposals. Neither setting can produce two
//! brokers that both believe they may write.
//!
//! # Why renewal failure is not retried in place
//!
//! When a renewal is refused the agent does not try again with a fresh epoch —
//! it publishes the loss to the broker first and only then re-enters the
//! acquisition path. A broker that keeps serving during that window is a broker
//! serving under an epoch metadata has already given away.
//!
//! # The local deadline
//!
//! An unreachable metadata plane is not proof the lease is gone — but past the
//! deadline of the last lease metadata actually confirmed, it is no longer
//! proof of anything, and a rival may already have been granted the range. The
//! agent therefore tracks that deadline locally and demotes the broker once it
//! passes without a successful renewal, instead of relying on a future read to
//! deliver the news. The tracked value is always at or before the deadline
//! metadata recorded (it is taken from the read view, or stamped before a
//! proposal is sent), so the local demotion always precedes the earliest
//! instant a rival could legally acquire. Administrative grants have no
//! deadline and are exempt: an operator's pin outlives any partition.

use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::LocalBroker;
use vtop_meta::command::CommandEnvelope;
use vtop_meta::{AdminClient, MetadataCommand, MetadataResponse};

/// Where the agent publishes what metadata decided.
///
/// A trait rather than a direct `LocalBroker` handle so the transitions that
/// matter — promotion, and the demotion a partitioned node discovers — can be
/// asserted without standing up a broker, a segment, and a disk.
pub trait LeasePublisher: Send + Sync {
    /// This node now holds the range at `fencing_epoch`.
    fn promote(&self, fencing_epoch: u64);
    /// This node no longer holds the range at `fencing_epoch`.
    fn demote(&self, fencing_epoch: u64);
}

/// Publishes into a live broker.
pub struct BrokerLeasePublisher {
    broker: Arc<LocalBroker>,
}

impl BrokerLeasePublisher {
    pub fn new(broker: Arc<LocalBroker>) -> Self {
        Self { broker }
    }
}

impl LeasePublisher for BrokerLeasePublisher {
    fn promote(&self, fencing_epoch: u64) {
        // Both values must end up equal or the broker refuses every request.
        // The order is not a safety question — produce checks equality, so any
        // window between the two writes fails closed — but both must happen.
        self.broker.adopt_fencing_epoch(fencing_epoch);
        self.broker.meta_fencing_epoch().set(fencing_epoch);
    }

    fn demote(&self, fencing_epoch: u64) {
        // Only the metadata view is cleared. The held epoch stays where it is:
        // it records what this process was last granted, and rewinding it
        // would let a later stale grant look current.
        self.broker.meta_fencing_epoch().clear_lease(fencing_epoch);
    }
}

/// How the agent paces itself.
#[derive(Clone, Copy, Debug)]
pub struct LeaseAgentConfig {
    /// Lease length requested on every acquire and renew.
    pub lease_duration: Duration,
    /// How often a holder renews. Must be comfortably shorter than
    /// `lease_duration`, or a single lost round trip costs the range.
    pub renew_interval: Duration,
    /// How often a non-holder re-checks whether the lease has lapsed.
    pub poll_interval: Duration,
}

impl Default for LeaseAgentConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(15),
            // A third of the lease: two consecutive renewals can fail — a
            // metadata leader election, say — and the range still does not
            // change hands. At half, one hiccup is a failover.
            renew_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(2),
        }
    }
}

impl LeaseAgentConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.lease_duration.is_zero() {
            return Err("lease_duration must be greater than zero".into());
        }
        if self.renew_interval >= self.lease_duration {
            return Err(format!(
                "renew_interval ({:?}) must be shorter than lease_duration ({:?}), or the \
                 lease expires before its holder renews it",
                self.renew_interval, self.lease_duration
            ));
        }
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be greater than zero".into());
        }
        Ok(())
    }
}

/// What the agent believes about this node's hold on the range.
///
/// Deliberately not exposed: the broker's fencing view is the authoritative
/// signal — it is what produce checks and what `/readyz` reports — and a second
/// public source of truth would only invite the two to diverge. This exists so
/// the agent can tell a transition from a repeat and log accordingly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseState {
    /// This node holds the range at the given epoch.
    Held { fencing_epoch: u64 },
    /// Someone else holds it, or nobody does.
    NotHeld,
}

/// One round of the agent's decision, separated from the I/O so it can be
/// tested without a metadata cluster.
///
/// The agent is a state machine over (what metadata says, what we believed).
/// Keeping that decision pure is what makes the interesting transitions —
/// losing a lease to a rival, discovering a stale epoch — testable at all;
/// they are exactly the cases that are hardest to stage against a live cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseDecision {
    /// Metadata agrees we hold it and the lease is still live; renew before
    /// the deadline.
    Renew { fencing_epoch: u64 },
    /// Metadata pinned this node administratively (no deadline). There is
    /// nothing to renew — the state machine refuses renewing a deadline-less
    /// lease — so publish it as held and check back later.
    HeldAdministratively { fencing_epoch: u64 },
    /// Nobody holds a live lease (including our own lapsed one); try to take
    /// it. A lapsed lease cannot be renewed back to life — the only way back
    /// is acquisition, which mints a fresh epoch.
    Acquire { expected_range_generation: u64 },
    /// Someone else holds a live lease at this epoch; wait.
    Wait { fencing_epoch: u64 },
    /// The range does not exist. Waiting is right: a range is created by an
    /// operator, and spinning on acquisition would bury that in noise.
    RangeMissing,
}

/// Decide what to do from a lease view and this node's identity.
///
/// `now_ms` is the caller's clock and is used only to judge liveness of
/// someone else's lease. Being wrong here costs a needless acquisition attempt
/// that the metadata group will adjudicate — never correctness, because the
/// epoch mint fences regardless.
pub fn decide(
    view: &vtop_meta::AdminReadRangeLeaseResponse,
    this_node: Uuid,
    now_ms: i64,
) -> LeaseDecision {
    if !view.found {
        return LeaseDecision::RangeMissing;
    }
    match view.lease.as_ref() {
        None => LeaseDecision::Acquire {
            expected_range_generation: view.range_generation,
        },
        Some(lease) if lease.holder_node_uuid == this_node => {
            match lease.expires_at_ms {
                // An operator pinned this node permanently. Renewing would be
                // refused by the state machine — and demoting on that refusal
                // would oscillate a holder metadata says owns the range.
                None => LeaseDecision::HeldAdministratively {
                    fencing_epoch: lease.fencing_epoch,
                },
                // Ours and live: renew it. Note this is keyed on the epoch
                // metadata reports, not the one we remember, so an agent whose
                // broker has drifted re-synchronises rather than renewing a
                // fiction.
                Some(deadline) if now_ms < deadline => LeaseDecision::Renew {
                    fencing_epoch: lease.fencing_epoch,
                },
                // Ours but lapsed. Returning Renew here would loop forever:
                // renew → refused (expiry is final) → demote → read → still
                // nominally ours → renew again. Re-enter acquisition instead,
                // which mints a fresh epoch.
                Some(_) => LeaseDecision::Acquire {
                    expected_range_generation: view.range_generation,
                },
            }
        }
        Some(lease) => {
            let live = match lease.expires_at_ms {
                // An administrative grant never lapses. Waiting forever is
                // correct: an operator pinned this holder, and an election
                // must not quietly undo that.
                None => true,
                Some(deadline) => now_ms < deadline,
            };
            if live {
                LeaseDecision::Wait {
                    fencing_epoch: lease.fencing_epoch,
                }
            } else {
                LeaseDecision::Acquire {
                    expected_range_generation: view.range_generation,
                }
            }
        }
    }
}

/// Drives one range's lease against the metadata group.
pub struct LeaseAgent {
    admin: AdminClient,
    config: LeaseAgentConfig,
    node_uuid: Uuid,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    /// Where transitions are published.
    publisher: Arc<dyn LeasePublisher>,
    state: LeaseState,
    /// Local upper bound on how long the current hold may be trusted without
    /// hearing from metadata, in the same wall-clock the envelope carries.
    ///
    /// Always at or before the deadline metadata recorded: it is taken from
    /// the lease view, or computed from a timestamp captured BEFORE the
    /// renewal/acquisition proposal — the state machine mints its deadline
    /// later than that. `None` while not held, or held administratively
    /// (which cannot lapse).
    held_until_ms: Option<i64>,
}

/// Whether a holder that cannot reach the metadata plane must stop serving.
///
/// Without this, a broker partitioned from metadata after promotion would
/// keep accepting writes forever on its process-local epoch while a rival
/// legitimately acquires the range after the recorded deadline. The epoch
/// still guarantees no torn history — this is about not accepting writes we
/// already know a rival may be authorized to supersede.
fn must_demote_locally(state: LeaseState, held_until_ms: Option<i64>, now_ms: i64) -> bool {
    matches!(state, LeaseState::Held { .. })
        && held_until_ms.is_some_and(|deadline| now_ms >= deadline)
}

impl LeaseAgent {
    pub fn new(
        admin: AdminClient,
        config: LeaseAgentConfig,
        node_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        publisher: Arc<dyn LeasePublisher>,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            admin,
            config,
            node_uuid,
            topic_uuid,
            range_uuid,
            publisher,
            state: LeaseState::NotHeld,
            held_until_ms: None,
        })
    }

    /// Run until the process stops. Never returns on success: losing a lease
    /// is a state transition, not a reason to exit — the node stays up so it
    /// can be inspected and can win the range back.
    pub async fn run(mut self) -> ! {
        loop {
            let delay = match self.step().await {
                Ok(delay) => delay,
                Err(error) => {
                    // A metadata group mid-election refuses reads. Retrying is
                    // right; what would be wrong is treating an unreachable
                    // metadata plane as proof that we still hold the lease.
                    // The broker keeps serving only until the deadline of the
                    // last lease metadata actually confirmed — past that, a
                    // rival may already have been granted the range, so the
                    // broker is demoted locally rather than trusting a future
                    // read to deliver the news.
                    tracing::warn!(
                        %error,
                        range = %self.range_uuid,
                        "lease agent round failed; retrying"
                    );
                    if must_demote_locally(self.state, self.held_until_ms, now_ms()) {
                        if let LeaseState::Held { fencing_epoch } = self.state {
                            tracing::warn!(
                                range = %self.range_uuid,
                                fencing_epoch,
                                "metadata unreachable past the lease deadline; demoting locally"
                            );
                            self.publish_lost(fencing_epoch);
                        }
                    }
                    self.config.poll_interval
                }
            };
            tokio::time::sleep(delay).await;
        }
    }

    /// Every admin round trip is bounded by this. A blackholed endpoint (as
    /// opposed to a refused connection) would otherwise leave the await
    /// pending indefinitely — and the local-deadline demotion only runs
    /// between rounds, so an unbounded await is a partitioned holder that
    /// stays promoted past the instant a rival may legally own the range.
    /// The renew interval is the natural bound: an answer that arrives later
    /// than the next renewal was due is an answer the pacing cannot use.
    fn rpc_deadline(&self) -> Duration {
        self.config.renew_interval
    }

    async fn bounded<T, E: std::fmt::Display>(
        &self,
        what: &str,
        call: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, String> {
        match tokio::time::timeout(self.rpc_deadline(), call).await {
            Ok(result) => result.map_err(|error| format!("{what}: {error}")),
            Err(_) => Err(format!(
                "{what}: no answer within {:?}",
                self.rpc_deadline()
            )),
        }
    }

    /// One round. Returns how long to wait before the next.
    async fn step(&mut self) -> Result<Duration, String> {
        // Captured BEFORE any proposal: the state machine mints its deadline
        // later than this, so a local deadline derived from it is always at
        // or before the one metadata records.
        let round_started_ms = now_ms();
        let view = self
            .bounded(
                "read range lease",
                self.admin
                    .read_range_lease(self.topic_uuid, self.range_uuid),
            )
            .await?;
        let decision = decide(&view, self.node_uuid, now_ms());
        match decision {
            LeaseDecision::Renew { fencing_epoch } => {
                // Trust exactly what metadata reported until the renewal
                // lands; a renewal that errors out must not slide the local
                // deadline forward on the strength of a read alone.
                self.publish_held(
                    fencing_epoch,
                    view.lease.as_ref().and_then(|lease| lease.expires_at_ms),
                );
                match self.renew(fencing_epoch).await? {
                    true => {
                        let renewed_until = round_started_ms
                            .saturating_add_unsigned(duration_ms(self.config.lease_duration)?);
                        self.held_until_ms = Some(
                            self.held_until_ms
                                .map_or(renewed_until, |current| current.max(renewed_until)),
                        );
                        Ok(self.config.renew_interval)
                    }
                    false => {
                        // Refused: metadata has moved on. Publish the loss
                        // BEFORE trying to win it back, so there is no window
                        // in which the broker serves under an epoch metadata
                        // has already given away.
                        self.publish_lost(fencing_epoch);
                        Ok(self.config.poll_interval)
                    }
                }
            }
            LeaseDecision::HeldAdministratively { fencing_epoch } => {
                // No deadline to track: an administrative lease cannot lapse,
                // so a metadata partition never forces a local demotion.
                self.publish_held(fencing_epoch, None);
                Ok(self.config.renew_interval)
            }
            LeaseDecision::Acquire {
                expected_range_generation,
            } => {
                // Metadata no longer guarantees us the range — no lease, an
                // expired rival, or our own lapsed one. If the broker was
                // serving, stop it BEFORE campaigning: a rival may win the
                // acquisition race the moment we enter it.
                if let LeaseState::Held { fencing_epoch } = self.state {
                    self.publish_lost(fencing_epoch);
                }
                match self.acquire(expected_range_generation).await? {
                    Some(fencing_epoch) => {
                        self.publish_held(
                            fencing_epoch,
                            Some(
                                round_started_ms.saturating_add_unsigned(duration_ms(
                                    self.config.lease_duration,
                                )?),
                            ),
                        );
                        Ok(self.config.renew_interval)
                    }
                    // Lost the race, or our CAS token was stale. Either way the
                    // next read tells us the truth; guessing would not.
                    None => Ok(self.config.poll_interval),
                }
            }
            LeaseDecision::Wait { fencing_epoch } => {
                if let LeaseState::Held {
                    fencing_epoch: ours,
                } = self.state
                {
                    // We thought we held it and metadata disagrees. This is
                    // the transition that matters most: it is how a node that
                    // was partitioned learns it has been replaced.
                    self.publish_lost(ours);
                }
                // Fence the local view up to the rival's epoch even when we
                // never believed we held the range: at startup the broker may
                // still carry a configured epoch metadata has already
                // superseded, and it must not keep serving on it. Demotion is
                // monotonic and idempotent, so repeating it every poll is
                // free.
                self.publisher.demote(fencing_epoch);
                Ok(self.config.poll_interval)
            }
            LeaseDecision::RangeMissing => {
                if let LeaseState::Held { fencing_epoch } = self.state {
                    // A range deleted out from under its holder is an operator
                    // action; the broker must not keep serving it.
                    self.publish_lost(fencing_epoch);
                }
                Ok(self.config.poll_interval)
            }
        }
    }

    async fn acquire(&self, expected_range_generation: u64) -> Result<Option<u64>, String> {
        let response = self
            .bounded(
                "propose acquire",
                self.admin.propose(MetadataCommand::AcquireRangeLease {
                    env: envelope(),
                    topic_uuid: self.topic_uuid,
                    range_uuid: self.range_uuid,
                    holder_node_uuid: self.node_uuid,
                    expected_range_generation,
                    lease_duration_ms: duration_ms(self.config.lease_duration)?,
                }),
            )
            .await?;
        Ok(match response.response {
            MetadataResponse::LeaseGranted { fencing_epoch } => Some(fencing_epoch),
            // A rejection here is ordinary: a rival won, or the generation
            // moved. It is not an error to log loudly.
            _ => None,
        })
    }

    async fn renew(&self, fencing_epoch: u64) -> Result<bool, String> {
        let response = self
            .bounded(
                "propose renew",
                self.admin.propose(MetadataCommand::RenewRangeLease {
                    env: envelope(),
                    topic_uuid: self.topic_uuid,
                    range_uuid: self.range_uuid,
                    holder_node_uuid: self.node_uuid,
                    expected_fencing_epoch: fencing_epoch,
                    lease_duration_ms: duration_ms(self.config.lease_duration)?,
                }),
            )
            .await?;
        Ok(matches!(
            response.response,
            MetadataResponse::LeaseGranted { .. }
        ))
    }

    fn publish_held(&mut self, fencing_epoch: u64, held_until_ms: Option<i64>) {
        if self.state != (LeaseState::Held { fencing_epoch }) {
            tracing::info!(
                range = %self.range_uuid,
                fencing_epoch,
                "range lease held"
            );
        }
        self.publisher.promote(fencing_epoch);
        self.state = LeaseState::Held { fencing_epoch };
        self.held_until_ms = held_until_ms;
    }

    fn publish_lost(&mut self, fencing_epoch: u64) {
        tracing::warn!(
            range = %self.range_uuid,
            fencing_epoch,
            "range lease lost; the broker will refuse writes under this epoch"
        );
        self.publisher.demote(fencing_epoch);
        self.state = LeaseState::NotHeld;
        self.held_until_ms = None;
    }
}

fn envelope() -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::new_v4(),
        issued_at_ms: now_ms(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn duration_ms(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis())
        .map_err(|_| format!("lease duration {duration:?} does not fit the wire format"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_meta::{AdminLeaseView, AdminReadRangeLeaseResponse};

    const US: Uuid = Uuid::from_u128(1);
    const THEM: Uuid = Uuid::from_u128(2);

    fn view(lease: Option<AdminLeaseView>) -> AdminReadRangeLeaseResponse {
        AdminReadRangeLeaseResponse {
            found: true,
            range_generation: 7,
            fencing_epoch: lease.map(|l| l.fencing_epoch).unwrap_or(0),
            lease,
            read_at_applied_index: 42,
        }
    }

    fn held_by(holder: Uuid, expires_at_ms: Option<i64>) -> Option<AdminLeaseView> {
        Some(AdminLeaseView {
            holder_node_uuid: holder,
            fencing_epoch: 4,
            expires_at_ms,
        })
    }

    #[test]
    fn an_unleased_range_is_acquired() {
        assert_eq!(
            decide(&view(None), US, 1_000),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    #[test]
    fn our_own_lease_is_renewed_at_the_epoch_metadata_reports() {
        // Keyed on metadata's epoch, not one we remember, so an agent whose
        // broker drifted re-synchronises instead of renewing a fiction.
        assert_eq!(
            decide(&view(held_by(US, Some(9_000))), US, 1_000),
            LeaseDecision::Renew { fencing_epoch: 4 }
        );
    }

    #[test]
    fn a_live_rival_lease_is_waited_out() {
        assert_eq!(
            decide(&view(held_by(THEM, Some(9_000))), US, 1_000),
            LeaseDecision::Wait { fencing_epoch: 4 }
        );
    }

    /// Our own administrative grant is held, not renewed: the state machine
    /// refuses renewing a deadline-less lease, and demoting on that refusal
    /// every round would oscillate a holder metadata says owns the range.
    #[test]
    fn our_own_administrative_lease_is_held_without_renewing() {
        assert_eq!(
            decide(&view(held_by(US, None)), US, i64::MAX),
            LeaseDecision::HeldAdministratively { fencing_epoch: 4 }
        );
    }

    /// Our own lapsed lease is re-acquired, never renewed: expiry is final in
    /// the state machine, so returning Renew would loop forever — renew,
    /// refused, demote, read, still nominally ours, renew — and the range
    /// would stay unservable.
    #[test]
    fn our_own_lapsed_lease_is_reacquired_not_renewed() {
        assert_eq!(
            decide(&view(held_by(US, Some(9_000))), US, 9_000),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    /// The liveness property the whole issue exists for: once the incumbent
    /// stops renewing, a rival takes over.
    #[test]
    fn an_expired_rival_lease_is_acquired() {
        assert_eq!(
            decide(&view(held_by(THEM, Some(9_000))), US, 9_001),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    /// An administrative grant is an operator's explicit choice and must never
    /// be taken by an election, however long the agent waits.
    #[test]
    fn an_administrative_rival_lease_is_never_taken() {
        assert_eq!(
            decide(&view(held_by(THEM, None)), US, i64::MAX),
            LeaseDecision::Wait { fencing_epoch: 4 }
        );
    }

    /// The fail-stop half of the lease contract: a holder that cannot reach
    /// metadata must stop serving once the last confirmed deadline passes,
    /// because past it a rival may legally hold the range.
    #[test]
    fn a_partitioned_holder_demotes_itself_once_its_deadline_passes() {
        let held = LeaseState::Held { fencing_epoch: 4 };
        assert!(must_demote_locally(held, Some(9_000), 9_000));
        assert!(
            !must_demote_locally(held, Some(9_000), 8_999),
            "before the deadline the lease is still metadata's word"
        );
        assert!(
            !must_demote_locally(held, None, i64::MAX),
            "an administrative hold has no deadline to outlive"
        );
        assert!(
            !must_demote_locally(LeaseState::NotHeld, Some(9_000), i64::MAX),
            "a non-holder has nothing to demote"
        );
    }

    /// A range nobody created is not a range to campaign for; spinning on
    /// acquisition would bury the real problem in noise.
    #[test]
    fn a_missing_range_is_not_campaigned_for() {
        let mut missing = view(None);
        missing.found = false;
        assert_eq!(decide(&missing, US, 1_000), LeaseDecision::RangeMissing);
    }

    /// Promotion must leave the broker's two epoch views EQUAL. Produce
    /// requires that equality, so a promotion that moved only one of them
    /// would wedge the range while looking like a successful failover.
    #[test]
    fn promotion_leaves_the_broker_able_to_serve_the_new_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(5);
        assert_eq!(broker.held_fencing_epoch(), 5);
        let (epoch, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert_eq!(epoch, 5);
        assert!(live, "a promoted broker must hold a live lease");
    }

    /// A stale promotion arriving late must not rewind the broker onto an
    /// epoch metadata has already superseded.
    #[test]
    fn a_stale_promotion_cannot_rewind_the_held_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(7);
        publisher.promote(5);
        assert_eq!(
            broker.held_fencing_epoch(),
            7,
            "adoption is monotonic; a reordered round must not undo a newer grant"
        );
    }

    /// Demotion is what a partitioned node discovers. The broker must stop
    /// believing it may write, without rewinding what it was last granted.
    #[test]
    fn demotion_stops_the_broker_serving_but_keeps_the_granted_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(5);
        publisher.demote(5);
        let (_, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert!(!live, "a demoted broker must not hold a live lease");
        assert_eq!(
            broker.held_fencing_epoch(),
            5,
            "the granted epoch records history; rewinding it would let a later \
             stale grant look current"
        );
    }

    fn test_broker(dir: &std::path::Path, epoch: u64) -> LocalBroker {
        let range = vtop_protocol::RangeIdentity {
            topic: "telemetry".into(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(7),
            range_generation: 0,
        };
        let descriptor = vtop_log::SegmentDescriptor {
            segment_id: Uuid::from_u128(9),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: vtop_log::RangeLineage {
                range_id: range.range_id,
                generation: range.range_generation,
                key_range: vtop_log::KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = vtop_log::ActiveSegment::create(
            dir.join("range.active"),
            descriptor,
            vtop_log::SegmentConfig::default(),
        )
        .unwrap();
        let epochs = vtop_broker::ProducerEpochJournal::open(dir.join("epochs")).unwrap();
        LocalBroker::new(segment, epochs, range, epoch).unwrap()
    }

    #[test]
    fn a_renew_interval_at_or_past_the_lease_is_rejected() {
        let config = LeaseAgentConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_secs(10),
            poll_interval: Duration::from_secs(1),
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("must be shorter than"),
            "a renew interval that never beats the deadline loses the range every \
             time: {error}"
        );
    }

    #[test]
    fn the_default_pacing_leaves_room_for_a_failed_renewal() {
        let config = LeaseAgentConfig::default();
        config.validate().unwrap();
        assert!(
            config.renew_interval * 2 < config.lease_duration,
            "two consecutive renewals must be able to fail without losing the range"
        );
    }
}

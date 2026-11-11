//! Verified promotion: what a new range leader must establish before it serves
//! (#223).
//!
//! Winning the lease is an act of the metadata plane. It says who *may* lead;
//! it says nothing about what the range actually contains. A leader that starts
//! serving on that basis alone is guessing at its own high-water mark, and the
//! two ways of guessing are both wrong:
//!
//! * Guess too **low** — serve below the real committed boundary — and fetch
//!   hides records that were acknowledged to a producer. Acknowledged data
//!   appearing to vanish is the failure this whole system exists to prevent.
//! * Guess too **high** — assume the previous leader's local tail was
//!   committed — and expose records that never reached a quorum. Those records
//!   can still be lost, so exposing them turns "durable once acknowledged" into
//!   a coin flip.
//!
//! So promotion is a *read* before it is a right to write: ask a quorum of
//! replicas where their disks are, and take the boundary a quorum can prove.
//!
//! # Known limitations, stated plainly
//!
//! This establishes a floor; it is not yet a complete recovery protocol. What
//! is closed and what is not, so nobody reads more safety into it than is here:
//!
//! **Closed: the probe no longer reads a moving target.** Every replica is
//! fenced and read in ONE round trip (#240), which is what BookKeeper's ledger
//! recovery does before it reads an ensemble. A replica that could not be
//! fenced reports absent rather than its last known offset, so it does not
//! count toward the quorum — an offset now either comes from a log that has
//! been stopped, or it does not come at all. A replica whose own metadata view
//! has not yet caught up to the grant refuses the fence, correctly, since it is
//! not fenced until it has; the candidate retries, or promotes on the replicas
//! it did fence.
//!
//! **Closed: replicas can say which epoch wrote which stretch of their log.**
//! The KIP-101 vector is durable per replica and travels on the fence reply.
//!
//! **Closed (#577): promotion USES that vector.** Every [`ReplicaProbe`]
//! carries a lineage verdict beside its offset ([`fenced_lineage`]): the
//! candidate compares the history it sent with the one the fenced replica
//! returned, and an answer is `Agreed`, `Reconciled` (the replica discarded a
//! diverged suffix first) or `Unknown` (no history to compare on one side: a
//! replica with records but no journal, a broken journal, a candidate that
//! cannot vouch for its own). [`establish`] counts only the first two. An
//! Unknown answer's offset is a count of records whatever wrote them, and
//! counting it is the one way left for a replica holding another leadership's
//! log to complete a majority, vote for a candidate missing acknowledged
//! records, or later ack a marker over records it never held - reconciliation
//! touched nothing on it, so the ack-on-divergence hole (#261) is open there.
//! The verdicts are recorded with the quorum (feature level 6), so the
//! replica-set gate and chain audits count the same answers this did.
//!
//! The rule is strict on purpose: Unknown answers never count, not even when
//! no provable answer exists. Falling back to offsets "when nothing better is
//! available" is a fence that fails open exactly when it is needed. A replica
//! that cannot prove lineage is repaired from one that can (`vtopctl node
//! repair` installs the source's history, #315), not promoted on a guess.
//! That costs liveness, and the costs are stated rather than hidden (#612):
//! a range that holds records but where NO replica has epoch history
//! (records that predate the journal everywhere, journals lost everywhere)
//! cannot promote at all, and repair
//! cannot fix it, because a history-less source has none to install. A
//! dispute confined to records retention reclaimed (#290/#408) also leaves
//! the replica's answer Unknown, because the candidate cannot see the
//! replica's retained base.
//!
//! **Closed (#611): the fence discards only what is proven unacknowledged.**
//! A replica ahead of the candidate keeps its records through the fence and
//! refuses the vote. It used to compare the candidate's tail adoption as a
//! divergence and truncate down to the candidate first. See the comment above
//! the vote restriction in [`establish`].
//!
//! **Open: an absent replica is not truncated by an election.** A replica
//! that was not fenced and holds uncommitted records above the established
//! boundary keeps them. This code correctly refuses to *expose* them, but
//! if that replica later wins the lease they resurface, and the catch-up
//! path can ack the new leader's records over them as duplicates (#261). A
//! replica that WAS fenced and outvoted discards them on the new leader's
//! first append over them (#611), unless it restarted in between.
//!
//! **Closed, from Raft §5.4.2 (#240):** a new leader must not commit entries
//! from a previous term by counting replicas. The boundary this module
//! establishes is therefore a CANDIDATE value, never published as such: the
//! broker publisher appends a marker record of the new epoch at its tail
//! (`LocalBroker::publish_boundary_marker`, a reserved producer identity the
//! consumer boundary filters), a driver proves it on a quorum — retrying for
//! as long as a majority cannot hold the tail — and the published high-water
//! mark is the marker's end, which covers the candidate because the election
//! restriction guarantees the winner holds everything the boundary names.
//! Until the marker is proven the watermark stays at the durable floor the
//! broker opened with, and a produce that lands first commits the marker
//! prefix implicitly, which is Raft's own rule. The invariant this buys: **a
//! published high-water mark never regresses across a failover.** Standalone
//! and v1-format ranges are not gated — the first has no quorum to convince,
//! the second no marker its consumers could be shielded from.
//!
//! What is here is the floor computation, a fenced read to compute it from, and
//! an honest refusal when a quorum cannot be reached; the publication lives
//! with the broker publisher in `lease_agent`.
//!
//! # Why the quorum floor, and not the maximum
//!
//! The committed offset is the highest offset a MAJORITY has durably stored.
//! Taking the maximum any single replica reports would count a replica that
//! received an append the old leader never managed to acknowledge. Taking the
//! minimum would discard offsets a quorum genuinely holds, stalling the range
//! behind its slowest member. The k-th largest value, where k is the majority
//! size, is exactly the boundary a quorum can vouch for.
//!
//! This is the same arithmetic the replication path already uses to advance the
//! watermark during steady-state produce; promotion applies it once, from a
//! standing start, to state written by someone else. Two further gates apply:
//! the candidate must itself hold the boundary the quorum proved
//! ([`Promotion::LeaderBehind`]) — a leader behind the boundary would publish
//! a high-water mark covering offsets its own log does not contain, and the
//! produce fast path would then acknowledge fresh writes into them — and a
//! majority of the fenced replicas must be at or below the candidate's own
//! offset ([`Promotion::CandidateBehindVoters`]), which is Raft's election
//! restriction (§5.4.1): the floor alone can sit below a record acknowledged
//! on a quorum whose survivors straddle the candidate, and a candidate that
//! majority would refuse the vote used to promote anyway and let
//! reconciliation truncate the acknowledged record away (#240).
//!
//! # Why an inherited watermark is never lowered
//!
//! Publishing an established boundary uses the watermark's monotonic
//! `advance_to`, so a re-promoted broker whose in-memory watermark is already
//! ABOVE the newly proven boundary keeps the higher value. That is deliberate.
//! Within one process lifetime the watermark only ever advanced by quorum
//! acknowledgement (steady state) or quorum proof (promotion), so everything
//! below it was committed — and commitment is permanent. Rewinding it would
//! hide records acknowledged to producers, the exact failure this module
//! exists to prevent; a promotion probe that reaches a *different* majority
//! (say, the slow follower after the fast one dropped out) proves a lower
//! floor, it does not disprove the higher one. The case where the same offset
//! names different records on different replicas is the epoch-qualification
//! gap above, and no watermark arithmetic can paper over it — it needs
//! KIP-101-style truncation, tracked with the rest of the recovery arc.

use std::collections::BTreeMap;
use uuid::Uuid;
use vtop_broker::fencing_epochs::{compare_lineages, EpochStart, Lineage};
use vtop_meta::AnswerLineage;

/// What one replica reported, or that it did not answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaProbe {
    pub node_id: Uuid,
    /// `None` when the replica could not be reached or refused.
    pub local_committed_offset: Option<u64>,
    /// Whether the offset describes the candidate's history (#577). Only an
    /// answer whose lineage was proven counts toward the quorum. Meaningless
    /// for an absent replica, which counts for nothing anyway.
    pub lineage: AnswerLineage,
}

/// The lineage verdict on one fenced replica's answer (#577), judged by the
/// candidate from what it sent and what came back.
///
/// `candidate` is the epoch history the candidate sent with the fence;
/// `answered` is the history the replica returned AFTER reconciling against
/// it, `answered_tail` the replica's next offset, and `truncated_records`
/// what reconciliation discarded.
///
/// Judged by the same comparison the replica's own reconciliation uses
/// ([`compare_lineages`]), and by the same rule for what counts as disputed:
/// a divergence at or above the replica's tail concerns records it does not
/// hold - typically just each side's adoption of the new epoch at its own
/// tail - so every record it holds agrees. A divergence BELOW its tail that
/// survived reconciliation is one the replica declined to act on (a dispute
/// confined to records retention reclaimed, #290/#408): its offset is not
/// proven to describe the candidate's history, so the verdict is Unknown,
/// as it is when either side has no history to compare.
///
/// A replica that holds NO records (`answered_tail` 0) disputes nothing: its
/// offset of 0 is a prefix of every history, so it is Agreed whatever either
/// vector says. That is the normal shape of a fresh range, where a lease-led
/// broker has adopted no epoch yet and so sends an empty vector (live-chaos
/// scenarios 09 and 11-13 caught a first draft that refused it).
///
/// The mirror rule holds for the candidate's own tail (#611):
/// `candidate_tail` is where the candidate's log ENDS, every record it holds
/// whether durable or not (#616 review: its durable vote can sit below
/// records it holds, and a dispute about those is real). A divergence at or above it
/// concerns records only the REPLICA holds. Typically that is the
/// candidate's adoption of the new epoch at its tail, against records the
/// candidate never received. The replica agrees with every record the
/// candidate holds, keeps the rest (the fence no longer discards records
/// the candidate cannot prove unacknowledged), and answers above the
/// candidate. Counting that answer is what lets the vote restriction refuse
/// a candidate that lacks an acknowledged record. Leaving it Unknown would
/// turn the refusal into a quorum miss and name nobody better.
pub fn fenced_lineage(
    candidate: &[EpochStart],
    answered: &[EpochStart],
    answered_tail: u64,
    truncated_records: u64,
    candidate_tail: u64,
) -> AnswerLineage {
    if answered_tail == 0 {
        return if truncated_records > 0 {
            AnswerLineage::Reconciled { truncated_records }
        } else {
            AnswerLineage::Agreed
        };
    }
    match compare_lineages(candidate, answered) {
        Lineage::Unknown => AnswerLineage::Unknown,
        Lineage::DivergesAt(offset) if offset < answered_tail && offset < candidate_tail => {
            AnswerLineage::Unknown
        }
        Lineage::Agreed | Lineage::DivergesAt(_) if truncated_records > 0 => {
            AnswerLineage::Reconciled { truncated_records }
        }
        Lineage::Agreed | Lineage::DivergesAt(_) => AnswerLineage::Agreed,
    }
}

/// The outcome of asking a replica set where it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Promotion {
    /// A quorum answered; this is the boundary they can prove.
    Established {
        committed_offset: u64,
        /// Replicas that answered with a proven lineage - the ones counted -
        /// for the operator record.
        answered: BTreeMap<Uuid, u64>,
        /// Replicas that answered but whose lineage could not be proven
        /// (#577): recorded, never counted.
        unproven: BTreeMap<Uuid, u64>,
    },
    /// Too few replicas answered with a proven lineage to establish anything.
    ///
    /// Refusing to serve is the only safe response. A leader that promoted
    /// anyway would be publishing a high-water mark no quorum had confirmed,
    /// which is precisely the guess this exists to prevent.
    QuorumUnavailable {
        /// Answers that count: fenced, with a proven lineage.
        answered: usize,
        required: usize,
        /// Answers that were fenced but whose lineage was Unknown (#577) -
        /// the ones that would have completed the quorum by offset alone.
        unproven: usize,
    },
    /// A quorum proved a boundary this leader's own disk does not reach.
    ///
    /// Serving here would be worse than a guess. Publishing the quorum's
    /// boundary as the high-water mark makes the produce fast path treat any
    /// append below it as already committed — so a leader whose log ends at
    /// 50 under a proven boundary of 90 would acknowledge fresh writes into
    /// offsets 50..90 that are occupied by committed records it never had.
    /// Refusing keeps the lease unrenewed; it lapses, and a caught-up replica
    /// can win the range instead. (Catching the leader up in place is the
    /// recovery-protocol arc tracked in the module docs.)
    LeaderBehind {
        committed_offset: u64,
        /// The leader's own reported boundary; `None` if its probe was absent,
        /// which is refused for the same reason.
        leader_committed_offset: Option<u64>,
    },
    /// The candidate holds the proven floor, but fewer than a majority of the
    /// fenced replicas are at or below its own offset — Raft's election
    /// restriction (§5.4.1), in its per-voter form.
    ///
    /// The floor alone is not enough: the k-th largest can sit BELOW a record
    /// that was acknowledged on a quorum whose survivors now straddle the
    /// candidate — a candidate at 100 counting a fenced replica at 101 passes
    /// the floor check (the floor computes to 100) and would then publish a
    /// boundary under which reconciliation truncates the acknowledged record
    /// away. In Raft terms, a replica whose log is ahead of the candidate
    /// would refuse it the vote; counting it toward the quorum anyway is how
    /// promotion used to conclude an entry was uncommitted merely because
    /// the candidate had not seen it. Deliberately per-voter rather than
    /// candidate-must-hold-the-maximum: a majority at or below the candidate
    /// is exactly §5.4.1's guarantee (any acknowledged record's quorum
    /// intersects every vote quorum), and the stricter form would refuse a
    /// legitimate leader over a record that was never acknowledged.
    CandidateBehindVoters {
        /// The candidate's own offset.
        candidate_offset: u64,
        /// How many fenced replicas are at or below the candidate.
        votes: usize,
        required: usize,
        /// The most complete replica observed — the one an operator or the
        /// lease agent should let win the range instead.
        most_complete: (Uuid, u64),
    },
    /// The boundary this quorum proves sits below a watermark an earlier
    /// promotion of the range already published (#449). Two majority-sized
    /// answer sets need share only one member, so the k-th highest offset
    /// of this one can fall under what the last one vouched for — and
    /// publishing it would hand consumers a watermark that went backwards
    /// and truncate acknowledged records off the one replica still holding
    /// them. Refused: the replica that holds the published prefix should
    /// win the range instead.
    BelowPublished {
        committed_offset: u64,
        published_floor: u64,
        /// The most complete replica observed.
        most_complete: (Uuid, u64),
    },
}

/// Majority of a replica set, including the leader itself.
///
/// Integer division then +1: for 3 replicas that is 2, for 5 it is 3. Writing
/// it as `len / 2 + 1` rather than `ceil(len / 2)` matters at even sizes — a
/// 4-replica set needs 3, not 2, or two disjoint "majorities" could exist.
pub fn majority(replica_count: usize) -> usize {
    replica_count / 2 + 1
}

/// Establish the committed boundary from replica probes.
///
/// `probes` must include the leader's own view: it is a replica of the range
/// and its disk counts toward the quorum. Omitting it would make a 3-replica
/// range need both followers, turning any single follower outage into a failed
/// promotion.
///
/// `replication_factor` is the CONFIGURED size of the replica set, not the
/// number of probes that came back. Deriving the majority from what answered
/// would let a partition shrink the quorum: three reachable replicas out of
/// five would compute a majority of two, and two disjoint halves could each
/// promote.
///
/// `leader_id` names which probe is the candidate itself. A boundary the
/// quorum can prove but the leader's own disk does not reach is refused
/// ([`Promotion::LeaderBehind`]): publishing it would let the produce fast
/// path acknowledge writes into offsets occupied by committed records the
/// leader never held.
/// Held ABOVE a watermark the range already published (#449).
///
/// `published_floor` is the highest boundary any earlier promotion of this
/// range published — what metadata's transition chain holds — and the
/// boundary established here must not fall below it. It can: the quorum's
/// k-th highest offset is computed over whichever majority answered THIS
/// fence, and two majorities need share only one member, so a candidate
/// whose answer set is mostly behind can prove a boundary under the one the
/// last leader published. That boundary would be a watermark going
/// backwards for every consumer, and reconciliation would truncate the
/// acknowledged records above it off the one replica still holding them.
/// `None` is a range with no published boundary yet, or a caller that
/// cannot supply one; the check is then not made, and the caller says so.
pub fn establish(
    probes: &[ReplicaProbe],
    replication_factor: usize,
    leader_id: Uuid,
    published_floor: Option<u64>,
) -> Promotion {
    debug_assert!(
        probes.len() <= replication_factor,
        "more probes ({}) than configured replicas ({replication_factor})",
        probes.len()
    );
    let required = majority(replication_factor);
    // ONLY A PROVEN LINEAGE COUNTS (#577). An Unknown answer is fenced and
    // stopped like any other, but nothing reconciled it against the
    // candidate, so its offset counts records whatever leadership wrote
    // them - see the module docs for what counting it would let through.
    // It is kept aside for the operator record and nothing else.
    let mut answered = BTreeMap::new();
    let mut unproven = BTreeMap::new();
    for probe in probes {
        if let Some(offset) = probe.local_committed_offset {
            if probe.lineage.is_provable() {
                answered.insert(probe.node_id, offset);
            } else {
                unproven.insert(probe.node_id, offset);
            }
        }
    }
    if answered.len() < required {
        return Promotion::QuorumUnavailable {
            answered: answered.len(),
            required,
            unproven: unproven.len(),
        };
    }
    debug_assert_eq!(
        answered.len() + unproven.len(),
        probes
            .iter()
            .filter(|p| p.local_committed_offset.is_some())
            .count(),
        "duplicate node ids would collapse in the map while the quorum \
         requirement did not"
    );
    // Sort descending and take the k-th, where k is the majority size: the
    // highest offset that `required` replicas can each vouch for.
    let mut offsets: Vec<u64> = answered.values().copied().collect();
    offsets.sort_unstable_by(|a, b| b.cmp(a));
    let mut committed_offset = offsets[required - 1];
    // The published watermark first (#449). A boundary under it is not
    // published as it stands: the floor was quorum-proven by the promotion
    // that published it, so a candidate that HOLDS the floor may publish the
    // floor itself — raised to the published floor, the followers behind it
    // reconciled up from the leader as usual (review: refusing every
    // candidate, the floor's holder included, would leave the range
    // unleadable). A candidate below the floor is refused, and the replica
    // that holds the most is named as the one that should win instead.
    if let Some(published_floor) = published_floor {
        if committed_offset < published_floor {
            let candidate_offset = answered.get(&leader_id).copied();
            if candidate_offset.is_some_and(|held| held >= published_floor) {
                committed_offset = published_floor;
            } else {
                let most_complete = answered
                    .iter()
                    .max_by_key(|(_, offset)| **offset)
                    .map(|(node, offset)| (*node, *offset))
                    .expect("a quorum answered");
                return Promotion::BelowPublished {
                    committed_offset,
                    published_floor,
                    most_complete,
                };
            }
        }
    }
    // The candidate must itself hold the boundary it is about to publish.
    let leader_committed_offset = answered.get(&leader_id).copied();
    if leader_committed_offset.is_none_or(|leader_offset| leader_offset < committed_offset) {
        return Promotion::LeaderBehind {
            committed_offset,
            leader_committed_offset,
        };
    }
    let candidate_offset =
        leader_committed_offset.expect("a candidate below the floor returned above");
    // Raft's election restriction (§5.4.1), per voter: only replicas at or
    // below the candidate's own offset would have granted it the vote, and a
    // majority of grants is what makes the promotion safe — any record
    // acknowledged on a quorum lives on at least one member of every
    // majority, so a candidate a majority can vouch for holds every
    // acknowledged record. The floor check above cannot substitute: the
    // k-th largest can sit below an acknowledged record whose surviving
    // holders straddle the candidate.
    //
    // These are POST-RECONCILIATION offsets: the fence reconciles before it
    // answers (#263). That is sound only because reconciliation discards
    // nothing that could be acknowledged. A record it discards can no longer
    // refuse the candidate here.
    //
    // A same-epoch prefix is not divergence, but the vectors do not show a
    // bare prefix. The candidate adopts the new epoch at its own tail before
    // it sends its history, and the replica adopts it at its tail, so a
    // replica ahead of the candidate compares `DivergesAt(candidate tail)`.
    // Until #611 that verdict truncated the replica down to the candidate.
    // With RF=3, a record the deposed leader and this replica acknowledged
    // was lost when the replica had not yet learned the commit mark. The
    // replica then granted the vote it should have refused.
    //
    // Two rules close it. They answer one question: whose records are
    // acknowledged?
    //
    // * The replica discards a disputed suffix only when the candidate
    //   PROVES it unacknowledged (`fencing_epochs::caller_supersedes_suffix`).
    //   The candidate must hold a record at the divergence point, written
    //   under an epoch newer than every record the replica would lose. Its
    //   adoption of the fence epoch at its tail owns no record and proves
    //   nothing. A replica that merely holds more, or holds records from a
    //   leadership the candidate never followed, keeps them.
    // * The candidate counts such an answer (`fenced_lineage`): a divergence
    //   at or above its own tail concerns only records it lacks. So the
    //   replica's full offset arrives here and refuses the candidate below.
    //
    // The inductive argument then holds without exception. A record
    // acknowledged on a quorum intersects every later fence-majority. The
    // intersecting voter keeps the record through the fence, since no
    // candidate lacking it can supersede it. If the candidate simply holds
    // less, the voter's offset is above the candidate's and it refuses
    // here. If the candidate holds a different record below its own tail,
    // the answer is Unknown and never counted. Every granted epoch's leader
    // therefore holds every previously acknowledged record, and a suffix a
    // voter does lose (the candidate's newer epoch owns a record there) was
    // written under a superseded line and never acknowledged.
    //
    // A kept suffix whose holder is outvoted is provably unacknowledged once
    // the candidate is established. The replica discards it on the new
    // leader's first append over it, not as an ack of a record it does not
    // hold (#261). It remembers that in memory only: across a restart it is
    // the same gap as a replica absent from the fence. The
    // truncation-below-HWM guard stays as the last resort for histories
    // that predate the restriction, seeded across restarts from the
    // follower's persisted `committed-floor`.
    let votes = answered
        .values()
        .filter(|offset| **offset <= candidate_offset)
        .count();
    if votes < required {
        let most_complete = answered
            .iter()
            .max_by_key(|(_, offset)| **offset)
            .map(|(node, offset)| (*node, *offset))
            .expect("a quorum answered");
        return Promotion::CandidateBehindVoters {
            candidate_offset,
            votes,
            required,
            most_complete,
        };
    }
    Promotion::Established {
        committed_offset,
        answered,
        unproven,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(node: u128, offset: Option<u64>) -> ReplicaProbe {
        ReplicaProbe {
            node_id: Uuid::from_u128(node),
            local_committed_offset: offset,
            lineage: AnswerLineage::Agreed,
        }
    }

    fn probe_with(node: u128, offset: u64, lineage: AnswerLineage) -> ReplicaProbe {
        ReplicaProbe {
            node_id: Uuid::from_u128(node),
            local_committed_offset: Some(offset),
            lineage,
        }
    }

    /// REGRESSION shape, from #240's #265 postmortem: A acknowledges a
    /// record on {A, B} and dies; B answers 101, candidate C answers 100.
    /// The floor computes to 100 and C holds it, so every pre-§5.4.1 check
    /// passed — and B's acknowledged record was then reconciled away under
    /// C's boundary. The election restriction refuses C: only one fenced
    /// replica is at or below C's offset, and one is not a majority.
    /// The boundary never falls below a watermark the range already
    /// published (#449): two majorities that share one member can prove a
    /// lower k-th offset, and that is refused, naming the replica that
    /// holds the most.
    #[test]
    fn a_boundary_below_the_published_watermark_is_refused() {
        // Replica 3 held the previously published 100; the other two are
        // behind, so the k-th highest of this majority is 90.
        let probes = [probe(1, Some(90)), probe(2, Some(80)), probe(3, Some(100))];
        // A candidate below the floor is refused, naming the replica that
        // holds it.
        assert_eq!(
            establish(&probes, 3, Uuid::from_u128(1), Some(100)),
            Promotion::BelowPublished {
                committed_offset: 90,
                published_floor: 100,
                most_complete: (Uuid::from_u128(3), 100),
            },
            "90 is below the published 100, and node 1 does not hold 100"
        );
        // The candidate that HOLDS the floor publishes the floor, not the
        // lower order statistic: the range stays leadable by its holder.
        assert!(
            matches!(
                establish(&probes, 3, Uuid::from_u128(3), Some(100)),
                Promotion::Established {
                    committed_offset: 100,
                    ..
                }
            ),
            "{:?}",
            establish(&probes, 3, Uuid::from_u128(3), Some(100))
        );
        // At the floor, or with no floor to hold, the boundary stands as
        // the quorum proved it.
        assert!(matches!(
            establish(&probes, 3, Uuid::from_u128(3), Some(90)),
            Promotion::Established {
                committed_offset: 90,
                ..
            }
        ));
        assert!(matches!(
            establish(&probes, 3, Uuid::from_u128(3), None),
            Promotion::Established {
                committed_offset: 90,
                ..
            }
        ));
    }

    #[test]
    fn a_candidate_a_fenced_replica_would_refuse_the_vote_is_not_promoted() {
        let outcome = establish(
            &[probe(2, Some(101)), probe(3, Some(100))],
            3,
            Uuid::from_u128(3),
            None,
        );
        assert_eq!(
            outcome,
            Promotion::CandidateBehindVoters {
                candidate_offset: 100,
                votes: 1,
                required: 2,
                most_complete: (Uuid::from_u128(2), 101),
            },
            "a candidate counting a fenced replica ahead of its own log used to promote at a              floor below an acknowledged record; the replica ahead is the one that must win"
        );
    }

    /// The remedy the refusal names: the more complete replica promotes over
    /// the identical probe set.
    #[test]
    fn the_replica_the_refusal_names_promotes_over_the_same_probes() {
        let outcome = establish(
            &[probe(2, Some(101)), probe(3, Some(100))],
            3,
            Uuid::from_u128(2),
            None,
        );
        match outcome {
            Promotion::Established {
                committed_offset, ..
            } => assert_eq!(
                committed_offset, 100,
                "the floor is still what the quorum can vouch for; the record above it is                  protected by the candidate holding it, not by the floor"
            ),
            other => panic!("the most complete replica must promote: {other:?}"),
        }
    }

    /// Deliberately Raft's PER-VOTER form, not candidate-holds-the-maximum:
    /// at RF 5 a candidate with a majority at or below it may lead even
    /// though one fenced replica is ahead — the record making that replica
    /// ahead was never acknowledged (its quorum would have needed three),
    /// and refusing here would trade availability for nothing.
    #[test]
    fn a_majority_at_or_below_the_candidate_promotes_despite_a_more_complete_minority() {
        let outcome = establish(
            &[
                probe(2, Some(101)),
                probe(3, Some(100)),
                probe(4, Some(100)),
                probe(5, Some(100)),
            ],
            5,
            Uuid::from_u128(3),
            None,
        );
        match outcome {
            Promotion::Established {
                committed_offset, ..
            } => assert_eq!(committed_offset, 100),
            other => panic!("a candidate with majority votes must promote: {other:?}"),
        }
    }

    #[test]
    fn a_majority_needs_more_than_half_even_at_even_sizes() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(3), 2);
        // 3-of-4, not 2-of-4: at even sizes half is not a majority, and two
        // disjoint groups of 2 could each call themselves one.
        assert_eq!(majority(4), 3);
        assert_eq!(majority(5), 3);
    }

    /// The central rule: take what a quorum can prove, not what the
    /// furthest-ahead replica happens to hold.
    #[test]
    fn the_boundary_is_what_a_majority_can_vouch_for() {
        let promotion = establish(
            &[probe(1, Some(100)), probe(2, Some(90)), probe(3, Some(50))],
            3,
            Uuid::from_u128(1),
            None,
        );
        let Promotion::Established {
            committed_offset, ..
        } = promotion
        else {
            panic!("expected an established boundary, got {promotion:?}")
        };
        assert_eq!(
            committed_offset, 90,
            "two of three replicas hold 90; only one holds 100"
        );
    }

    /// Taking the maximum would expose records the old leader never managed to
    /// acknowledge — records that can still be lost.
    #[test]
    fn a_lone_replica_ahead_of_the_pack_does_not_set_the_boundary() {
        let promotion = establish(
            &[
                probe(1, Some(1_000)),
                probe(2, Some(10)),
                probe(3, Some(10)),
            ],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert!(matches!(
            promotion,
            Promotion::Established {
                committed_offset: 10,
                ..
            }
        ));
    }

    /// Taking the minimum would discard offsets a quorum genuinely holds and
    /// stall the range behind its slowest member.
    #[test]
    fn a_lagging_replica_does_not_drag_the_boundary_down() {
        let promotion = establish(
            &[probe(1, Some(100)), probe(2, Some(100)), probe(3, Some(0))],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert!(matches!(
            promotion,
            Promotion::Established {
                committed_offset: 100,
                ..
            }
        ));
    }

    /// Without a quorum the only safe answer is to refuse. Promoting anyway
    /// would publish a high-water mark nobody had confirmed.
    #[test]
    fn too_few_answers_refuses_rather_than_guessing() {
        let promotion = establish(
            &[probe(1, Some(100)), probe(2, None), probe(3, None)],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert_eq!(
            promotion,
            Promotion::QuorumUnavailable {
                answered: 1,
                required: 2,
                unproven: 0,
            }
        );
    }

    /// A replica that is merely slow to answer must not be counted as holding
    /// offset zero — that would drag the boundary to nothing.
    #[test]
    fn an_unreachable_replica_is_absent_not_zero() {
        let promotion = establish(
            &[probe(1, Some(100)), probe(2, Some(100)), probe(3, None)],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert!(
            matches!(
                promotion,
                Promotion::Established {
                    committed_offset: 100,
                    ..
                }
            ),
            "two answers meet the majority of three; the silent one is not a zero"
        );
    }

    /// The gate Codex's 50/90/90 scenario demands: a quorum can prove 90, but
    /// a leader whose own log ends at 50 must not publish it — the produce
    /// fast path would then acknowledge fresh writes into offsets 50..90 that
    /// are occupied by committed records the leader never held.
    #[test]
    fn a_leader_behind_the_proven_boundary_is_refused() {
        let promotion = establish(
            &[probe(1, Some(50)), probe(2, Some(90)), probe(3, Some(90))],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert_eq!(
            promotion,
            Promotion::LeaderBehind {
                committed_offset: 90,
                leader_committed_offset: Some(50),
            }
        );
    }

    /// Exactly AT the boundary is servable: the leader holds everything the
    /// quorum can prove. Only strictly behind is refused.
    #[test]
    fn a_leader_at_the_proven_boundary_promotes() {
        let promotion = establish(
            &[probe(1, Some(90)), probe(2, Some(90)), probe(3, Some(100))],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert!(matches!(
            promotion,
            Promotion::Established {
                committed_offset: 90,
                ..
            }
        ));
    }

    /// A leader whose own probe is missing cannot publish any boundary,
    /// however many followers answered — it cannot verify its own disk covers
    /// what it is about to advertise.
    #[test]
    fn a_leader_that_did_not_answer_its_own_probe_is_refused() {
        let promotion = establish(
            &[probe(1, None), probe(2, Some(90)), probe(3, Some(90))],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert_eq!(
            promotion,
            Promotion::LeaderBehind {
                committed_offset: 90,
                leader_committed_offset: None,
            }
        );
    }

    /// A standalone range is its own quorum; requiring anyone else would make
    /// single-replica deployments unpromotable.
    #[test]
    fn a_single_replica_range_promotes_on_its_own_view() {
        let promotion = establish(&[probe(1, Some(42))], 1, Uuid::from_u128(1), None);
        assert!(matches!(
            promotion,
            Promotion::Established {
                committed_offset: 42,
                ..
            }
        ));
    }

    /// #577, the issue's own shape: the candidate and one replica answer,
    /// the third is unreachable. The replica answered with the candidate's
    /// offset, but its lineage could not be compared (no epoch history), so
    /// that offset is a count of records whatever wrote them. By offset it
    /// completed the majority and promotion established; it must not.
    #[test]
    fn an_unknown_lineage_replica_does_not_complete_a_majority() {
        let promotion = establish(
            &[
                probe(1, Some(100)),
                probe_with(2, 100, AnswerLineage::Unknown),
                probe(3, None),
            ],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert_eq!(
            promotion,
            Promotion::QuorumUnavailable {
                answered: 1,
                required: 2,
                unproven: 1,
            },
            "an answer whose lineage was not proven was counted toward the quorum"
        );
    }

    /// #577: nor does an Unknown answer move the boundary. By offset, the
    /// k-th highest of {500, 100, 90} is 100; replica 3's 500 is records
    /// nobody proved are the candidate's, so the boundary is what the two
    /// proven answers hold, and it is set aside in the record.
    #[test]
    fn an_unknown_lineage_replica_does_not_set_the_boundary() {
        let promotion = establish(
            &[
                probe(1, Some(100)),
                probe(2, Some(90)),
                probe_with(3, 500, AnswerLineage::Unknown),
            ],
            3,
            Uuid::from_u128(1),
            None,
        );
        let Promotion::Established {
            committed_offset,
            answered,
            unproven,
        } = promotion
        else {
            panic!("two proven answers are a majority of three: {promotion:?}")
        };
        assert_eq!(committed_offset, 90, "the unknown 500 lifted the boundary");
        assert_eq!(answered.len(), 2);
        assert_eq!(
            unproven.into_iter().collect::<Vec<_>>(),
            vec![(Uuid::from_u128(3), 500)]
        );
    }

    /// #577: a replica that discarded a diverged suffix and THEN answered
    /// holds the candidate's history up to its offset, as surely as one that
    /// never diverged. It counts.
    #[test]
    fn a_reconciled_replica_counts() {
        let promotion = establish(
            &[
                probe(1, Some(100)),
                probe_with(
                    2,
                    100,
                    AnswerLineage::Reconciled {
                        truncated_records: 3,
                    },
                ),
                probe(3, None),
            ],
            3,
            Uuid::from_u128(1),
            None,
        );
        assert!(
            matches!(
                promotion,
                Promotion::Established {
                    committed_offset: 100,
                    ..
                }
            ),
            "{promotion:?}"
        );
    }

    fn starts(entries: &[(u64, u64)]) -> Vec<EpochStart> {
        entries
            .iter()
            .map(|(epoch, start_offset)| EpochStart {
                epoch: *epoch,
                start_offset: *start_offset,
            })
            .collect()
    }

    /// #577: the verdict the candidate draws from what it sent and what the
    /// fenced replica returned, case by case - each the way a real fence
    /// leaves the two vectors (both sides adopt the new epoch at their OWN
    /// tail, which is bookkeeping, not dispute).
    #[test]
    fn the_verdict_on_a_fenced_answer() {
        // Candidate at 100 under epochs 1 and 2, fenced at 3.
        let candidate = starts(&[(1, 0), (2, 50), (3, 100)]);

        // Lagging but agreeing: the replica's only difference is where it
        // adopted 3, at its own tail.
        assert_eq!(
            fenced_lineage(&candidate, &starts(&[(1, 0), (2, 50), (3, 90)]), 90, 0, 100),
            AnswerLineage::Agreed
        );
        // Lagging so far it never saw epoch 2: every record it holds is
        // epoch 1's, as on the candidate.
        assert_eq!(
            fenced_lineage(&candidate, &starts(&[(1, 0), (3, 40)]), 40, 0, 100),
            AnswerLineage::Agreed
        );
        // Diverged and reconciled: it discarded what a rival epoch wrote and
        // re-anchored 3 where the candidate has it.
        assert_eq!(
            fenced_lineage(
                &candidate,
                &starts(&[(1, 0), (2, 50), (3, 100)]),
                100,
                4,
                100
            ),
            AnswerLineage::Reconciled {
                truncated_records: 4
            }
        );
        // No history on the replica (records, no journal): Unknown.
        assert_eq!(
            fenced_lineage(&candidate, &[], 100, 0, 100),
            AnswerLineage::Unknown
        );
        // No history on the CANDIDATE: nothing is provable about anyone.
        assert_eq!(
            fenced_lineage(&[], &starts(&[(1, 0), (3, 90)]), 90, 0, 100),
            AnswerLineage::Unknown
        );
        // A replica holding nothing disputes nothing, even against a
        // candidate with no history (a fresh range's first promotion).
        assert_eq!(fenced_lineage(&[], &[], 0, 0, 0), AnswerLineage::Agreed);
        assert_eq!(
            fenced_lineage(&[], &starts(&[(3, 0)]), 0, 0, 0),
            AnswerLineage::Agreed
        );
        // #611: AHEAD of the candidate in the candidate's own last epoch.
        // The two disagree only from 100, where the candidate's adoption of
        // 3 owns no record, so every record the candidate holds is agreed.
        // The replica kept 100..120 and its answer counts, above the
        // candidate, which is the vote it refuses.
        assert_eq!(
            fenced_lineage(
                &candidate,
                &starts(&[(1, 0), (2, 50), (3, 120)]),
                120,
                0,
                100
            ),
            AnswerLineage::Agreed
        );
        // The same answer judged by a candidate that HOLDS records past 100:
        // the replica's records there are not the candidate's, unproven.
        assert_eq!(
            fenced_lineage(
                &candidate,
                &starts(&[(1, 0), (2, 50), (3, 120)]),
                120,
                0,
                110
            ),
            AnswerLineage::Unknown
        );
        // A divergence below the replica's tail that it did not act on (the
        // retention-confined dispute): its offset is not proven.
        assert_eq!(
            fenced_lineage(&candidate, &starts(&[(1, 0), (7, 50), (3, 90)]), 90, 0, 100),
            AnswerLineage::Unknown
        );
    }

    #[test]
    fn nothing_answering_is_never_an_established_zero() {
        assert!(matches!(
            establish(
                &[probe(1, None), probe(2, None), probe(3, None)],
                3,
                Uuid::from_u128(1),
                None,
            ),
            Promotion::QuorumUnavailable { answered: 0, .. }
        ));
    }
}

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
//! **Open: promotion does not yet USE that vector.** A [`ReplicaProbe`] still
//! carries a bare offset, so two replicas reporting 90 are still compared as
//! numbers here even though each can now prove whose writes put it there.
//!
//! **Open: followers are never truncated by an election.** A replica holding
//! uncommitted records above the established boundary keeps them. This code
//! correctly refuses to *expose* them, but if that replica later wins the lease
//! they resurface. The truncation primitive exists and is bounded; what is
//! missing is this module driving it from the divergence point.
//!
//! **Open, and subtler, from Raft §5.4.2:** a new leader must not commit
//! entries from a previous term by counting replicas. The Raft-safe form is to
//! append a marker in the new epoch and let prior entries commit implicitly
//! once that is quorum-acked. VTOP gets the epoch for free from the lease mint;
//! the marker record does not exist yet.
//!
//! What is here is the floor computation, a fenced read to compute it from, and
//! an honest refusal when a quorum cannot be reached.
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

/// What one replica reported, or that it did not answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaProbe {
    pub node_id: Uuid,
    /// `None` when the replica could not be reached or refused.
    pub local_committed_offset: Option<u64>,
}

/// The outcome of asking a replica set where it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Promotion {
    /// A quorum answered; this is the boundary they can prove.
    Established {
        committed_offset: u64,
        /// Replicas that answered, for the operator record.
        answered: BTreeMap<Uuid, u64>,
    },
    /// Too few replicas answered to establish anything.
    ///
    /// Refusing to serve is the only safe response. A leader that promoted
    /// anyway would be publishing a high-water mark no quorum had confirmed,
    /// which is precisely the guess this exists to prevent.
    QuorumUnavailable { answered: usize, required: usize },
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
pub fn establish(probes: &[ReplicaProbe], replication_factor: usize, leader_id: Uuid) -> Promotion {
    debug_assert!(
        probes.len() <= replication_factor,
        "more probes ({}) than configured replicas ({replication_factor})",
        probes.len()
    );
    let required = majority(replication_factor);
    let mut answered = BTreeMap::new();
    for probe in probes {
        if let Some(offset) = probe.local_committed_offset {
            answered.insert(probe.node_id, offset);
        }
    }
    if answered.len() < required {
        return Promotion::QuorumUnavailable {
            answered: answered.len(),
            required,
        };
    }
    debug_assert_eq!(
        answered.len(),
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
    let committed_offset = offsets[required - 1];
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(node: u128, offset: Option<u64>) -> ReplicaProbe {
        ReplicaProbe {
            node_id: Uuid::from_u128(node),
            local_committed_offset: offset,
        }
    }

    /// REGRESSION shape, from #240's #265 postmortem: A acknowledges a
    /// record on {A, B} and dies; B answers 101, candidate C answers 100.
    /// The floor computes to 100 and C holds it, so every pre-§5.4.1 check
    /// passed — and B's acknowledged record was then reconciled away under
    /// C's boundary. The election restriction refuses C: only one fenced
    /// replica is at or below C's offset, and one is not a majority.
    #[test]
    fn a_candidate_a_fenced_replica_would_refuse_the_vote_is_not_promoted() {
        let outcome = establish(
            &[probe(2, Some(101)), probe(3, Some(100))],
            3,
            Uuid::from_u128(3),
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
        );
        assert_eq!(
            promotion,
            Promotion::QuorumUnavailable {
                answered: 1,
                required: 2
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
        let promotion = establish(&[probe(1, Some(42))], 1, Uuid::from_u128(1));
        assert!(matches!(
            promotion,
            Promotion::Established {
                committed_offset: 42,
                ..
            }
        ));
    }

    #[test]
    fn nothing_answering_is_never_an_established_zero() {
        assert!(matches!(
            establish(
                &[probe(1, None), probe(2, None), probe(3, None)],
                3,
                Uuid::from_u128(1)
            ),
            Promotion::QuorumUnavailable { answered: 0, .. }
        ));
    }
}

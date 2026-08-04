//! A VTOP-owned, consensus-library-free snapshot of a metadata node's Raft
//! state, for the node operational surface (#224).
//!
//! # Why a translation and not a passthrough
//!
//! The crate's containment policy (see [`crate::raft`]) keeps every openraft
//! type inside `raft/`. That is not bookkeeping: it is what lets the consensus
//! implementation be replaced without touching the metrics, the CLI, or the
//! wire. So the metrics exporter in `vtop-node` consumes
//! [`RaftObservation`] — plain integers and a closed state enum — and the
//! field-by-field translation lives here beside every other adapter boundary.
//!
//! # Non-blocking by contract
//!
//! [`observe`](crate::raft::consensus::OpenraftConsensus::observe) reads the
//! latest published metrics snapshot. It never sends a message to the Raft
//! core and never awaits, because it is called from a scrape handler: a
//! `/metrics` request must not be able to queue behind consensus work, least of
//! all on a node that is struggling.

use std::collections::BTreeMap;

use crate::keys::MetaNodeId;

/// The role a Raft node is currently playing. Closed set: it is exported as a
/// metric label, and a free-form string there would be unbounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftServerState {
    Learner,
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

impl RaftServerState {
    pub const ALL: [Self; 5] = [
        Self::Learner,
        Self::Follower,
        Self::Candidate,
        Self::Leader,
        Self::Shutdown,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Learner => "learner",
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Everything an operator can learn about a metadata node without proposing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftObservation {
    pub node_id: MetaNodeId,
    /// False once a fatal error stopped the Raft core. The process may still be
    /// alive and answering health checks, which is exactly why this is
    /// reported separately from liveness.
    pub running: bool,
    pub current_term: u64,
    pub server_state: RaftServerState,
    pub current_leader: Option<MetaNodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub voters: usize,
    pub learners: usize,
    /// Milliseconds since a quorum last acknowledged this leader; `None` when
    /// this node is not leading or has not been acknowledged yet. A value that
    /// climbs on a self-declared leader is the signature of a partition, so it
    /// is the one metric that distinguishes "isolated" from "slow".
    pub millis_since_quorum_ack: Option<u64>,
    /// Highest index each peer has acknowledged, or `None` for a peer that has
    /// acknowledged nothing yet.
    ///
    /// The `None` is load-bearing: a freshly added learner that has replied to
    /// nothing and a peer sitting at the first index are entirely different
    /// situations, and collapsing both to `0` would let a dashboard read "no
    /// contact at all" as real replication progress.
    ///
    /// Populated on a leader only; empty elsewhere, so a demoted node stops
    /// publishing stale follower progress rather than leaving yesterday's
    /// numbers on a dashboard.
    pub peer_matched_index: BTreeMap<MetaNodeId, Option<u64>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_state_has_a_distinct_bounded_label() {
        let labels: std::collections::BTreeSet<_> =
            RaftServerState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            labels.len(),
            RaftServerState::ALL.len(),
            "two states sharing a label would silently merge series on a dashboard"
        );
    }
}

//! Requests the gateway refused, counted by API and reason (#596).
//!
//! A refusal the gateway logs under a rate limit is invisible to an alert
//! unless it is also counted: the log says it once per power of two, and a
//! client that commits every five seconds against a node with no offset
//! store produces a handful of lines a day and thousands of refusals. The
//! node exports these totals as `vtop_kafka_requests_refused_total{api,reason}`.
//!
//! Both labels come from this closed set, never from the request: an api key
//! a client sends is an `i16` of its choosing, so every key this gateway does
//! not serve is counted under one `api="unserved"`, and the log line names
//! the key.

use std::sync::atomic::{AtomicU64, Ordering};

/// One kind of refusal, with the labels it is exported under.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// OffsetCommit on a gateway with no durable offset store (a static
    /// leader or a standalone node): answered INVALID_REQUEST.
    OffsetCommitWithoutStore,
    /// A request whose api key this gateway does not serve (CreateTopics,
    /// DescribeConfigs and the rest): the connection is closed.
    UnservedApi,
    /// A Produce this node answered NOT_LEADER_OR_FOLLOWER because it does
    /// not hold the range (#751): a candidate that follows, or a leader whose
    /// lease moved on.
    ProduceNotLeader,
    /// The same for a Fetch.
    FetchNotLeader,
    /// The same for a ListOffsets.
    ListOffsetsNotLeader,
}

impl Refusal {
    /// Every refusal, in export order. A new variant goes here too, or it is
    /// never exported (a test pins the count).
    pub const ALL: [Refusal; 5] = [
        Refusal::OffsetCommitWithoutStore,
        Refusal::UnservedApi,
        Refusal::ProduceNotLeader,
        Refusal::FetchNotLeader,
        Refusal::ListOffsetsNotLeader,
    ];

    /// The `api` label.
    pub fn api(self) -> &'static str {
        match self {
            Refusal::OffsetCommitWithoutStore => "offset_commit",
            Refusal::UnservedApi => "unserved",
            Refusal::ProduceNotLeader => "produce",
            Refusal::FetchNotLeader => "fetch",
            Refusal::ListOffsetsNotLeader => "list_offsets",
        }
    }

    /// The `reason` label.
    pub fn reason(self) -> &'static str {
        match self {
            Refusal::OffsetCommitWithoutStore => "no_offset_store",
            Refusal::UnservedApi => "unserved_api",
            Refusal::ProduceNotLeader | Refusal::FetchNotLeader | Refusal::ListOffsetsNotLeader => {
                "not_leader"
            }
        }
    }

    fn index(self) -> usize {
        match self {
            Refusal::OffsetCommitWithoutStore => 0,
            Refusal::UnservedApi => 1,
            Refusal::ProduceNotLeader => 2,
            Refusal::FetchNotLeader => 3,
            Refusal::ListOffsetsNotLeader => 4,
        }
    }
}

/// Monotonic totals, one per [`Refusal`], shared between the gateway that
/// counts and the collector that exports.
#[derive(Debug, Default)]
pub struct Refusals {
    counts: [AtomicU64; Refusal::ALL.len()],
}

impl Refusals {
    /// Count one refusal and return the new total for its kind, so the
    /// caller can rate-limit its log line on the same number.
    pub fn count(&self, refusal: Refusal) -> u64 {
        self.counts[refusal.index()].fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The total for one kind.
    pub fn get(&self, refusal: Refusal) -> u64 {
        self.counts[refusal.index()].load(Ordering::Relaxed)
    }

    /// Every kind with its total, zeros included: a series that exists at
    /// zero is one an alert can be written against before the first refusal.
    pub fn snapshot(&self) -> Vec<(Refusal, u64)> {
        Refusal::ALL
            .iter()
            .map(|refusal| (*refusal, self.get(*refusal)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_refusal_is_exported_once_with_bounded_labels() {
        let mut seen = std::collections::HashSet::new();
        for (index, refusal) in Refusal::ALL.iter().enumerate() {
            assert_eq!(refusal.index(), index, "{refusal:?} out of export order");
            assert!(
                seen.insert((refusal.api(), refusal.reason())),
                "{refusal:?}"
            );
            for label in [refusal.api(), refusal.reason()] {
                assert!(
                    label.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                    "{label:?} is not a fixed snake_case label"
                );
            }
        }
    }

    #[test]
    fn counts_are_per_kind_and_returned_for_rate_limiting() {
        let refusals = Refusals::default();
        assert_eq!(refusals.count(Refusal::UnservedApi), 1);
        assert_eq!(refusals.count(Refusal::UnservedApi), 2);
        assert_eq!(refusals.get(Refusal::OffsetCommitWithoutStore), 0);
        assert_eq!(
            refusals.snapshot(),
            vec![
                (Refusal::OffsetCommitWithoutStore, 0),
                (Refusal::UnservedApi, 2),
                (Refusal::ProduceNotLeader, 0),
                (Refusal::FetchNotLeader, 0),
                (Refusal::ListOffsetsNotLeader, 0),
            ]
        );
    }
}

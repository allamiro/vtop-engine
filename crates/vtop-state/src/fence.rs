//! A fencing token for the batch ledger's claim (#557).
//!
//! `claim_incomplete_batches` records who may work on a batch (`owner`) and
//! until when (`lease_expires_at`). Neither stops a writer that has already
//! decided to write: an engine that stalls past its lease (a long multipart
//! upload under throttling, a VM pause) and then resumes would write its next
//! transition over a row that another engine has since claimed and moved on,
//! and `UPDATE ... WHERE state = $expected` lets it through whenever the new
//! owner happens to have reached the state the stale one expects, because
//! state names recur. A lease is a lock with a timeout, not a fence.
//!
//! The fence is a generation on the row: every claim bumps `claim_gen`, and
//! every transition by a process that holds a generation is conditional on the
//! row still being at it. A process that lost the claim writes zero rows and
//! is told so, whatever state the new owner has reached and whatever name it
//! runs under (two engines that share an `owner` string are still fenced
//! apart).
//!
//! This process remembers the generation of the rows it created (0) and the
//! rows it claimed (the value the claim returned), and writes ONLY those: a
//! row it holds no generation for is refused, never written unconditionally.
//! The engine is the only writer of ledger rows and every row it writes it
//! created or claimed first (recovery claims before it touches anything), so
//! a missing token means the token was dropped - and a dropped token that
//! silently disabled the check would let exactly the stalled writer this
//! exists to stop through (review of #569).
//!
//! Tokens are kept while they can still be written:
//! - **active** - created or claimed, work in flight. Never dropped, whatever
//!   the count: a claim that returns more rows than any bound still fences
//!   every one of them. They cost no more than the rows the claim returned.
//! - **parked** - the work stopped without ending: the batch FAILED (recovery
//!   writes REPLAY_REQUIRED over it next, and must still be fenced while it
//!   does), or this process lost the claim (the token is what refuses the
//!   stalled writer if it tries again). At most [`MAX_PARKED`] are kept; the
//!   oldest go first, and a write to one that went is refused.
//! - **released** - committed or handed back for replay: nothing more to write.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;

/// The most parked tokens kept. A parked token belongs to a batch this
/// process expects at most one more write to (REPLAY_REQUIRED after FAILED)
/// or none, so the bound is reached only by a long outage that fails batch
/// after batch; dropping the oldest then refuses their writes (fail closed).
const MAX_PARKED: usize = 65_536;

struct Token {
    generation: i64,
    /// `Some(seq)` while parked: the position it was parked at, so an entry
    /// in the eviction queue left behind by a token since released, re-claimed
    /// or re-parked is recognised as stale and cannot evict the live token.
    parked: Option<u64>,
}

#[derive(Default)]
struct Held {
    tokens: HashMap<String, Token>,
    /// Parked tokens in the order they were parked, with their seq. May hold
    /// stale entries (see [`Token::parked`]); they are skipped, and compacted
    /// away once they outnumber the live ones.
    parked_order: VecDeque<(String, u64)>,
    parked: usize,
    next_seq: u64,
}

/// The claim generation this process holds, per batch.
#[derive(Clone, Default)]
pub(crate) struct ClaimFence {
    held: Arc<Mutex<Held>>,
}

impl ClaimFence {
    /// A row this process just inserted: generation 0, active.
    pub(crate) fn created(&self, batch_id: &str) {
        self.activate(batch_id, 0);
    }

    /// A row this process just claimed, at the generation the claim returned.
    /// The highest generation seen wins: two same-process claims can return in
    /// either order, and a delayed older result must not replace the token the
    /// latest claimant holds (it would then have its valid writes refused).
    pub(crate) fn claimed(&self, batch_id: &str, generation: i64) {
        self.activate(batch_id, generation);
    }

    /// The generation held for `batch_id`, if any.
    pub(crate) fn held(&self, batch_id: &str) -> Option<i64> {
        self.lock().tokens.get(batch_id).map(|t| t.generation)
    }

    /// Record a successful transition to `to`.
    pub(crate) fn wrote(&self, batch_id: &str, to: BatchState) {
        match to {
            BatchState::SourceCommitted | BatchState::ReplayRequired => self.release(batch_id),
            BatchState::Failed => self.park(batch_id),
            _ => {}
        }
    }

    /// A write was refused because another engine claimed the batch. The
    /// token is kept - it is what refuses this process if it tries again.
    pub(crate) fn lost(&self, batch_id: &str) {
        self.park(batch_id);
    }

    fn release(&self, batch_id: &str) {
        let mut held = self.lock();
        if let Some(token) = held.tokens.remove(batch_id) {
            if token.parked.is_some() {
                held.parked -= 1;
            }
        }
        held.compact();
    }

    fn activate(&self, batch_id: &str, generation: i64) {
        let mut held = self.lock();
        let held = &mut *held;
        match held.tokens.get_mut(batch_id) {
            Some(token) => {
                token.generation = token.generation.max(generation);
                if token.parked.take().is_some() {
                    held.parked -= 1;
                }
            }
            None => {
                held.tokens.insert(
                    batch_id.to_owned(),
                    Token {
                        generation,
                        parked: None,
                    },
                );
            }
        }
        held.compact();
    }

    fn park(&self, batch_id: &str) {
        let mut held = self.lock();
        let held = &mut *held;
        let seq = held.next_seq;
        let Some(token) = held.tokens.get_mut(batch_id) else {
            return;
        };
        if token.parked.is_none() {
            held.parked += 1;
        }
        token.parked = Some(seq);
        held.next_seq += 1;
        held.parked_order.push_back((batch_id.to_owned(), seq));
        while held.parked > MAX_PARKED {
            let Some((oldest, seq)) = held.parked_order.pop_front() else {
                break;
            };
            if held.tokens.get(&oldest).and_then(|t| t.parked) == Some(seq) {
                held.tokens.remove(&oldest);
                held.parked -= 1;
            }
        }
        held.compact();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Held> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Held {
    /// Drop stale queue entries once they outnumber the live parked tokens,
    /// so a churn of park-then-release cannot grow the queue without bound.
    fn compact(&mut self) {
        if self.parked_order.len() > 2 * self.parked + 1024 {
            let Held {
                tokens,
                parked_order,
                ..
            } = self;
            parked_order.retain(|(id, seq)| tokens.get(id).and_then(|t| t.parked) == Some(*seq));
        }
    }
}

/// The error for a write to a batch this process holds no claim on: it never
/// created or claimed it, or its parked token was dropped at the bound.
pub(crate) fn not_held(batch_id: &str) -> VtopError {
    VtopError::State(format!(
        "batch {batch_id} is not claimed by this engine (it neither created nor claimed it, \
         or the claim was dropped); the write was refused rather than made unfenced"
    ))
}

/// The error for a write refused because the claim moved on.
pub(crate) fn claim_lost(batch_id: &str, held: i64, actual: i64) -> VtopError {
    VtopError::State(format!(
        "batch {batch_id} was claimed by another engine (claim generation {held} -> {actual}); \
         this engine's write was refused so it cannot overwrite the new owner's work"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_remembers_what_it_created_and_claimed_and_forgets_what_finished() {
        let fence = ClaimFence::default();
        assert_eq!(fence.held("b"), None, "nothing held before it is created");
        fence.created("b");
        assert_eq!(fence.held("b"), Some(0));
        fence.claimed("b", 3);
        assert_eq!(fence.held("b"), Some(3), "a claim replaces the creation");
        // A clone is the same map: the store is Clone and every handle shares it.
        let other = fence.clone();
        other.claimed("b", 4);
        assert_eq!(fence.held("b"), Some(4));
        fence.wrote("b", BatchState::SourceCommitted);
        assert_eq!(other.held("b"), None);
    }

    /// Two claims by one process can return out of order; the latest claimant
    /// must keep the highest generation (review of #569).
    #[test]
    fn a_delayed_older_claim_cannot_replace_the_latest_token() {
        let fence = ClaimFence::default();
        fence.claimed("b", 5);
        fence.claimed("b", 4);
        assert_eq!(fence.held("b"), Some(5));
        fence.claimed("b", 6);
        assert_eq!(fence.held("b"), Some(6));
    }

    /// Review of #569: recovery writes FAILED and then REPLAY_REQUIRED. The
    /// token must survive the first, or the second is written unfenced over
    /// an engine that claimed the row in between.
    #[test]
    fn a_failed_batch_keeps_its_token_until_it_is_handed_back() {
        let fence = ClaimFence::default();
        fence.claimed("b", 2);
        fence.wrote("b", BatchState::Failed);
        assert_eq!(
            fence.held("b"),
            Some(2),
            "FAILED is not the end of the work"
        );
        fence.wrote("b", BatchState::ReplayRequired);
        assert_eq!(fence.held("b"), None);
    }

    /// Review of #569: active work is never dropped, however much of it there
    /// is - only parked tokens count against the bound.
    #[test]
    fn active_tokens_are_never_evicted() {
        let fence = ClaimFence::default();
        for i in 0..(MAX_PARKED + 10) {
            fence.claimed(&format!("b{i}"), 1);
        }
        assert_eq!(fence.lock().tokens.len(), MAX_PARKED + 10);
        assert_eq!(fence.held("b0"), Some(1));
    }

    /// Parked tokens are bounded, oldest first; the newest stay fenced.
    #[test]
    fn parked_tokens_are_bounded_and_the_oldest_go_first() {
        let fence = ClaimFence::default();
        fence.claimed("active", 7);
        for i in 0..(MAX_PARKED + 10) {
            let id = format!("b{i}");
            fence.claimed(&id, 1);
            fence.lost(&id);
        }
        assert_eq!(fence.lock().parked, MAX_PARKED);
        assert_eq!(fence.held("b0"), None, "the oldest parked token went");
        assert_eq!(fence.held(&format!("b{}", MAX_PARKED + 9)), Some(1));
        assert_eq!(fence.held("active"), Some(7), "active work is untouched");
    }

    /// Review of #569: a queue entry left behind by a token that was since
    /// released and re-created must not evict the new token.
    #[test]
    fn a_stale_queue_entry_cannot_evict_a_token_that_came_back() {
        let fence = ClaimFence::default();
        fence.claimed("again", 1);
        fence.lost("again"); // queued at the front
        fence.claimed("again", 2); // re-claimed: active now, front entry stale
        for i in 0..MAX_PARKED {
            let id = format!("b{i}");
            fence.claimed(&id, 1);
            fence.lost(&id);
        }
        fence.claimed("x", 1);
        fence.lost("x"); // one over the bound: the front entry is popped
        assert_eq!(
            fence.held("again"),
            Some(2),
            "evicted by its own stale entry"
        );
        assert_eq!(fence.held("b0"), None, "the real oldest parked token went");
    }

    /// Park-then-release churn must not grow the queue without bound.
    #[test]
    fn churn_does_not_grow_the_queue() {
        let fence = ClaimFence::default();
        for i in 0..(3 * MAX_PARKED) {
            let id = format!("churn{i}");
            fence.created(&id);
            fence.wrote(&id, BatchState::Failed);
            fence.wrote(&id, BatchState::ReplayRequired);
        }
        let held = fence.lock();
        assert_eq!(held.parked, 0);
        assert!(
            held.parked_order.len() <= 1024 + 1,
            "{}",
            held.parked_order.len()
        );
    }

    #[test]
    fn the_refusals_name_the_batch() {
        let message = claim_lost("batch-1", 2, 5).to_string();
        for needle in ["batch-1", "2 -> 5", "claimed by another engine"] {
            assert!(message.contains(needle), "{message}");
        }
        let message = not_held("batch-2").to_string();
        for needle in ["batch-2", "not claimed by this engine", "refused"] {
            assert!(message.contains(needle), "{message}");
        }
    }
}

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
//! rows it claimed (the value the claim returned). A row it holds no
//! generation for — one an operator tool touches, or one an older build
//! created — is written without a generation check, exactly as before.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use vtop_core::errors::VtopError;

/// The most batches tracked at once. A batch is released when its work ends
/// (committed, failed, handed back for replay), so the map normally holds only
/// work in flight; this bounds the leftovers - a batch whose claim was LOST
/// keeps its token on purpose (see [`ClaimFence::release`]) and is never
/// written again. The oldest are dropped first.
const MAX_TRACKED: usize = 65_536;

#[derive(Default)]
struct Held {
    generation: HashMap<String, i64>,
    /// Insertion order, for eviction. May name batches already released.
    order: VecDeque<String>,
}

/// The claim generation this process holds, per batch.
#[derive(Clone, Default)]
pub(crate) struct ClaimFence {
    held: Arc<Mutex<Held>>,
}

impl ClaimFence {
    /// A row this process just inserted: generation 0, held.
    pub(crate) fn created(&self, batch_id: &str) {
        self.set(batch_id, 0);
    }

    /// A row this process just claimed, at the generation the claim returned.
    /// The highest generation seen wins: two same-process claims can return in
    /// either order, and a delayed older result must not replace the token the
    /// latest claimant holds (it would then have its valid writes refused).
    pub(crate) fn claimed(&self, batch_id: &str, generation: i64) {
        self.set(batch_id, generation);
    }

    /// The generation held for `batch_id`, if any.
    pub(crate) fn held(&self, batch_id: &str) -> Option<i64> {
        self.lock().generation.get(batch_id).copied()
    }

    /// Stop tracking a batch whose work ended on this process: committed,
    /// failed, or handed back for replay. NOT called when a write is refused
    /// because the claim moved on - that token is what keeps the stalled
    /// writer fenced if it tries again, so it stays until the bound evicts it.
    pub(crate) fn release(&self, batch_id: &str) {
        let mut held = self.lock();
        held.generation.remove(batch_id);
        // Keep the order list proportional to what is tracked.
        if held.order.len() > 2 * MAX_TRACKED {
            let Held { generation, order } = &mut *held;
            order.retain(|id| generation.contains_key(id));
        }
    }

    fn set(&self, batch_id: &str, generation: i64) {
        let mut held = self.lock();
        match held.generation.get_mut(batch_id) {
            Some(current) => *current = (*current).max(generation),
            None => {
                held.generation.insert(batch_id.to_owned(), generation);
                held.order.push_back(batch_id.to_owned());
            }
        }
        while held.generation.len() > MAX_TRACKED {
            let Some(oldest) = held.order.pop_front() else {
                break;
            };
            held.generation.remove(&oldest);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Held> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Whether a transition ends this process's work on a batch, so its token can
/// go: it is committed, failed, or handed back for replay. A failed batch is
/// picked up again only by a recovery claim, which sets a fresh token.
pub(crate) fn ends_work(to: vtop_core::state_machine::BatchState) -> bool {
    use vtop_core::state_machine::BatchState;
    matches!(
        to,
        BatchState::SourceCommitted | BatchState::Failed | BatchState::ReplayRequired
    )
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
        fence.release("b");
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

    /// Tokens that are never released (a lost claim keeps its token) must not
    /// grow without bound; the oldest go first and recent work stays fenced.
    #[test]
    fn the_map_is_bounded_and_evicts_the_oldest() {
        let fence = ClaimFence::default();
        for i in 0..(MAX_TRACKED + 10) {
            fence.claimed(&format!("b{i}"), 1);
        }
        assert_eq!(fence.lock().generation.len(), MAX_TRACKED);
        assert_eq!(fence.held("b0"), None, "the oldest was evicted");
        assert_eq!(fence.held(&format!("b{}", MAX_TRACKED + 9)), Some(1));
        // Releasing then re-adding does not let the order list grow unbounded.
        for i in 0..(3 * MAX_TRACKED) {
            let id = format!("churn{i}");
            fence.created(&id);
            fence.release(&id);
        }
        assert!(fence.lock().order.len() <= 2 * MAX_TRACKED + MAX_TRACKED);
    }

    #[test]
    fn the_refusal_names_the_batch_and_both_generations() {
        let message = claim_lost("batch-1", 2, 5).to_string();
        for needle in ["batch-1", "2 -> 5", "claimed by another engine"] {
            assert!(message.contains(needle), "{message}");
        }
    }
}

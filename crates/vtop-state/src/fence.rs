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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use vtop_core::errors::VtopError;

/// The claim generation this process holds, per batch.
#[derive(Clone, Default)]
pub(crate) struct ClaimFence {
    held: Arc<Mutex<HashMap<String, i64>>>,
}

impl ClaimFence {
    /// A row this process just inserted: generation 0, held.
    pub(crate) fn created(&self, batch_id: &str) {
        self.set(batch_id, 0);
    }

    /// A row this process just claimed, at the generation the claim returned.
    pub(crate) fn claimed(&self, batch_id: &str, generation: i64) {
        self.set(batch_id, generation);
    }

    /// The generation held for `batch_id`, if any.
    pub(crate) fn held(&self, batch_id: &str) -> Option<i64> {
        self.lock().get(batch_id).copied()
    }

    /// Stop tracking a batch that reached its terminal state, so the map holds
    /// only work in flight.
    pub(crate) fn release(&self, batch_id: &str) {
        self.lock().remove(batch_id);
    }

    fn set(&self, batch_id: &str, generation: i64) {
        self.lock().insert(batch_id.to_owned(), generation);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, i64>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
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

    #[test]
    fn the_refusal_names_the_batch_and_both_generations() {
        let message = claim_lost("batch-1", 2, 5).to_string();
        for needle in ["batch-1", "2 -> 5", "claimed by another engine"] {
            assert!(message.contains(needle), "{message}");
        }
    }
}

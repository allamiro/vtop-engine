//! Property tests for the core safety invariant:
//!
//! ```text
//! SOURCE_COMMITTED is forbidden until VERIFIED is true.
//! ```
//!
//! The engine's entire value proposition rests on this rule, so it is checked
//! here against *randomly generated* transition sequences rather than only the
//! hand-picked examples in the unit tests. If any of these fail, the protocol's
//! central guarantee is broken.

use proptest::prelude::*;
use proptest::sample::select;
use vtop_core::errors::VtopError;
use vtop_core::replay::{next_recovery_action, requires_replay_protection, RecoveryAction};
use vtop_core::state_machine::{can_transition, transition, BatchState};

const ALL_STATES: [BatchState; 11] = [
    BatchState::Discovered,
    BatchState::Batching,
    BatchState::Sealed,
    BatchState::Compressed,
    BatchState::Checksummed,
    BatchState::ObjectUploaded,
    BatchState::ManifestUploaded,
    BatchState::Verified,
    BatchState::SourceCommitted,
    BatchState::Failed,
    BatchState::ReplayRequired,
];

fn any_state() -> impl Strategy<Value = BatchState> {
    select(ALL_STATES.as_slice())
}

proptest! {
    /// THE invariant, stated directly: a commit is accepted from VERIFIED and
    /// from nowhere else, and the rejection is the precise error (not a generic
    /// illegal-transition), so callers can distinguish it.
    #[test]
    fn commit_is_reachable_only_from_verified(from in any_state()) {
        let res = transition(from, BatchState::SourceCommitted);
        if from == BatchState::Verified {
            prop_assert_eq!(res.unwrap(), BatchState::SourceCommitted);
        } else {
            prop_assert!(
                matches!(res, Err(VtopError::CommitBeforeVerified { actual }) if actual == from),
                "commit from {:?} must fail with CommitBeforeVerified, got {:?}",
                from,
                res
            );
        }
    }

    /// Random walk: drive a batch through arbitrary transition *attempts* and
    /// assert it can never end up committed without VERIFIED immediately
    /// preceding it. This is the sequence-level form of the invariant — the one
    /// that would catch a bug reachable only via an unusual path.
    #[test]
    fn random_walk_never_commits_without_verified(
        attempts in prop::collection::vec(any_state(), 1..60)
    ) {
        let mut state = BatchState::Discovered;
        let mut ever_verified = false;
        let mut prev_before_commit: Option<BatchState> = None;

        for target in attempts {
            if let Ok(next) = transition(state, target) {
                if next == BatchState::Verified {
                    ever_verified = true;
                }
                if next == BatchState::SourceCommitted {
                    prev_before_commit = Some(state);
                }
                state = next;
            }
            // Rejected transitions must leave the state untouched.
            if state == BatchState::SourceCommitted {
                prop_assert!(
                    ever_verified,
                    "reached SOURCE_COMMITTED without ever being VERIFIED"
                );
                prop_assert_eq!(
                    prev_before_commit,
                    Some(BatchState::Verified),
                    "SOURCE_COMMITTED was entered from a non-VERIFIED state"
                );
            }
        }
    }

    /// `transition` must agree with `can_transition`: no path may be legal in one
    /// and not the other, or the guard could be bypassed by calling the wrong one.
    #[test]
    fn transition_agrees_with_can_transition(from in any_state(), to in any_state()) {
        prop_assert_eq!(
            transition(from, to).is_ok(),
            can_transition(from, to),
            "transition/can_transition disagree for {:?} -> {:?}",
            from,
            to
        );
    }

    /// SOURCE_COMMITTED is terminal: once progress is committed nothing may move
    /// it, not even to FAILED (that would reopen a settled batch).
    #[test]
    fn source_committed_is_terminal(to in any_state()) {
        prop_assert!(
            !can_transition(BatchState::SourceCommitted, to),
            "SOURCE_COMMITTED must be terminal, but -> {:?} was allowed",
            to
        );
    }

    /// Anything that is not already committed may fail; a committed batch may not.
    #[test]
    fn failure_is_always_available_except_when_committed(from in any_state()) {
        prop_assert_eq!(
            can_transition(from, BatchState::Failed),
            from != BatchState::SourceCommitted
        );
    }

    /// Recovery must never hand back an action that advances source progress for
    /// unverified data: only a VERIFIED batch may retry its commit.
    #[test]
    fn recovery_never_commits_unverified(state in any_state()) {
        let action = next_recovery_action(state);
        if action == RecoveryAction::RetrySourceCommit {
            prop_assert_eq!(
                state,
                BatchState::Verified,
                "RetrySourceCommit offered for non-VERIFIED state {:?}",
                state
            );
        }
        // Replay protection is released only once progress is actually committed.
        prop_assert_eq!(
            requires_replay_protection(state),
            state != BatchState::SourceCommitted
        );
    }

    /// The happy path must remain walkable end to end — a guard that rejects
    /// everything would satisfy the invariant but break the engine.
    #[test]
    fn canonical_happy_path_is_walkable(_seed in 0u8..8) {
        let path = [
            BatchState::Batching,
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
            BatchState::SourceCommitted,
        ];
        let mut state = BatchState::Discovered;
        for next in path {
            state = transition(state, next).expect("canonical path must be legal");
        }
        prop_assert_eq!(state, BatchState::SourceCommitted);
    }
}

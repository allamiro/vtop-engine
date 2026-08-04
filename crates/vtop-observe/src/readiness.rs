//! Readiness as a first-class, observable fact.
//!
//! The live-chaos harness used to decide a node was up by grepping its stdout
//! for a ready marker. That works exactly until a scenario needs to know
//! *whether a node is still ready* — after a partition heals, after a leader is
//! killed, after a disk fills. A marker is a one-shot edge; readiness is a
//! level, and this is the level.
//!
//! The gate is deliberately a level with a *reason*: `/readyz` returning 503
//! with "metadata raft has no leader" tells an operator (and a scenario's
//! failure output) what to look at, where a bare 503 starts a bisect.

use std::sync::{Arc, RwLock};

/// Whether a process can serve right now, and if not, why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    /// Short, bounded, human-readable reason. It is written to an HTTP body and
    /// into scenario failure output, never used as a metric label.
    NotReady(String),
}

impl Readiness {
    /// Construct a not-ready state from anything string-like.
    pub fn not_ready(reason: impl Into<String>) -> Self {
        Readiness::NotReady(reason.into())
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Readiness::Ready)
    }

    /// Body served on `/readyz`.
    pub fn describe(&self) -> String {
        match self {
            Readiness::Ready => "ready".to_string(),
            Readiness::NotReady(reason) => format!("not ready: {reason}"),
        }
    }
}

/// A shared, cheaply-cloneable readiness level.
///
/// Starts NOT ready on purpose. A process that forgets to flip the gate reports
/// "not ready" forever, which is loud and obvious; the opposite default would
/// have a half-initialized node advertise itself as servable and send a load
/// balancer (or a chaos scenario) straight at it.
#[derive(Clone, Debug)]
pub struct ReadinessGate {
    state: Arc<RwLock<Readiness>>,
}

impl ReadinessGate {
    /// A gate that is not ready yet, carrying the reason it is still starting.
    pub fn starting(reason: impl Into<String>) -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::not_ready(reason))),
        }
    }

    /// A gate that is already ready — for processes with nothing to warm up.
    pub fn ready() -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::Ready)),
        }
    }

    pub fn mark_ready(&self) {
        self.set(Readiness::Ready);
    }

    pub fn mark_not_ready(&self, reason: impl Into<String>) {
        self.set(Readiness::not_ready(reason));
    }

    pub fn get(&self) -> Readiness {
        match self.state.read() {
            Ok(guard) => guard.clone(),
            // A poisoned lock means some other task panicked while holding it.
            // Reporting "not ready" is the safe read: the process is in an
            // unknown state, and claiming readiness there is how a damaged node
            // keeps taking traffic.
            Err(poisoned) => {
                let _ = poisoned;
                Readiness::not_ready("readiness state poisoned by a panicking task")
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        self.get().is_ready()
    }

    fn set(&self, next: Readiness) {
        match self.state.write() {
            Ok(mut guard) => *guard = next,
            Err(mut poisoned) => {
                // Recover deliberately rather than propagating a panic out of a
                // telemetry path: the gate holds no invariant beyond "latest
                // level wins", so the newest write is always the correct value.
                **poisoned.get_mut() = next;
                drop(poisoned);
                self.state.clear_poison();
            }
        }
    }
}

impl Default for ReadinessGate {
    fn default() -> Self {
        Self::starting("process is still starting")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_gate_is_not_ready_so_a_forgotten_flip_fails_closed() {
        let gate = ReadinessGate::default();
        assert!(!gate.is_ready());
        assert!(gate.get().describe().starts_with("not ready:"));
    }

    #[test]
    fn the_reason_survives_into_the_served_body() {
        let gate = ReadinessGate::starting("waiting for metadata leader");
        assert_eq!(
            gate.get().describe(),
            "not ready: waiting for metadata leader"
        );
    }

    #[test]
    fn readiness_is_a_level_not_an_edge() {
        let gate = ReadinessGate::starting("boot");
        gate.mark_ready();
        assert!(gate.is_ready());
        // The point of a level: a node that loses quorum goes back to not-ready
        // instead of staying ready because it once was.
        gate.mark_not_ready("lost metadata quorum");
        assert_eq!(
            gate.get(),
            Readiness::NotReady("lost metadata quorum".to_string())
        );
    }

    #[test]
    fn clones_share_one_level() {
        let gate = ReadinessGate::starting("boot");
        let observer = gate.clone();
        gate.mark_ready();
        assert!(
            observer.is_ready(),
            "the HTTP handler holds a clone; it must observe the process's flip"
        );
    }

    #[test]
    fn a_poisoned_gate_reads_as_not_ready() {
        let gate = ReadinessGate::ready();
        let poisoner = gate.clone();
        // Panic while holding the write lock, exactly as a crashing task would.
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.state.write().unwrap();
            panic!("task died holding the gate");
        })
        .join();
        assert!(
            !gate.is_ready(),
            "a process whose state is unknown must not advertise readiness"
        );
        // And a subsequent write recovers the gate rather than wedging it.
        gate.mark_ready();
        assert!(gate.is_ready());
    }
}

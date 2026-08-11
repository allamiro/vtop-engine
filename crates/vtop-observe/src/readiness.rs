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
    /// Startup marks still owed before [`Self::mark_ready`] opens the gate
    /// (see [`Self::require_marks`]). Distinct from the level itself: once
    /// the gate has opened, later flips are ordinary level writes.
    pending_marks: Arc<std::sync::atomic::AtomicUsize>,
}

impl ReadinessGate {
    /// A gate that is not ready yet, carrying the reason it is still starting.
    pub fn starting(reason: impl Into<String>) -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::not_ready(reason))),
            pending_marks: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        }
    }

    /// A gate that is already ready — for processes with nothing to warm up.
    pub fn ready() -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::Ready)),
            pending_marks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Declare that the gate opens only after `n` distinct [`Self::mark_ready`]
    /// calls — the conjunction a co-located process needs, where "ready" must
    /// mean EVERY role hosted in the process has finished starting, not
    /// whichever one won the race to flip a shared gate. Call before the
    /// components start.
    pub fn require_marks(&self, n: usize) {
        self.pending_marks
            .store(n.max(1), std::sync::atomic::Ordering::SeqCst);
    }

    /// Declare `n` MORE required marks on top of whatever is already owed —
    /// for a component wiring itself into a gate other components share.
    /// [`Self::require_marks`] states a total and so cannot compose: two
    /// callers each declaring their own count would overwrite each other,
    /// and the gate would open before the overwritten component finished
    /// starting (review). Startup wiring only, like `require_marks`: call
    /// before the components begin marking.
    pub fn add_required_marks(&self, n: usize) {
        self.pending_marks
            .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
    }

    /// One component finished starting. The gate opens once every required
    /// mark (default: one) has arrived.
    pub fn mark_ready(&self) {
        use std::sync::atomic::Ordering;
        let previous = self
            .pending_marks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |marks| {
                Some(marks.saturating_sub(1))
            })
            .unwrap_or(0);
        if previous <= 1 {
            self.set(Readiness::Ready);
        } else {
            self.set(Readiness::not_ready(format!(
                "waiting for {} more component(s) to finish starting",
                previous - 1
            )));
        }
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

    /// The conjunction a co-located process needs: with two required marks,
    /// the first role to finish starting must NOT open the gate — a load
    /// balancer routing on that half-ready signal reaches a process whose
    /// other plane has no listener yet.
    #[test]
    fn a_gate_requiring_two_marks_opens_only_on_the_second() {
        let gate = ReadinessGate::starting("boot");
        gate.require_marks(2);

        gate.mark_ready();
        assert!(
            !gate.is_ready(),
            "one of two roles is not a ready process: {}",
            gate.get().describe()
        );
        assert!(
            gate.get().describe().contains("1 more component"),
            "the reason must say what is still owed: {}",
            gate.get().describe()
        );

        gate.mark_ready();
        assert!(gate.is_ready(), "both roles up IS the conjunction");

        // Post-startup the gate is an ordinary level again.
        gate.mark_not_ready("lost quorum");
        assert!(!gate.is_ready());
        gate.mark_ready();
        assert!(gate.is_ready());
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

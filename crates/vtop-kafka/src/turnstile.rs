//! Arrival-ordered mutual exclusion (#457): what a fair mutex would be, made
//! of the unfair one the standard library has. One producer's appends must
//! reach the broker in the order the gateway received them — a mutex hands
//! its lock to waiters in no order, and a reordering the gateway introduced
//! is one the client never made and reads as fatal. A ticket is taken on
//! arrival and served in ticket order; the turn is a guard, so a holder that
//! panics passes it on like one that returns.

/// A turn being served. Dropping it passes the turn on.
pub(crate) struct Turn<'a> {
    turnstile: &'a Turnstile,
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.turnstile.leave();
    }
}

/// Arrival-ordered mutual exclusion for one producer's appends: tickets are
/// handed out in the order callers arrive, and a caller runs only when its
/// ticket is the one being served. What a fair mutex would be, made of the
/// unfair one the standard library has.
#[derive(Default)]
pub(crate) struct Turnstile {
    /// `(next ticket to hand out, ticket now being served)`.
    state: std::sync::Mutex<(u64, u64)>,
    turn: std::sync::Condvar,
}

impl Turnstile {
    /// A ticket, in arrival order. Taken under the caller's own lock (the
    /// producer map's, in the bridge), so arrival there is arrival here.
    pub(crate) fn enter(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let ticket = state.0;
        state.0 += 1;
        ticket
    }

    /// Blocks until `ticket` is the one being served, and holds the turn
    /// until the guard drops — on a panic as much as on a return (review),
    /// so a holder that dies does not strand every ticket behind it.
    pub(crate) fn wait_turn(&self, ticket: u64) -> Turn<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.1 != ticket {
            state = self
                .turn
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        Turn { turnstile: self }
    }

    /// The turn passes to the next ticket.
    fn leave(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.1 += 1;
        drop(state);
        self.turn.notify_all();
    }

    /// No ticket outstanding: every one handed out has been served.
    pub(crate) fn idle(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.0 == state.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The turnstile serves tickets in the order they were taken (review),
    /// which a mutex does not promise: three tickets taken in order, the
    /// later two waited on from threads started in the OTHER order, and the
    /// first released — they finish first-ticket-first, every time.
    #[test]
    fn a_turnstile_serves_tickets_in_arrival_order() {
        for _ in 0..50 {
            let turnstile = Arc::new(Turnstile::default());
            let first = turnstile.enter();
            let second = turnstile.enter();
            let third = turnstile.enter();
            assert_eq!((first, second, third), (0, 1, 2));
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut waiters = Vec::new();
            for ticket in [third, second] {
                let turnstile = Arc::clone(&turnstile);
                let order = Arc::clone(&order);
                waiters.push(std::thread::spawn(move || {
                    let _turn = turnstile.wait_turn(ticket);
                    order.lock().unwrap().push(ticket);
                }));
            }
            // Both are queued (or about to be) behind the first ticket, which
            // is being served and now leaves.
            std::thread::sleep(std::time::Duration::from_millis(2));
            assert!(!turnstile.idle());
            // The first ticket's holder dies mid-append (review): its turn
            // passes on with the guard, and the queue moves.
            let held = Arc::clone(&turnstile);
            let died = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _turn = Turn { turnstile: &held };
                panic!("mid-append");
            }));
            assert!(died.is_err());
            for waiter in waiters {
                waiter.join().unwrap();
            }
            assert_eq!(order.lock().unwrap().as_slice(), &[second, third]);
            assert!(turnstile.idle(), "every ticket served");
        }
    }
}

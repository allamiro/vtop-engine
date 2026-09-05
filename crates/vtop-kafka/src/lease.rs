//! Whether this node still holds the range it serves (#457 slice 2b).
//!
//! A Kafka listener outlives the authority behind it: a lease can be stolen
//! or lapse while the socket stays reachable and the process stays healthy.
//! Everything the gateway says on behalf of the range — that it leads the
//! partition, that it coordinates the groups on it, that it takes their
//! committed offsets — is only true while the lease is. This is the seam
//! where the gateway asks.

/// The range's lease as the node's broker holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// This node holds the range at this fencing epoch.
    Held(u64),
    /// It does not: never granted, released, or fenced by a newer holder.
    Gone,
    /// Not knowable right now without waiting — the broker's own view is
    /// busy (review). Distinct from `Gone` on purpose: a produce holding the
    /// broker's lock through its fsync is not a lost lease, so a commit is
    /// answered as retryable and the gateway keeps serving; only evidence
    /// that the lease is GONE makes it stop claiming the range.
    Unknown,
}

/// The range's lease, read without blocking (review): a view that cannot
/// answer at once answers [`LeaseState::Unknown`] rather than waiting on the
/// broker's lock, which a produce holds through its fsync.
pub trait LeaseView: Send + Sync + 'static {
    fn lease(&self) -> LeaseState;
}

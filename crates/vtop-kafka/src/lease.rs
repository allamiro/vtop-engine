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

/// The node that holds the range now, as this node's view of the lease names
/// it (#751).
///
/// A candidate that does not hold the range still answers Metadata, and a
/// client that asked it needs the holder to go to: a client told only about a
/// dead broker has nowhere to go. `broker_id` is the holder's Kafka broker id,
/// already mapped from whatever the lease names it by; `epoch` is the
/// fencing epoch it holds the range at, which Metadata hands out as the
/// partition's leader epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeHolder {
    pub broker_id: i32,
    pub epoch: u64,
}

/// Who holds the range, read without blocking (#751).
///
/// `None` is "not known right now": the lease has no holder yet, the view
/// has not read one, or the holder is a node with no Kafka endpoint. It is
/// never a guess: a gateway that names a holder the client cannot reach
/// strands the client, and one that names itself while it does not hold the
/// range sends the client back to the node that refused it.
pub trait HolderView: Send + Sync + 'static {
    fn holder(&self) -> Option<RangeHolder>;
}

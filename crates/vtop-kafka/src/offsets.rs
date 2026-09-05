//! Where a group's committed offsets live (#457, slice 2).
//!
//! The coordinator keeps membership in memory, because membership is
//! ephemeral by design; a committed offset is not, and this trait is the
//! seam where it becomes durable. The metadata plane's lineage-bound cursor
//! implements it where a node has one; [`MemoryOffsetStore`] is the tests'
//! and a lab's, never a deployment's — a listener built without a store
//! refuses OffsetCommit by name rather than remembering anything it would
//! forget, and answers OffsetFetch with what is committed anywhere: nothing.
//! A store refuses a negative offset: -1 is the wire's "nothing committed",
//! and a position below zero is not one a consumer can resume from.

use crate::messages::ErrorCode;
use std::collections::HashMap;
use std::sync::Mutex;

/// A committed position and the metadata the client attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    pub offset: i64,
    pub metadata: Option<String>,
}

#[async_trait::async_trait]
pub trait OffsetStore: Send + Sync + 'static {
    /// Durably records `offset` — the NEXT offset the group will consume from
    /// `topic`'s partition — for `group`. A negative offset is refused
    /// (`OffsetOutOfRange`): the listener refuses it before the store sees
    /// it, and the store holds the same line for any other caller.
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        committed: Committed,
    ) -> Result<(), ErrorCode>;

    /// What `group` last committed for `topic`'s partition, if anything.
    async fn fetch(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Committed>, ErrorCode>;

    /// Every partition `group` has committed, with what it committed — what
    /// an OffsetFetch naming no topics asks for (review): the group's
    /// commits, not the topics currently served.
    async fn committed(&self, group: &str) -> Result<Vec<(String, i32, Committed)>, ErrorCode>;
}

/// An in-memory store: the tests' and a lab's.
#[derive(Default)]
pub struct MemoryOffsetStore {
    committed: Mutex<HashMap<(String, String, i32), Committed>>,
}

#[async_trait::async_trait]
impl OffsetStore for MemoryOffsetStore {
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        committed: Committed,
    ) -> Result<(), ErrorCode> {
        if committed.offset < 0 {
            return Err(ErrorCode::OffsetOutOfRange);
        }
        self.committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((group.to_owned(), topic.to_owned(), partition), committed);
        Ok(())
    }

    async fn fetch(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Committed>, ErrorCode> {
        Ok(self
            .committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(group.to_owned(), topic.to_owned(), partition))
            .cloned())
    }

    async fn committed(&self, group: &str) -> Result<Vec<(String, i32, Committed)>, ErrorCode> {
        let mut rows: Vec<(String, i32, Committed)> = self
            .committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|((g, _, _), _)| g == group)
            .map(|((_, topic, partition), committed)| {
                (topic.clone(), *partition, committed.clone())
            })
            .collect();
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        Ok(rows)
    }
}

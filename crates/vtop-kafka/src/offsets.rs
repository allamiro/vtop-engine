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
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

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
    /// commits, not the topics currently served. At most `at_most + 1` rows
    /// (review): the caller says how many it can carry, and one row over is
    /// how the store says the group has committed more than that — nothing
    /// beyond is built.
    async fn committed(
        &self,
        group: &str,
        at_most: usize,
    ) -> Result<Vec<(String, i32, Committed)>, ErrorCode>;
}

/// An in-memory store: the tests' and a lab's. Rows are kept ordered by
/// (group, topic, partition) so a group's rows are a contiguous walk
/// (review): `committed` stops at one over the caller's bound without
/// visiting, cloning or sorting the rest of the group's history.
#[derive(Default)]
pub struct MemoryOffsetStore {
    committed: Mutex<BTreeMap<(String, String, i32), Committed>>,
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
        // Names the wire cannot carry name nothing (review): the listener
        // decoded its names within the bound, and the store holds it for any
        // other caller, so nothing it returns can exceed a STRING field.
        if group.len() > crate::wire::MAX_STRING_BYTES {
            return Err(ErrorCode::InvalidGroupId);
        }
        if topic.len() > crate::wire::MAX_STRING_BYTES {
            return Err(ErrorCode::UnknownTopicOrPartition);
        }
        if committed
            .metadata
            .as_ref()
            .is_some_and(|m| m.len() > crate::api_groups::MAX_OFFSET_METADATA_BYTES)
        {
            // The listener refuses it first; the store holds the same line for
            // any other caller (review), so nothing it returns can exceed what
            // the wire carries.
            return Err(ErrorCode::OffsetMetadataTooLarge);
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

    async fn committed(
        &self,
        group: &str,
        at_most: usize,
    ) -> Result<Vec<(String, i32, Committed)>, ErrorCode> {
        let rows = self
            .committed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The group's rows begin at the smallest key naming it, and end where
        // the group name changes; the walk stops one over the bound.
        Ok(rows
            .range((group.to_owned(), String::new(), i32::MIN)..)
            .take_while(|((g, _, _), _)| g == group)
            .take(at_most.saturating_add(1))
            .map(|((_, topic, partition), committed)| {
                (topic.clone(), *partition, committed.clone())
            })
            .collect())
    }
}

/// Topics the metadata-plane store does not name (a `backend: kafka` catalog
/// route) have no range cursor on this node. Commits for those names are
/// refused rather than remembered in this process: a successful OffsetCommit
/// that lived only here would vanish on restart or coordinator move.
pub struct OverlayOffsetStore {
    inner: Arc<dyn OffsetStore>,
    extra_topics: HashSet<String>,
}

impl OverlayOffsetStore {
    /// `owned` names keep their inner (durable) cursor. Remaining `extra`
    /// names are kafka-only: OffsetCommit is `UNKNOWN_TOPIC_OR_PARTITION`.
    pub fn wrapping(
        inner: Arc<dyn OffsetStore>,
        extra_topics: Vec<String>,
        owned: impl IntoIterator<Item = String>,
    ) -> Arc<dyn OffsetStore> {
        let owned: HashSet<String> = owned.into_iter().collect();
        let extra_topics: HashSet<String> = extra_topics
            .into_iter()
            .filter(|name| !owned.contains(name))
            .collect();
        if extra_topics.is_empty() {
            return inner;
        }
        Arc::new(Self {
            inner,
            extra_topics,
        })
    }

    fn is_extra(&self, topic: &str) -> bool {
        self.extra_topics.contains(topic)
    }
}

#[async_trait::async_trait]
impl OffsetStore for OverlayOffsetStore {
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        committed: Committed,
    ) -> Result<(), ErrorCode> {
        if self.is_extra(topic) {
            tracing::error!(
                topic,
                "kafka-only catalog name has no range cursor; OffsetCommit is refused rather than remembered in this process"
            );
            Err(ErrorCode::UnknownTopicOrPartition)
        } else {
            self.inner.commit(group, topic, partition, committed).await
        }
    }

    async fn fetch(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Committed>, ErrorCode> {
        if self.is_extra(topic) {
            Err(ErrorCode::UnknownTopicOrPartition)
        } else {
            self.inner.fetch(group, topic, partition).await
        }
    }

    async fn committed(
        &self,
        group: &str,
        at_most: usize,
    ) -> Result<Vec<(String, i32, Committed)>, ErrorCode> {
        self.inner.committed(group, at_most).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The store holds the commit path's lines itself (review): a negative
    /// offset and metadata over the cap are refused whoever the caller is.
    #[tokio::test]
    async fn the_store_holds_the_commit_paths_lines() {
        let store = MemoryOffsetStore::default();
        assert_eq!(
            store
                .commit(
                    "g",
                    "t",
                    0,
                    Committed {
                        offset: -1,
                        metadata: None,
                    },
                )
                .await,
            Err(ErrorCode::OffsetOutOfRange)
        );
        assert_eq!(
            store
                .commit(
                    "g",
                    "t",
                    0,
                    Committed {
                        offset: 1,
                        metadata: Some(
                            "x".repeat(crate::api_groups::MAX_OFFSET_METADATA_BYTES + 1)
                        ),
                    },
                )
                .await,
            Err(ErrorCode::OffsetMetadataTooLarge)
        );
        assert!(store.fetch("g", "t", 0).await.unwrap().is_none());
        let long = "n".repeat(crate::wire::MAX_STRING_BYTES + 1);
        let row = Committed {
            offset: 1,
            metadata: None,
        };
        assert_eq!(
            store.commit("g", &long, 0, row.clone()).await,
            Err(ErrorCode::UnknownTopicOrPartition),
            "a topic name the wire cannot carry"
        );
        assert_eq!(
            store.commit(&long, "t", 0, row).await,
            Err(ErrorCode::InvalidGroupId),
            "a group name the wire cannot carry"
        );
    }

    /// The walk stops at one over the bound (review): of a group's many
    /// rows, `at_most + 1` come back, in order, and another group's never.
    #[tokio::test]
    async fn a_stores_walk_stops_at_one_over_the_bound() {
        let store = MemoryOffsetStore::default();
        for (group, topic) in [("g", "b"), ("g", "a"), ("g", "c"), ("g", "d"), ("h", "a")] {
            store
                .commit(
                    group,
                    topic,
                    0,
                    Committed {
                        offset: 1,
                        metadata: None,
                    },
                )
                .await
                .unwrap();
        }
        let rows = store.committed("g", 2).await.unwrap();
        let topics: Vec<&str> = rows.iter().map(|(t, _, _)| t.as_str()).collect();
        assert_eq!(topics, vec!["a", "b", "c"], "one over the bound, in order");
        assert_eq!(store.committed("g", 10).await.unwrap().len(), 4);
        assert_eq!(store.committed("h", 10).await.unwrap().len(), 1);
        assert!(store.committed("none", 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn overlay_keeps_extra_topics_off_the_inner_store() {
        let inner = Arc::new(MemoryOffsetStore::default());
        let store = OverlayOffsetStore::wrapping(
            Arc::clone(&inner) as Arc<dyn OffsetStore>,
            vec!["legacy".to_owned()],
            Vec::new(),
        );
        assert_eq!(
            store
                .commit(
                    "g",
                    "legacy",
                    0,
                    Committed {
                        offset: 9,
                        metadata: None,
                    },
                )
                .await,
            Err(ErrorCode::UnknownTopicOrPartition),
            "a kafka-only name is refused rather than remembered here"
        );
        assert_eq!(
            inner.fetch("g", "legacy", 0).await.unwrap(),
            None,
            "a kafka-only name is not this range's cursor"
        );
    }

    #[tokio::test]
    async fn overlay_does_not_steal_names_the_inner_store_owns() {
        let inner = Arc::new(MemoryOffsetStore::default());
        let store = OverlayOffsetStore::wrapping(
            Arc::clone(&inner) as Arc<dyn OffsetStore>,
            vec!["events".to_owned()],
            vec!["events".to_owned()],
        );
        store
            .commit(
                "g",
                "events",
                0,
                Committed {
                    offset: 4,
                    metadata: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            inner.fetch("g", "events", 0).await.unwrap().unwrap().offset,
            4,
            "an owned name keeps the durable inner cursor"
        );
    }
}

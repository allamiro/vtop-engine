//! Per-topic virtualization (#458): which backend answers a Kafka topic name.
//!
//! Phase 1 had one [`Bridge`] for every name. A topic served by the gateway
//! is now backed by the backend the map names — native, an external cluster,
//! or a dual-write/shadow-read pair — and a name in no backend is
//! `UNKNOWN_TOPIC_OR_PARTITION` by name, never a guess at another broker.
//! Today's constructor still builds a single-backend map, so a deployment
//! that never sets `kafka.topics` is unchanged: Metadata is whatever that
//! backend enumerates, and a produce of an unknown name is the backend's
//! own refusal.

use crate::bridge::Bridge;
use std::collections::HashMap;
use std::sync::Arc;

/// One Kafka name and the backend that answers it.
#[derive(Clone)]
pub struct TopicBinding {
    /// The name a Kafka client produces and consumes.
    pub name: String,
    pub backend: Arc<dyn Bridge>,
    /// The name that backend knows the log as, if the Kafka name is a
    /// virtual one. Equal to `name` when they coincide.
    pub backend_topic: String,
}

/// The backend that answers `kafka_topic`, and the name to use on it.
#[derive(Clone)]
pub struct Resolved {
    pub backend: Arc<dyn Bridge>,
    pub backend_topic: String,
}

#[derive(Clone)]
enum TopicMapKind {
    /// Today's shape: one backend, Metadata is whatever it enumerates, and
    /// every name is handed to it — unknown included, so the backend's own
    /// `UNKNOWN_TOPIC_OR_PARTITION` is what a client sees, as it did before.
    Single(Arc<dyn Bridge>),
    /// Named routes: Metadata is the catalog, a name not in it is unknown
    /// before any backend is asked. Two routes may share a backend process
    /// only when they name different backend logs; two Kafka names of one
    /// log would share one producer-sequence space.
    Routed {
        order: Vec<String>,
        by_name: HashMap<String, Binding>,
    },
}

#[derive(Clone)]
struct Binding {
    backend: Arc<dyn Bridge>,
    backend_topic: String,
}

/// Which backend answers a Kafka topic name (#458 slice 1).
#[derive(Clone)]
pub struct TopicMap {
    kind: TopicMapKind,
}

impl TopicMap {
    /// One backend for every name, which is every deployment that never
    /// sets a topic map.
    pub fn single(backend: Arc<dyn Bridge>) -> Self {
        Self {
            kind: TopicMapKind::Single(backend),
        }
    }

    /// Named routes. A topic named twice, a topic with no name, a route
    /// whose backend topic is empty, or a map with no routes at all is
    /// refused before the listener binds — an empty catalog would advertise
    /// nothing and a duplicate would make Metadata's union a coin toss.
    pub fn routed(entries: Vec<TopicBinding>) -> Result<Self, String> {
        if entries.is_empty() {
            return Err(
                "kafka topic map is empty: name at least one topic, or omit the map for the \
                 single native topic"
                    .to_owned(),
            );
        }
        let mut order = Vec::with_capacity(entries.len());
        let mut by_name: HashMap<String, Binding> = HashMap::with_capacity(entries.len());
        for entry in entries {
            if entry.name.is_empty() {
                return Err(
                    "kafka topic map names a topic with no name: every route needs a Kafka name"
                        .to_owned(),
                );
            }
            if entry.backend_topic.is_empty() {
                return Err(format!(
                    "kafka topic map gives {name:?} an empty backend topic: the backend has \
                     nothing to serve under that name",
                    name = entry.name
                ));
            }
            if by_name.contains_key(&entry.name) {
                return Err(format!(
                    "kafka topic map names {:?} twice: one name is one backend",
                    entry.name
                ));
            }
            if let Some((other, _)) = by_name.iter().find(|(_, binding)| {
                Arc::ptr_eq(&binding.backend, &entry.backend)
                    && binding.backend_topic == entry.backend_topic
            }) {
                return Err(format!(
                    "kafka topic map gives {other:?} and {:?} the same backend log: idempotent \
                     producers keep a sequence space per Kafka name, and this backend has one; \
                     give each name its own log, or wait until sequences are namespaced",
                    entry.name
                ));
            }
            order.push(entry.name.clone());
            by_name.insert(
                entry.name,
                Binding {
                    backend: entry.backend,
                    backend_topic: entry.backend_topic,
                },
            );
        }
        Ok(Self {
            kind: TopicMapKind::Routed { order, by_name },
        })
    }

    /// The backend that answers `kafka_topic`. `None` only on a routed map
    /// for a name nobody bound — a single-backend map always resolves, and
    /// the backend then refuses names it does not serve.
    pub fn resolve(&self, kafka_topic: &str) -> Option<Resolved> {
        match &self.kind {
            TopicMapKind::Single(backend) => Some(Resolved {
                backend: Arc::clone(backend),
                backend_topic: kafka_topic.to_owned(),
            }),
            TopicMapKind::Routed { by_name, .. } => {
                by_name.get(kafka_topic).map(|binding| Resolved {
                    backend: Arc::clone(&binding.backend),
                    backend_topic: binding.backend_topic.clone(),
                })
            }
        }
    }

    /// Names Metadata lists when the client named none: the backend's own
    /// enumeration on a single-backend map, the catalog on a routed one.
    pub fn names(&self) -> Vec<String> {
        match &self.kind {
            TopicMapKind::Single(backend) => backend.topics(),
            TopicMapKind::Routed { order, .. } => order.clone(),
        }
    }

    /// Whether this map would resolve `kafka_topic` without asking a backend.
    pub fn contains(&self, kafka_topic: &str) -> bool {
        match &self.kind {
            TopicMapKind::Single(_) => true,
            TopicMapKind::Routed { by_name, .. } => by_name.contains_key(kafka_topic),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::messages::ErrorCode;

    fn mem(topics: &[&str]) -> Arc<dyn Bridge> {
        Arc::new(MemoryBridge::with_topics(topics.iter().copied()))
    }

    #[test]
    fn a_single_backend_enumerates_and_resolves_every_name() {
        let map = TopicMap::single(mem(&["events"]));
        assert_eq!(map.names(), vec!["events".to_owned()]);
        assert!(map.contains("nope"), "the backend, not the map, refuses it");
        let resolved = map.resolve("nope").expect("single always resolves");
        assert_eq!(resolved.backend_topic, "nope");
        assert_eq!(
            resolved.backend.bounds("nope"),
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(
            map.resolve("events").unwrap().backend.bounds("events"),
            Ok((0, 0))
        );
    }

    #[test]
    fn a_routed_map_is_the_catalog_and_an_unmapped_name_is_unknown() {
        let native = mem(&["events.v1"]);
        let other = mem(&["legacy"]);
        let map = TopicMap::routed(vec![
            TopicBinding {
                name: "events".to_owned(),
                backend: Arc::clone(&native),
                backend_topic: "events.v1".to_owned(),
            },
            TopicBinding {
                name: "legacy".to_owned(),
                backend: other,
                backend_topic: "legacy".to_owned(),
            },
        ])
        .unwrap();
        assert_eq!(
            map.names(),
            vec!["events".to_owned(), "legacy".to_owned()],
            "catalog order, not the backends' names"
        );
        assert!(!map.contains("nope"));
        assert!(map.resolve("nope").is_none());
        let events = map.resolve("events").unwrap();
        assert_eq!(events.backend_topic, "events.v1");
        assert_eq!(events.backend.bounds("events.v1"), Ok((0, 0)));
        assert_eq!(
            events.backend.bounds("events"),
            Err(ErrorCode::UnknownTopicOrPartition),
            "the native log is not named by the Kafka name"
        );
    }

    #[test]
    fn routed_refuses_an_empty_map_a_blank_name_a_blank_backend_topic_and_a_duplicate() {
        let backend = mem(&["events"]);
        assert!(TopicMap::routed(Vec::new())
            .err()
            .expect("empty")
            .contains("empty"));
        assert!(TopicMap::routed(vec![TopicBinding {
            name: String::new(),
            backend: Arc::clone(&backend),
            backend_topic: "events".to_owned(),
        }])
        .err()
        .expect("blank name")
        .contains("no name"));
        assert!(TopicMap::routed(vec![TopicBinding {
            name: "events".to_owned(),
            backend: Arc::clone(&backend),
            backend_topic: String::new(),
        }])
        .err()
        .expect("blank backend")
        .contains("empty backend topic"));
        assert!(TopicMap::routed(vec![
            TopicBinding {
                name: "events".to_owned(),
                backend: Arc::clone(&backend),
                backend_topic: "events".to_owned(),
            },
            TopicBinding {
                name: "events".to_owned(),
                backend: Arc::clone(&backend),
                backend_topic: "other".to_owned(),
            },
        ])
        .err()
        .expect("duplicate")
        .contains("twice"));
        assert!(TopicMap::routed(vec![
            TopicBinding {
                name: "events".to_owned(),
                backend: Arc::clone(&backend),
                backend_topic: "events".to_owned(),
            },
            TopicBinding {
                name: "alias".to_owned(),
                backend: Arc::clone(&backend),
                backend_topic: "events".to_owned(),
            },
        ])
        .err()
        .expect("shared log")
        .contains("same backend log"));
    }
}

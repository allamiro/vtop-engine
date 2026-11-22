//! A Prometheus registry that can say what is registered in it.

use prometheus::core::{Collector, Desc};
use prometheus::proto::{MetricFamily, MetricType};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

/// A Prometheus registry that also keeps what was registered in it.
///
/// `prometheus::Registry` cannot list its collectors, and its `gather` leaves
/// out a labelled family that has no series yet, so neither the registry nor a
/// scrape can say which families a process exports. This registers every
/// collector into an ordinary `prometheus::Registry`, exactly as before, and
/// keeps a shared handle on it, so [`Registry::families`] can describe every
/// family whether or not it has a series. Scrapes are unchanged: [`gather`]
/// is the inner registry's.
///
/// It has the same constructor and `register` as `prometheus::Registry` on
/// purpose. Code that builds a registry and registers into it compiles
/// unchanged once it names this type, and a family added there is in the
/// surface snapshot without any edit to the snapshot code.
///
/// [`gather`]: Registry::gather
#[derive(Clone)]
pub struct Registry {
    inner: prometheus::Registry,
    prefix: Option<String>,
    const_labels: Vec<String>,
    collectors: Arc<Mutex<Vec<Arc<dyn Collector>>>>,
}

/// One exported metric family, as a dashboard or an alert sees it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Family {
    /// The exported name, with the registry's prefix (`vtop_...`). A
    /// histogram is listed by its base name, not its `_bucket`, `_sum` and
    /// `_count` series.
    pub name: String,
    /// `counter`, `gauge`, `histogram`, `summary` or `untyped`.
    pub kind: &'static str,
    /// Every label name a series of the family carries, sorted.
    pub labels: Vec<String>,
}

impl Family {
    /// The snapshot line: `name kind label1,label2`, or `name kind` for a
    /// family with no labels.
    pub fn line(&self) -> String {
        if self.labels.is_empty() {
            format!("{} {}", self.name, self.kind)
        } else {
            format!("{} {} {}", self.name, self.kind, self.labels.join(","))
        }
    }
}

/// Registered into the inner registry in place of the collector itself, so
/// the same collector can also be kept here. Scrapes go straight through.
struct Shared(Arc<dyn Collector>);

impl Collector for Shared {
    fn desc(&self) -> Vec<&Desc> {
        self.0.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: prometheus::Registry::new(),
            prefix: None,
            const_labels: Vec::new(),
            collectors: Arc::default(),
        }
    }

    /// As `prometheus::Registry::new_custom`.
    pub fn new_custom(
        prefix: Option<String>,
        labels: Option<HashMap<String, String>>,
    ) -> prometheus::Result<Self> {
        let const_labels = labels
            .as_ref()
            .map(|labels| labels.keys().cloned().collect())
            .unwrap_or_default();
        Ok(Self {
            inner: prometheus::Registry::new_custom(prefix.clone(), labels)?,
            prefix,
            const_labels,
            collectors: Arc::default(),
        })
    }

    /// As `prometheus::Registry::register`, including its `AlreadyReg` error
    /// for a collector that is already registered.
    pub fn register(&self, collector: Box<dyn Collector>) -> prometheus::Result<()> {
        let shared: Arc<dyn Collector> = Arc::from(collector);
        self.inner.register(Box::new(Shared(Arc::clone(&shared))))?;
        self.collectors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(shared);
        Ok(())
    }

    /// As `prometheus::Registry::gather`.
    pub fn gather(&self) -> Vec<MetricFamily> {
        self.inner.gather()
    }

    /// The registry underneath, for code that takes a `prometheus::Registry`
    /// to read from. Register through [`Registry::register`], never through
    /// this: a collector registered here is served but left out of
    /// [`Registry::families`].
    pub fn prometheus(&self) -> &prometheus::Registry {
        &self.inner
    }

    /// Every family registered here, sorted by name.
    ///
    /// The name and labels come from each collector's descriptors; the type
    /// from what the collector emits, since a descriptor does not carry it.
    /// This calls `collect` on every collector, as a scrape does; it is meant
    /// for the snapshot tests, not for a serving process.
    pub fn families(&self) -> Result<Vec<Family>, String> {
        let collectors = self
            .collectors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut families = BTreeMap::new();
        for collector in collectors {
            let collected = collector.collect();
            for desc in collector.desc() {
                let kind = collected
                    .iter()
                    .find(|family| family.name() == desc.fq_name)
                    .map(|family| kind_name(family.get_field_type()))
                    .ok_or_else(|| {
                        format!(
                            "{} is described by its collector but the collector emitted no \
                             family of that name, so its type is unknown: a collector must \
                             emit every family it describes, with or without series",
                            desc.fq_name
                        )
                    })?;
                let name = match &self.prefix {
                    Some(prefix) => format!("{prefix}_{}", desc.fq_name),
                    None => desc.fq_name.clone(),
                };
                let labels: BTreeSet<String> = desc
                    .variable_labels
                    .iter()
                    .cloned()
                    .chain(
                        desc.const_label_pairs
                            .iter()
                            .map(|pair| pair.name().to_string()),
                    )
                    .chain(self.const_labels.iter().cloned())
                    .collect();
                families.insert(
                    name.clone(),
                    Family {
                        name,
                        kind,
                        labels: labels.into_iter().collect(),
                    },
                );
            }
        }
        Ok(families.into_values().collect())
    }
}

fn kind_name(kind: MetricType) -> &'static str {
    match kind {
        MetricType::COUNTER => "counter",
        MetricType::GAUGE => "gauge",
        MetricType::HISTOGRAM => "histogram",
        MetricType::SUMMARY => "summary",
        MetricType::UNTYPED => "untyped",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntGaugeVec, Opts};

    fn registry() -> Registry {
        Registry::new_custom(Some("vtop".into()), None).unwrap()
    }

    /// The reason this type exists: a labelled family with no series is
    /// absent from a scrape, and must still be in the surface.
    #[test]
    fn a_labelled_family_with_no_series_is_listed_though_a_scrape_omits_it() {
        let registry = registry();
        let idle =
            IntGaugeVec::new(Opts::new("idle", "no series yet"), &["state", "role"]).unwrap();
        registry.register(Box::new(idle)).unwrap();
        assert!(
            registry.gather().is_empty(),
            "the premise: gather leaves the family out"
        );
        assert_eq!(
            registry.families().unwrap(),
            vec![Family {
                name: "vtop_idle".into(),
                kind: "gauge",
                labels: vec!["role".into(), "state".into()],
            }]
        );
    }

    #[test]
    fn each_family_has_its_type_and_its_line() {
        let registry = registry();
        registry
            .register(Box::new(
                IntCounter::with_opts(Opts::new("a_total", "a")).unwrap(),
            ))
            .unwrap();
        registry
            .register(Box::new(
                HistogramVec::new(HistogramOpts::new("b_seconds", "b"), &["stage"]).unwrap(),
            ))
            .unwrap();
        let lines: Vec<String> = registry
            .families()
            .unwrap()
            .iter()
            .map(Family::line)
            .collect();
        assert_eq!(
            lines,
            vec!["vtop_a_total counter", "vtop_b_seconds histogram stage"]
        );
    }

    /// Scrapes are what they were: the same text as a plain registry.
    #[test]
    fn gather_is_the_inner_registrys() {
        let ours = registry();
        let plain = prometheus::Registry::new_custom(Some("vtop".into()), None).unwrap();
        let counter = IntCounter::with_opts(Opts::new("c_total", "c")).unwrap();
        counter.inc_by(3);
        ours.register(Box::new(counter.clone())).unwrap();
        plain.register(Box::new(counter)).unwrap();
        assert_eq!(ours.gather(), plain.gather());
    }

    /// Registering twice keeps prometheus's answer, and lists the family once.
    #[test]
    fn a_second_registration_is_refused_as_before() {
        let registry = registry();
        let counter = IntCounter::with_opts(Opts::new("d_total", "d")).unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        assert!(matches!(
            registry.register(Box::new(counter)),
            Err(prometheus::Error::AlreadyReg)
        ));
        assert_eq!(registry.families().unwrap().len(), 1);
    }

    #[test]
    fn registry_wide_labels_are_on_every_family() {
        let registry = Registry::new_custom(
            Some("vtop".into()),
            Some(HashMap::from([("cluster".to_string(), "a".to_string())])),
        )
        .unwrap();
        registry
            .register(Box::new(
                IntCounter::with_opts(Opts::new("e_total", "e")).unwrap(),
            ))
            .unwrap();
        assert_eq!(
            registry.families().unwrap()[0].line(),
            "vtop_e_total counter cluster"
        );
    }
}

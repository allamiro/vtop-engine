//! One process hosting both a metadata voter and a data-plane replica (#215).
//!
//! Until now a live cluster meant six processes for three machines: a `meta`
//! and a `data` invocation each, with separate configs, separate ready markers,
//! and separate `/metrics` endpoints. That is not how anyone deploys this, and
//! the gap mattered for more than tidiness — it meant the harness never
//! exercised the two planes sharing a process, a runtime, and a fate.
//!
//! # What co-location actually changes
//!
//! **One observability surface.** An operator scraping a host finds one target,
//! not two, and does not have to know which roles happen to share it. Both
//! roles register their collectors into the same registry, and readiness is the
//! conjunction: the process is ready when both roles are.
//!
//! **Shared fate, made explicit.** If either role stops, the process stops. A
//! metadata voter that has died inside a process still serving data is worse
//! than a dead process: the cluster keeps counting it toward quorum while it
//! answers nothing. Exiting makes the failure legible to whatever supervises
//! the node.
//!
//! # What it does not change
//!
//! The two roles remain independent at the protocol level. The data plane
//! reaches metadata through the admin endpoint exactly as it would across a
//! network, including when that endpoint is this same process — there is no
//! in-memory shortcut. A shortcut would make the co-located path diverge from
//! the distributed one precisely where the harness is meant to prove they
//! agree.

use crate::config::{DataNodeConfig, MetaNodeConfig, ObservabilityConfig};
use crate::observe::NodeObservability;
use crate::{data_node, meta_node};
use serde::Deserialize;

/// A node that is both a metadata voter and a data-plane replica.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColocatedNodeConfig {
    pub meta: MetaNodeConfig,
    pub data: DataNodeConfig,
    /// The single endpoint for the whole process.
    ///
    /// Deliberately here rather than on either role: two roles in one process
    /// that each bound their own port would be two targets for one host, which
    /// is the confusion co-location exists to remove. Per-role `observability`
    /// blocks are rejected below rather than silently ignored.
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

impl ColocatedNodeConfig {
    fn validate(&self) -> Result<(), String> {
        // A data role this config can never serve is refused before either
        // role binds a port (review): the standalone entry judges it in
        // `data_node::run`, and co-location enters `serve` directly.
        crate::config::refuse_plaintext_promotion(
            self.data.role,
            self.data.lease.is_some(),
            !self.data.followers.is_empty(),
            self.data.replica_transport,
        )?;
        crate::config::refuse_kafka_gateway_misuse(self.data.role, self.data.kafka.as_ref())?;
        // And the partition topology, before either role binds (review). This
        // check lives in `data_node::run` for the standalone entry and in
        // `serve` for direct callers, but `run` below binds the shared
        // observability endpoint BEFORE `serve` is ever polled: without it, a
        // co-located node with an unroutable topology fails late and leaves
        // the metrics port held.
        if let Some(kafka) = self.data.kafka.as_ref() {
            crate::config::kafka_partitions(kafka)?;
        }
        // Fail loudly rather than picking a winner. A config that names three
        // listen addresses and gets one is a config whose author has a wrong
        // model of the process. PRESENCE of the block is what is rejected —
        // even an empty `observability: {}` — because an author who wrote the
        // key at all believed the role owns an endpoint, and silently ignoring
        // that belief hides the wrong model instead of correcting it.
        if self.meta.observability.is_some() || self.data.observability.is_some() {
            return Err(
                "a co-located node exposes ONE observability endpoint: set it at the top \
                 level, not under `meta` or `data`"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

pub async fn run(
    config: ColocatedNodeConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    config.validate()?;
    let ColocatedNodeConfig {
        meta,
        data,
        observability: endpoint,
    } = config;

    // One registry, one gate, one endpoint. `node_info` carries the combined
    // role so a dashboard can tell a co-located node from a dedicated one
    // without inferring it from which metrics happen to be present.
    let observability = NodeObservability::new(
        "colocated",
        &format!("meta-{}/data-{}", meta.node_id, data.node_uuid),
    )?;
    // Readiness is the CONJUNCTION of the roles: each role flips the shared
    // gate once its listeners are bound, and the gate opens only on the
    // second flip. Without this, whichever role won startup would advertise
    // the whole process as ready while the other still had no listener —
    // routing traffic at a half-started node.
    observability.gate.require_marks(2);
    let metrics_addr = observability.serve(&endpoint).await?;

    println!(
        "colocated_node_starting meta={} data={}",
        meta.node_id, data.node_uuid
    );
    use std::io::Write;
    std::io::stdout().flush().ok();

    // Shared fate: whichever role exits first ends the process, carrying its
    // error. A half-alive node is the one failure mode co-location must not
    // introduce — a metadata voter that has died inside a process still serving
    // data keeps being counted toward quorum while answering nothing.
    //
    // Shutdown is SEQUENCED, not shared (#280). The data role's drain ends by
    // proposing ReleaseRangeLease — through the local admin endpoint, in the
    // supported single-endpoint configuration. Handing the raw signal to both
    // roles would race that proposal against the admin listener closing, and
    // the release would commonly lose: the lease would lapse on its deadline,
    // defeating the orderly handoff. So the metadata role's shutdown fires
    // only after the data role has fully drained, and the drain waits are
    // bounded by the lease duration rather than a constant, so a slow release
    // round trip is not abandoned inside the chart's grace budget.
    let (meta_shutdown, meta_shutdown_rx) = tokio::sync::watch::channel(false);
    let lease_bound = data
        .lease
        .as_ref()
        .map(|lease| std::time::Duration::from_millis(lease.lease_duration_ms))
        .unwrap_or_default();
    let drain = std::time::Duration::from_secs(10).max(lease_bound);
    let mut meta_role = std::pin::pin!(meta_node::serve(
        meta,
        &observability,
        metrics_addr,
        meta_shutdown_rx
    ));
    let mut data_role = std::pin::pin!(data_node::serve(
        data,
        &observability,
        metrics_addr,
        shutdown.clone()
    ));
    tokio::select! {
        result = &mut meta_role => {
            // The metadata role can only exit on a fault now — its shutdown
            // channel has not fired — so this is the shared-fate arm.
            result.map_err(|error| format!("metadata role exited: {error}"))?;
            Err("metadata role exited unexpectedly without an error".to_owned())
        }
        result = &mut data_role => {
            result.map_err(|error| format!("data role exited: {error}"))?;
            // The data role drained (lease released, boundary committed);
            // only now may the local admin plane go down.
            let _ = meta_shutdown.send(true);
            tokio::time::timeout(drain, meta_role)
                .await
                .map_err(|_| "metadata role did not finish draining".to_owned())?
                .map_err(|error| format!("metadata role exited: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two roles in one process that each bound their own port would be two
    /// scrape targets for one host — the confusion co-location exists to
    /// remove. Rejecting is better than picking a winner: a config naming
    /// three listen addresses and getting one belongs to an author with a
    /// wrong model of the process.
    #[test]
    fn a_per_role_observability_block_is_rejected_not_ignored() {
        let yaml = r#"
meta:
  node_id: 1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/meta
  peer_listen: "127.0.0.1:9101"
  admin_listen: "127.0.0.1:9201"
  tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  observability: { listen: "127.0.0.1:9501" }
data:
  role: standalone
  node_uuid: 00000000-0000-0000-0000-0000000000a1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/data
  fencing_epoch: 1
  range: { topic: t, topic_epoch: 1, range_id: 00000000-0000-0000-0000-0000000000c1, range_generation: 0 }
  segment_id: 00000000-0000-0000-0000-0000000000d1
  native_listen: "127.0.0.1:9400"
  replica_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  native_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  principal_id: 00000000-0000-0000-0000-0000000000e1
observability:
  listen: "127.0.0.1:9500"
"#;
        let config: ColocatedNodeConfig = serde_yaml::from_str(yaml).unwrap();
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("ONE observability endpoint"),
            "the error must say what to do instead: {error}"
        );
    }

    /// A topology no client could route is refused by the co-located entry
    /// too. `run` binds the shared observability endpoint before `serve` is
    /// polled, so a check that lived only in `serve` would fail late and
    /// leave the metrics port held.
    #[test]
    fn a_bad_partition_topology_is_refused_before_either_role_binds() {
        let yaml = r#"
meta:
  node_id: 1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/meta
  peer_listen: "127.0.0.1:9101"
  admin_listen: "127.0.0.1:9201"
  tls: { ca: ca.pem, cert: c.pem, key: k.pem }
data:
  role: standalone
  node_uuid: 00000000-0000-0000-0000-0000000000a1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/data
  fencing_epoch: 1
  range: { topic: t, topic_epoch: 1, range_id: 00000000-0000-0000-0000-0000000000c1, range_generation: 0 }
  segment_id: 00000000-0000-0000-0000-0000000000d1
  native_listen: "127.0.0.1:9400"
  replica_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  native_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  principal_id: 00000000-0000-0000-0000-0000000000e1
  kafka:
    listen: "127.0.0.1:9092"
    node_id: 1
    partition: 0
    partitions:
      - { partition: 0, node_id: 1, host: a, port: 9092 }
      - { partition: 1, node_id: 1, host: a, port: 9092 }
observability:
  listen: "127.0.0.1:9500"
"#;
        let config: ColocatedNodeConfig = serde_yaml::from_str(yaml).unwrap();
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("one gateway serves one range"),
            "the co-located entry must judge the topology too: {error}"
        );
    }

    /// PRESENCE is the error, not just a bound address: an author who wrote
    /// `observability: {}` under a role believed that role owns an endpoint,
    /// and silently ignoring the block would hide the wrong model instead of
    /// correcting it.
    #[test]
    fn an_empty_per_role_observability_block_is_still_rejected() {
        let yaml = r#"
meta:
  node_id: 1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/meta
  peer_listen: "127.0.0.1:9101"
  admin_listen: "127.0.0.1:9201"
  tls: { ca: ca.pem, cert: c.pem, key: k.pem }
data:
  role: standalone
  node_uuid: 00000000-0000-0000-0000-0000000000a1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/data
  fencing_epoch: 1
  range: { topic: t, topic_epoch: 1, range_id: 00000000-0000-0000-0000-0000000000c1, range_generation: 0 }
  segment_id: 00000000-0000-0000-0000-0000000000d1
  native_listen: "127.0.0.1:9400"
  replica_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  native_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  principal_id: 00000000-0000-0000-0000-0000000000e1
  observability: {}
observability:
  listen: "127.0.0.1:9500"
"#;
        let config: ColocatedNodeConfig = serde_yaml::from_str(yaml).unwrap();
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("ONE observability endpoint"),
            "an empty block is still a per-role block: {error}"
        );
    }

    #[test]
    fn one_top_level_endpoint_is_accepted() {
        let yaml = r#"
meta:
  node_id: 1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/meta
  peer_listen: "127.0.0.1:9101"
  admin_listen: "127.0.0.1:9201"
  tls: { ca: ca.pem, cert: c.pem, key: k.pem }
data:
  role: standalone
  node_uuid: 00000000-0000-0000-0000-0000000000a1
  cluster_id: 00000000-0000-0000-0000-0000000000c0
  data_dir: /tmp/data
  fencing_epoch: 1
  range: { topic: t, topic_epoch: 1, range_id: 00000000-0000-0000-0000-0000000000c1, range_generation: 0 }
  segment_id: 00000000-0000-0000-0000-0000000000d1
  native_listen: "127.0.0.1:9400"
  replica_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  native_tls: { ca: ca.pem, cert: c.pem, key: k.pem }
  principal_id: 00000000-0000-0000-0000-0000000000e1
observability:
  listen: "127.0.0.1:9500"
"#;
        let config: ColocatedNodeConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
    }
}

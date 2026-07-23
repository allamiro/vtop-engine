//! Deterministic, constraint-aware replica placement.
//!
//! Placement decisions are pure functions of segment identity and the eligible
//! node set: hard constraints filter first, then weighted rendezvous hashing
//! ranks survivors. The same inputs always produce the same ordered replica
//! list, which the metadata state machine validates before committing.

use uuid::Uuid;

/// Upper bound on replicas recorded for one segment placement.
pub const MAX_REPLICAS: usize = 8;

/// Bound for failure-domain attribute strings on node records.
pub const MAX_FAILURE_DOMAIN_BYTES: usize = 64;

/// Default placement weight for a newly registered node.
pub const DEFAULT_PLACEMENT_WEIGHT: u32 = 100;

/// Minimum accepted placement weight. Zero would collapse rendezvous scores.
pub const MIN_PLACEMENT_WEIGHT: u32 = 1;

/// Domain-separated BLAKE3 key for weighted rendezvous scores.
const RENDEZVOUS_DERIVE_KEY: &str = "vtop-meta/placement/rendezvous/v1";

/// An eligible placement candidate after hard-constraint filtering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementCandidate {
    pub node_uuid: Uuid,
    pub failure_domain: String,
    pub weight: u32,
}

/// Why a placement request cannot be satisfied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementError {
    /// `replication_factor` is zero or above [`MAX_REPLICAS`].
    InvalidReplicationFactor(usize),
    /// Not enough eligible nodes (or distinct failure domains) remain.
    InsufficientEligibleNodes { requested: usize, available: usize },
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlacementError::InvalidReplicationFactor(rf) => {
                write!(
                    formatter,
                    "replication factor {rf} must be 1..={MAX_REPLICAS}"
                )
            }
            PlacementError::InsufficientEligibleNodes {
                requested,
                available,
            } => write!(
                formatter,
                "need {requested} eligible replica(s), only {available} available"
            ),
        }
    }
}

/// Weighted rendezvous score for `(segment, node)`. Higher is better.
///
/// Uses a domain-separated BLAKE3 digest so scores are stable across platforms
/// and independent of other VTOP hash uses. The high 64 bits of the digest are
/// scaled by weight into a `u128` so ordinary weights cannot saturate the
/// product; ties are broken by node UUID when ranking.
pub fn rendezvous_score(segment_uuid: Uuid, node_uuid: Uuid, weight: u32) -> u128 {
    let weight = weight.max(MIN_PLACEMENT_WEIGHT);
    let mut hasher = blake3::Hasher::new_derive_key(RENDEZVOUS_DERIVE_KEY);
    hasher.update(segment_uuid.as_bytes());
    hasher.update(node_uuid.as_bytes());
    let digest = hasher.finalize();
    let mut high = [0u8; 8];
    high.copy_from_slice(&digest.as_bytes()[..8]);
    let hash = u64::from_be_bytes(high);
    u128::from(hash).saturating_mul(u128::from(weight))
}

/// Select an ordered replica set for `segment_uuid`.
///
/// Candidates are ranked by descending rendezvous score, then ascending node
/// UUID. When `require_distinct_failure_domains` is true, a candidate whose
/// failure domain is already represented is skipped.
pub fn select_replicas(
    segment_uuid: Uuid,
    candidates: &[PlacementCandidate],
    replication_factor: usize,
    require_distinct_failure_domains: bool,
) -> Result<Vec<Uuid>, PlacementError> {
    if !(1..=MAX_REPLICAS).contains(&replication_factor) {
        return Err(PlacementError::InvalidReplicationFactor(replication_factor));
    }

    let mut ranked: Vec<(u128, Uuid, &str)> = candidates
        .iter()
        .filter(|candidate| candidate.weight >= MIN_PLACEMENT_WEIGHT)
        .map(|candidate| {
            (
                rendezvous_score(segment_uuid, candidate.node_uuid, candidate.weight),
                candidate.node_uuid,
                candidate.failure_domain.as_str(),
            )
        })
        .collect();
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

    let mut selected = Vec::with_capacity(replication_factor);
    let mut used_domains = Vec::with_capacity(replication_factor);
    for (_, node_uuid, domain) in ranked {
        if selected.contains(&node_uuid) {
            continue;
        }
        if require_distinct_failure_domains && used_domains.iter().any(|used| used == &domain) {
            continue;
        }
        selected.push(node_uuid);
        used_domains.push(domain);
        if selected.len() == replication_factor {
            return Ok(selected);
        }
    }

    Err(PlacementError::InsufficientEligibleNodes {
        requested: replication_factor,
        available: selected.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: u128, domain: &str, weight: u32) -> PlacementCandidate {
        PlacementCandidate {
            node_uuid: Uuid::from_u128(id),
            failure_domain: domain.to_owned(),
            weight,
        }
    }

    #[test]
    fn rendezvous_scores_are_deterministic_and_weight_sensitive() {
        let segment = Uuid::from_u128(42);
        let node = Uuid::from_u128(7);
        let once = rendezvous_score(segment, node, 100);
        let twice = rendezvous_score(segment, node, 100);
        assert_eq!(once, twice);
        assert!(rendezvous_score(segment, node, 200) > once);
        assert_ne!(
            rendezvous_score(segment, node, 100),
            rendezvous_score(segment, Uuid::from_u128(8), 100)
        );
    }

    #[test]
    fn select_replicas_is_deterministic_and_honors_failure_domains() {
        let segment = Uuid::from_u128(100);
        let candidates = vec![
            candidate(1, "rack-a", 100),
            candidate(2, "rack-a", 100),
            candidate(3, "rack-b", 100),
            candidate(4, "rack-c", 100),
        ];
        let first = select_replicas(segment, &candidates, 3, true).unwrap();
        let second = select_replicas(segment, &candidates, 3, true).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 3);

        let domains: Vec<&str> = first
            .iter()
            .map(|id| {
                candidates
                    .iter()
                    .find(|c| c.node_uuid == *id)
                    .unwrap()
                    .failure_domain
                    .as_str()
            })
            .collect();
        let mut unique = domains.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 3, "domains={domains:?}");
    }

    #[test]
    fn select_replicas_rejects_undersized_domain_sets() {
        let segment = Uuid::from_u128(9);
        let candidates = vec![
            candidate(1, "rack-a", 100),
            candidate(2, "rack-a", 100),
            candidate(3, "rack-a", 100),
        ];
        assert_eq!(
            select_replicas(segment, &candidates, 2, true),
            Err(PlacementError::InsufficientEligibleNodes {
                requested: 2,
                available: 1,
            })
        );
    }

    #[test]
    fn select_replicas_rejects_invalid_replication_factor() {
        assert_eq!(
            select_replicas(Uuid::from_u128(1), &[], 0, false),
            Err(PlacementError::InvalidReplicationFactor(0))
        );
        assert_eq!(
            select_replicas(Uuid::from_u128(1), &[], MAX_REPLICAS + 1, false),
            Err(PlacementError::InvalidReplicationFactor(MAX_REPLICAS + 1))
        );
    }
}

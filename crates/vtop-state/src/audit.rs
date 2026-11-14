//! The state store's `audit_events` table (#694).
//!
//! The ledger commands that delete, rewind or reshape what the engine will
//! do (`prune-ledger`, `replay`, `migrate`) leave a durable, attributable
//! record in the store they changed: one row per audit record, append only.
//! The record itself is built by `vtop_observe::audit` and stored here as the
//! exact JSON line the log and file sinks carry; the columns beside it are
//! what a listing filters on.
//!
//! **Append only.** Rows are inserted, never updated or deleted: an outcome
//! is a second row, not an update of the attempt, and `prune-ledger` never
//! touches this table. On PostgreSQL the runtime role is granted `SELECT,
//! INSERT` only, and a trigger refuses `UPDATE`, `DELETE` and `TRUNCATE` for
//! every role but the table's owner (the migration identity). On SQLite a
//! trigger refuses them; anyone who can write the file can remove it, which
//! is what the chain below is for.
//!
//! **The hash chain.** Each row stores the link of the row before it and its
//! own: `link = BLAKE3(previous_link "\n" record)`, keyed with a key derived
//! from the manifest MAC key when one is configured
//! ([`vtop_core::manifest::audit_chain_link`]). [`verify_chain`] recomputes
//! it: an edited, deleted or reordered row breaks it, and a keyed link cannot
//! be recomputed without the key. What a chain cannot show is rows removed
//! from the end; `vtopctl audit verify` prints the last link and the row
//! count, for an operator to keep elsewhere and compare.

use vtop_core::manifest::{
    audit_chain_link, ManifestMacKey, AUDIT_CHAIN_KEYED, AUDIT_CHAIN_UNKEYED,
};

/// A row to append. The indexed columns are copies of the record's fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditInsert {
    pub audit_id: String,
    pub time: String,
    pub action: String,
    pub outcome: String,
    pub actor: Option<String>,
    pub target: Option<String>,
    pub audit_version: i64,
    /// The record, exactly as the log and file sinks carry it.
    pub record: String,
}

impl AuditInsert {
    /// The row for a record's JSON line, its columns read from the record.
    pub fn from_record(record: &str) -> Result<Self, String> {
        let value: serde_json::Value =
            serde_json::from_str(record).map_err(|e| format!("audit record is not JSON: {e}"))?;
        let text = |path: &[&str]| -> Option<String> {
            let mut at = &value;
            for key in path {
                at = at.get(*key)?;
            }
            at.as_str().map(str::to_owned)
        };
        Ok(AuditInsert {
            audit_id: text(&["audit_id"]).ok_or("audit record has no audit_id")?,
            time: text(&["time"]).ok_or("audit record has no time")?,
            action: text(&["action"]).ok_or("audit record has no action")?,
            outcome: text(&["outcome"]).ok_or("audit record has no outcome")?,
            actor: text(&["actor", "name"]).or_else(|| {
                value
                    .get("actor")
                    .and_then(|a| a.get("uid"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|uid| format!("uid:{uid}"))
            }),
            target: text(&["target", "id"]).or_else(|| text(&["target", "kind"])),
            audit_version: value
                .get("audit_version")
                .and_then(serde_json::Value::as_i64)
                .ok_or("audit record has no audit_version")?,
            record: record.to_owned(),
        })
    }
}

/// A stored row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditRow {
    /// Insertion order; the chain runs in this order.
    pub seq: i64,
    pub audit_id: String,
    pub time: String,
    pub action: String,
    pub outcome: String,
    pub actor: Option<String>,
    pub target: Option<String>,
    pub audit_version: i64,
    pub record: String,
    /// The link of the row before this one; `None` for the first row.
    pub prev_hash: Option<String>,
    pub hash: String,
    /// `blake3` or `blake3-keyed`.
    pub chain: String,
}

/// The link and chain name for a row appended after `previous`.
pub fn link_for(
    previous: Option<&str>,
    record: &str,
    key: Option<&ManifestMacKey>,
) -> (String, &'static str) {
    audit_chain_link(previous, record, key)
}

/// What [`verify_chain`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChainReport {
    pub rows: u64,
    pub keyed_rows: u64,
    /// The last row's link: keep it elsewhere to detect rows removed from
    /// the end, which no chain can show by itself.
    pub last_hash: Option<String>,
    pub last_seq: Option<i64>,
    /// Each break, naming the row.
    pub problems: Vec<String>,
}

impl ChainReport {
    pub fn passed(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Recompute the chain over `rows` (in `seq` order, from the first row).
///
/// A row breaks the chain when its `prev_hash` is not the previous row's
/// link (a row deleted or reordered), or its link does not match its record
/// (a row edited). A keyed row cannot be checked without `key`, and is
/// reported as such; an unkeyed row after a keyed one is reported too: a
/// writer without the key could otherwise rewrite the rest of the table as
/// an unkeyed chain.
pub fn verify_chain(rows: &[AuditRow], key: Option<&ManifestMacKey>) -> ChainReport {
    let mut report = ChainReport::default();
    let mut previous: Option<&str> = None;
    let mut seen_keyed = false;
    for row in rows {
        report.rows += 1;
        let at = format!("row {} (audit_id {})", row.seq, row.audit_id);
        if row.prev_hash.as_deref() != previous {
            report.problems.push(format!(
                "{at}: its previous link is {}, but the row before it has {}: a row was \
                 deleted, inserted or reordered here",
                row.prev_hash.as_deref().unwrap_or("none"),
                previous.unwrap_or("none (this is the first row)")
            ));
        }
        match row.chain.as_str() {
            AUDIT_CHAIN_KEYED => {
                seen_keyed = true;
                report.keyed_rows += 1;
                match key {
                    Some(key) => {
                        let (expected, _) =
                            link_for(row.prev_hash.as_deref(), &row.record, Some(key));
                        if expected != row.hash {
                            report.problems.push(format!(
                                "{at}: its keyed link does not match its record: the row was \
                                 edited, or the configured key is not the one it was written with"
                            ));
                        }
                    }
                    None => report.problems.push(format!(
                        "{at}: keyed with the manifest MAC key, which is not configured here; \
                         set manifest_mac_key_env in the config to verify it"
                    )),
                }
            }
            AUDIT_CHAIN_UNKEYED => {
                if seen_keyed {
                    report.problems.push(format!(
                        "{at}: an unkeyed link after a keyed one: the chain was rewritten \
                         without the key, or the key was removed from the configuration"
                    ));
                }
                let (expected, _) = link_for(row.prev_hash.as_deref(), &row.record, None);
                if expected != row.hash {
                    report.problems.push(format!(
                        "{at}: its link does not match its record: the row was edited"
                    ));
                }
            }
            other => report.problems.push(format!(
                "{at}: chain {other:?} is not one this build reads (a newer release wrote it)"
            )),
        }
        previous = Some(&row.hash);
        report.last_hash = Some(row.hash.clone());
        report.last_seq = Some(row.seq);
    }
    report
}

/// The refusal for a store that keeps no audit table (a test double).
pub(crate) fn unsupported() -> vtop_core::errors::VtopError {
    vtop_core::errors::VtopError::State("this state store keeps no audit_events table".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(n: usize, key: Option<&ManifestMacKey>) -> Vec<AuditRow> {
        let mut rows: Vec<AuditRow> = Vec::new();
        for i in 0..n {
            let record = format!(r#"{{"audit_version":1,"audit_id":"a{i}"}}"#);
            let prev = rows.last().map(|r| r.hash.clone());
            let (hash, name) = link_for(prev.as_deref(), &record, key);
            rows.push(AuditRow {
                seq: i as i64 + 1,
                audit_id: format!("a{i}"),
                time: "2026-11-14T00:00:00.000Z".into(),
                action: "ledger.prune".into(),
                outcome: "success".into(),
                actor: None,
                target: None,
                audit_version: 1,
                record,
                prev_hash: prev,
                hash,
                chain: name.into(),
            });
        }
        rows
    }

    fn key() -> ManifestMacKey {
        ManifestMacKey::from_hex(&"ab".repeat(32)).unwrap()
    }

    #[test]
    fn an_intact_chain_verifies_and_names_its_last_link() {
        let rows = chain(5, None);
        let report = verify_chain(&rows, None);
        assert!(report.passed(), "{:?}", report.problems);
        assert_eq!(report.rows, 5);
        assert_eq!(report.last_hash.as_deref(), Some(rows[4].hash.as_str()));
        let keyed = chain(3, Some(&key()));
        assert!(verify_chain(&keyed, Some(&key())).passed());
    }

    #[test]
    fn an_edited_row_breaks_the_chain() {
        let mut rows = chain(4, None);
        rows[2].record = rows[2].record.replace("a2", "a9");
        let report = verify_chain(&rows, None);
        assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
        assert!(report.problems[0].contains("row 3") && report.problems[0].contains("edited"));
    }

    #[test]
    fn a_deleted_row_breaks_the_chain() {
        let mut rows = chain(4, None);
        rows.remove(1);
        let report = verify_chain(&rows, None);
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.contains("row 3") && p.contains("deleted")),
            "{:?}",
            report.problems
        );
    }

    #[test]
    fn reordered_rows_break_the_chain() {
        let mut rows = chain(4, None);
        rows.swap(1, 2);
        assert!(!verify_chain(&rows, None).passed());
    }

    #[test]
    fn a_keyed_chain_cannot_be_recomputed_without_the_key() {
        let mut rows = chain(3, Some(&key()));
        // A writer without the key edits a row and recomputes its link as
        // best it can: unkeyed, or with a guessed key.
        rows[1].record = rows[1].record.replace("a1", "a7");
        let other = ManifestMacKey::from_hex(&"cd".repeat(32)).unwrap();
        let (forged, _) = link_for(rows[1].prev_hash.as_deref(), &rows[1].record, Some(&other));
        rows[1].hash = forged;
        rows[2].prev_hash = Some(rows[1].hash.clone());
        let report = verify_chain(&rows, Some(&key()));
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.contains("row 2") && p.contains("keyed link")),
            "{:?}",
            report.problems
        );
        // Rewritten as an unkeyed chain from there on: caught too.
        let mut downgraded = chain(3, Some(&key()));
        let (hash, name) = link_for(
            downgraded[1].prev_hash.as_deref(),
            &downgraded[1].record,
            None,
        );
        downgraded[1].hash = hash;
        downgraded[1].chain = name.into();
        downgraded[2].prev_hash = Some(downgraded[1].hash.clone());
        let (hash, _) = link_for(
            downgraded[2].prev_hash.as_deref(),
            &downgraded[2].record,
            Some(&key()),
        );
        downgraded[2].hash = hash;
        let report = verify_chain(&downgraded, Some(&key()));
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.contains("unkeyed link after a keyed one")),
            "{:?}",
            report.problems
        );
        // And without the key at all, a keyed row is not passed as checked.
        let report = verify_chain(&chain(2, Some(&key())), None);
        assert_eq!(report.problems.len(), 2);
        assert!(report.problems[0].contains("manifest_mac_key_env"));
    }

    #[test]
    fn a_row_reads_its_columns_from_the_record() {
        let row = AuditInsert::from_record(
            r#"{"audit_version":1,"audit_id":"x","time":"t","action":"ledger.prune","outcome":"attempt","actor":{"kind":"os","uid":1000,"source":"os"},"target":{"kind":"ledger"}}"#,
        )
        .unwrap();
        assert_eq!(row.actor.as_deref(), Some("uid:1000"));
        assert_eq!(row.target.as_deref(), Some("ledger"));
        assert!(AuditInsert::from_record("{}").is_err());
    }
}

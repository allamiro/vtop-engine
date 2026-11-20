//! No two codes in one code space share a value (#636 review).
//!
//! #634 and #636 were written in parallel and both took command kind 38,
//! admin frame kinds 29/30, key category 21 and value tag 24. Each branch was
//! internally consistent, so every round-trip test passed on each; only the
//! merge would have collided, and a collision in a durable code is a log
//! entry or snapshot record that decodes as the wrong thing. This test reads
//! the declarations themselves, so a merge that leaves two names on one
//! value fails here by name, whatever the match arms happen to do.

use std::collections::BTreeMap;

/// One code space: the constants in `source` whose names start `prefix`.
struct Space {
    what: &'static str,
    file: &'static str,
    source: &'static str,
    prefix: &'static str,
    /// Names with this prefix that belong to another space, or are not codes.
    exclude: &'static [&'static str],
}

const COMMAND: &str = include_str!("../src/command.rs");
const KEYS: &str = include_str!("../src/keys.rs");
const STATE: &str = include_str!("../src/state.rs");
const WIRE: &str = include_str!("../src/transport/wire.rs");

const SPACES: &[Space] = &[
    Space {
        what: "metadata command kind",
        file: "command.rs",
        source: COMMAND,
        prefix: "COMMAND_KIND_",
        exclude: &[],
    },
    Space {
        what: "metadata response kind",
        file: "command.rs",
        source: COMMAND,
        prefix: "RESPONSE_KIND_",
        exclude: &[],
    },
    Space {
        what: "metadata error kind",
        file: "command.rs",
        source: COMMAND,
        prefix: "ERROR_KIND_",
        exclude: &[],
    },
    Space {
        what: "promotion outcome tag",
        file: "command.rs",
        source: COMMAND,
        prefix: "PROMOTION_OUTCOME_",
        exclude: &[],
    },
    Space {
        what: "quorum answer lineage tag",
        file: "command.rs",
        source: COMMAND,
        prefix: "ANSWER_LINEAGE_",
        exclude: &[],
    },
    Space {
        what: "meta key category",
        file: "keys.rs",
        source: KEYS,
        prefix: "CATEGORY_",
        exclude: &[],
    },
    Space {
        what: "snapshot record value tag",
        file: "state.rs",
        source: STATE,
        prefix: "VALUE_TAG_",
        exclude: &[],
    },
    Space {
        what: "transport frame kind",
        file: "transport/wire.rs",
        source: WIRE,
        prefix: "KIND_",
        exclude: &[],
    },
];

/// Every `const NAME: uN = VALUE;` in `source` whose name starts `prefix`.
fn declarations(space: &Space) -> Vec<(String, u64)> {
    let mut found = Vec::new();
    for line in space.source.lines() {
        let line = line.trim();
        let line = line
            .strip_prefix("pub(crate) ")
            .or_else(|| line.strip_prefix("pub "))
            .unwrap_or(line);
        let Some(rest) = line.strip_prefix("const ") else {
            continue;
        };
        if !rest.starts_with(space.prefix) {
            continue;
        }
        let Some((name, rest)) = rest.split_once(':') else {
            continue;
        };
        if space.exclude.contains(&name) {
            continue;
        }
        let Some((ty, value)) = rest.split_once('=') else {
            continue;
        };
        if !matches!(ty.trim(), "u8" | "u16" | "u32") {
            continue;
        }
        let value = value.trim().trim_end_matches(';').trim();
        let value: u64 = value
            .parse()
            .unwrap_or_else(|_| panic!("{}: {name} = {value} is not a literal", space.file));
        found.push((name.to_owned(), value));
    }
    found
}

#[test]
fn no_two_codes_in_one_space_share_a_value() {
    let mut problems = Vec::new();
    for space in SPACES {
        let found = declarations(space);
        // A scanner that finds nothing would pass for the wrong reason.
        assert!(
            found.len() >= 2,
            "{} ({}): found {} declarations with prefix {}; the scan is blind",
            space.what,
            space.file,
            found.len(),
            space.prefix
        );
        let mut by_value: BTreeMap<u64, Vec<&str>> = BTreeMap::new();
        for (name, value) in &found {
            by_value.entry(*value).or_default().push(name);
        }
        for (value, names) in by_value {
            if names.len() > 1 {
                problems.push(format!(
                    "{} {value} is taken by {} ({})",
                    space.what,
                    names.join(" and "),
                    space.file
                ));
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The scan itself catches a collision (so the test above is not vacuous).
#[test]
fn the_scan_reports_a_duplicate() {
    let space = Space {
        what: "test",
        file: "inline",
        source: "const COMMAND_KIND_A: u16 = 38;\npub const COMMAND_KIND_B: u16 = 38;\n",
        prefix: "COMMAND_KIND_",
        exclude: &[],
    };
    let found = declarations(&space);
    assert_eq!(
        found,
        vec![
            ("COMMAND_KIND_A".to_owned(), 38),
            ("COMMAND_KIND_B".to_owned(), 38)
        ]
    );
}

/// #636 and #634 take the next free code in each space, in that merge order:
/// pinned, so a later renumbering is a visible decision rather than drift.
#[test]
fn the_override_holds_the_codes_after_the_version_report() {
    let value = |space: usize, name: &str| {
        declarations(&SPACES[space])
            .into_iter()
            .find(|(found, _)| found == name)
            .map(|(_, value)| value)
            .unwrap_or_else(|| panic!("{name} not declared"))
    };
    assert_eq!(value(0, "COMMAND_KIND_REPORT_NODE_VERSION"), 38);
    assert_eq!(value(0, "COMMAND_KIND_ATTEST_UNPROVEN_PROMOTION"), 39);
    assert_eq!(value(5, "CATEGORY_NODE_VERSION"), 21);
    assert_eq!(value(5, "CATEGORY_PROMOTION_ATTESTATION"), 22);
    assert_eq!(value(6, "VALUE_TAG_NODE_VERSION"), 24);
    assert_eq!(value(6, "VALUE_TAG_PROMOTION_ATTESTATION"), 25);
    assert_eq!(value(7, "KIND_ADMIN_READ_CLUSTER_VERSIONS_REQ"), 29);
    assert_eq!(value(7, "KIND_ADMIN_READ_CLUSTER_VERSIONS_RESP"), 30);
    assert_eq!(value(7, "KIND_ADMIN_READ_PROMOTION_ATTESTATION_REQ"), 31);
    assert_eq!(value(7, "KIND_ADMIN_READ_PROMOTION_ATTESTATION_RESP"), 32);
}

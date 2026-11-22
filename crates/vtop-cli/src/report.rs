//! The version of a `--json` report (#743).
//!
//! Every `vtopctl` command that prints a JSON **object** with `--json` stamps
//! it with `report_version`, so a script can tell which shape of that report it
//! is reading, and a later release can change a report without the script
//! guessing. The field is additive: a reader that ignores unknown fields
//! needs no change. A report whose shape changes incompatibly bumps the number
//! (never within a minor of 1.x, `docs/reference/compatibility.md`).
//!
//! Output that is not an object cannot carry a field without changing shape, so
//! it is versioned by what it is and is listed in [`NOT_STAMPED`] with the
//! reason: an array of rows, a map of counts, a stored record that has its own
//! version marker, and the effective-config report, which has one too. A test
//! reads this crate's sources and fails for any other place that prints JSON.

use serde::Serialize;
use serde_json::Value;

/// The current version of every stamped report. They share one number: a
/// report changing incompatibly is the event that matters, not which one.
pub const REPORT_VERSION: u64 = 1;

/// The field a stamped report carries.
pub const REPORT_FIELD: &str = "report_version";

/// A JSON report with `report_version` set when it is an object. Any other
/// value is returned as it is (see the module note).
pub fn stamp(mut value: Value) -> Value {
    if let Value::Object(map) = &mut value {
        map.insert(REPORT_FIELD.to_owned(), Value::from(REPORT_VERSION));
    }
    value
}

/// [`serde_json::to_string_pretty`] of a stamped report: a drop-in for the
/// call a command made before it had a version.
pub fn pretty<T: Serialize + ?Sized>(report: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&stamp(serde_json::to_value(report)?))
}

/// `serde_json::json!` of a stamped report.
macro_rules! report_json {
    ($($json:tt)+) => {
        $crate::report::stamp(serde_json::json!($($json)+))
    };
}
pub(crate) use report_json;

/// Every place a command prints JSON that is not stamped:
/// `(file, how many print statements, what they print, why there is no
/// `report_version`)`. The source scan in the tests holds this list to the
/// code: a new JSON print is either stamped or listed here, and a listed one
/// that goes away must be removed.
pub const NOT_STAMPED: &[(&str, usize, &str, &str)] = &[
    (
        "commands.rs",
        1,
        "receipt --json: each stored receipt line",
        "a stored record with its own `receipt_version`, printed as written",
    ),
    (
        "commands.rs",
        3,
        "discover --json, process-once --json, list-batches --json",
        "arrays of rows: a row gains fields, the array has nowhere to put one",
    ),
    (
        "commands.rs",
        1,
        "status --json",
        "a map from batch state to count: a key that is not a state would be read as one",
    ),
    (
        "commands.rs",
        1,
        "validate --print-effective --json",
        "carries its own `effective_version`",
    ),
    (
        "segment_tools.rs",
        1,
        "segment verify --json",
        "an array with one report per file",
    ),
    (
        "segment_tools.rs",
        1,
        "segment prove-chunk --json",
        "a proof file that `segment verify-chunk` reads back, versioned by its `scheme`",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn an_object_gains_the_field_and_keeps_its_own() {
        let stamped = stamp(serde_json::json!({"status": "ok", "passed": true}));
        assert_eq!(stamped["report_version"], 1);
        assert_eq!(stamped["status"], "ok");
        assert_eq!(stamped["passed"], true);
    }

    #[test]
    fn anything_that_is_not_an_object_is_left_alone() {
        for value in [
            serde_json::json!([{"a": 1}]),
            serde_json::json!(3),
            serde_json::json!("x"),
            Value::Null,
        ] {
            assert_eq!(stamp(value.clone()), value);
        }
    }

    #[test]
    fn pretty_prints_the_stamped_report() {
        let text = pretty(&serde_json::json!({"found": false})).unwrap();
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["found"], false);
        assert_eq!(parsed["report_version"], REPORT_VERSION);
    }

    /// Every JSON print in this crate goes through `pretty` or `report_json!`,
    /// or is one of the [`NOT_STAMPED`] ones. Written as a scan of the sources
    /// because the commands print from many modules and a new one is the thing
    /// to catch.
    #[test]
    fn every_json_print_is_stamped_or_listed() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut unstamped: Vec<(String, usize)> = Vec::new();
        for entry in std::fs::read_dir(&src).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if name == "report.rs" {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            // Only the code above the file's test module prints for a user.
            let code = text.split("\n#[cfg(test)]\nmod ").next().unwrap();
            for (index, line) in code.lines().enumerate() {
                let prints_json = line.contains("serde_json::to_string_pretty(")
                    || (line.contains("println!(") && line.contains("serde_json::json!("))
                    || line.trim_start().starts_with("serde_json::json!({")
                        && code
                            .lines()
                            .nth(index.wrapping_sub(1))
                            .is_some_and(|previous| previous.trim() == "\"{}\",");
                if prints_json {
                    unstamped.push((name.clone(), index + 1));
                }
            }
        }
        let mut found = std::collections::BTreeMap::new();
        for (file, _) in &unstamped {
            *found.entry(file.clone()).or_insert(0usize) += 1;
        }
        let mut listed = std::collections::BTreeMap::new();
        for (file, prints, _, _) in NOT_STAMPED {
            *listed.entry((*file).to_owned()).or_insert(0usize) += prints;
        }
        assert_eq!(
            found, listed,
            "the unstamped JSON prints per file differ from NOT_STAMPED: print through \
             crate::report::pretty or report_json!, or list the output in NOT_STAMPED with the \
             reason it has no report_version (and remove an entry whose print is gone)"
        );
    }
}

//! Comparing the generated surface with its committed snapshot.

use std::collections::BTreeSet;
use std::path::Path;

/// Set to `1` to rewrite the snapshots instead of comparing them.
/// `scripts/update-surface-snapshots.sh` sets it.
pub const UPDATE_ENV: &str = "VTOP_UPDATE_SURFACE";

const HEADER: &str = "# Generated from the code by scripts/update-surface-snapshots.sh (#719); \
                      do not edit by hand. One item per line, sorted.";

/// Compare `generated` with the snapshot at `path`, or rewrite the snapshot
/// when [`UPDATE_ENV`] is `1`.
///
/// Compared as a set of lines, so order never matters: two pull requests
/// that each add a line merge cleanly (`.gitattributes` marks the snapshots
/// `merge=union`), and the result still passes. Blank lines and `#` comments
/// are ignored. On a difference, the error lists the lines on each side and
/// says what to do.
pub fn check(path: &Path, generated: impl IntoIterator<Item = String>) -> Result<(), String> {
    let update = std::env::var(UPDATE_ENV).as_deref() == Ok("1");
    compare_or_write(path, generated.into_iter().collect(), update)
}

fn compare_or_write(path: &Path, generated: BTreeSet<String>, update: bool) -> Result<(), String> {
    let shown = path.display();
    if update {
        let mut text = String::from(HEADER);
        text.push('\n');
        for line in &generated {
            text.push_str(line);
            text.push('\n');
        }
        return std::fs::write(path, text).map_err(|e| format!("write {shown}: {e}"));
    }
    let committed = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            return Err(format!(
                "cannot read the surface snapshot {shown}: {e}. Generate it with \
                 scripts/update-surface-snapshots.sh and commit it."
            ))
        }
    };
    let committed: BTreeSet<String> = committed
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect();
    if committed == generated {
        return Ok(());
    }
    let mut message =
        format!("The operator-facing surface changed: {shown} no longer matches the code.\n");
    let added: Vec<_> = generated.difference(&committed).collect();
    let removed: Vec<_> = committed.difference(&generated).collect();
    if !added.is_empty() {
        message.push_str(&format!(
            "  in the code but not in the snapshot ({}):\n",
            added.len()
        ));
        for line in &added {
            message.push_str(&format!("    + {line}\n"));
        }
    }
    if !removed.is_empty() {
        message.push_str(&format!(
            "  in the snapshot but not in the code ({}):\n",
            removed.len()
        ));
        for line in &removed {
            message.push_str(&format!("    - {line}\n"));
        }
    }
    message.push_str(
        "If this is intended, regenerate with scripts/update-surface-snapshots.sh and commit \
         the result in this pull request. An addition needs nothing more. Removing or \
         renaming a command, flag, accepted value, metric or label is a compatibility \
         change: it also needs an entry in docs/deprecations.md and an upgrade note in the \
         release notes, because dashboards, alerts and scripts use the old name.",
    );
    Err(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// The tests compare, whatever the environment says.
    fn check(path: &Path, generated: BTreeSet<String>) -> Result<(), String> {
        compare_or_write(path, generated, false)
    }

    #[test]
    fn an_update_writes_a_sorted_snapshot_that_then_compares_equal() {
        let dir = std::env::temp_dir().join(format!("vtop-surface-up-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("up.txt");
        compare_or_write(&path, lines(&["b gauge", "a counter"]), true).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with("a counter\nb gauge\n"), "{text}");
        check(&path, lines(&["a counter", "b gauge"])).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn order_comments_and_blank_lines_do_not_matter() {
        let dir = std::env::temp_dir().join(format!("vtop-surface-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("order.txt");
        std::fs::write(&path, "# header\nb gauge\n\na counter x\n").unwrap();
        check(&path, lines(&["a counter x", "b gauge"])).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_difference_names_both_sides_and_how_to_fix_it() {
        let dir = std::env::temp_dir().join(format!("vtop-surface-diff-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("diff.txt");
        std::fs::write(&path, "vtop_a_total counter state\nvtop_b gauge\n").unwrap();
        let error = check(
            &path,
            lines(&["vtop_a_total counter reason", "vtop_b gauge"]),
        )
        .unwrap_err();
        assert!(error.contains("+ vtop_a_total counter reason"), "{error}");
        assert!(error.contains("- vtop_a_total counter state"), "{error}");
        assert!(!error.contains("vtop_b gauge"), "{error}");
        assert!(
            error.contains("scripts/update-surface-snapshots.sh"),
            "{error}"
        );
        assert!(error.contains("docs/deprecations.md"), "{error}");
        assert!(error.contains("upgrade note"), "{error}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_snapshot_says_how_to_make_one() {
        let error = check(Path::new("/nonexistent/vtop-surface.txt"), lines(&["a"])).unwrap_err();
        assert!(
            error.contains("scripts/update-surface-snapshots.sh"),
            "{error}"
        );
    }
}

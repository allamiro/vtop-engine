//! `config_version` and strict key checking (#743, decision 7).
//!
//! Loading stays lenient through 0.x: most `config.yaml` sections ignore a
//! key they do not know, so a misspelt key silently falls back to its
//! default. Two additions make that visible without changing what loads:
//!
//! - an optional top-level `config_version` key. Absent means
//!   [`CONFIG_VERSION`]; a newer version than this binary supports is
//!   refused, naming both, instead of being read as if it were current;
//! - [`unknown_config_keys`] and [`unknown_streams_keys`], which list every
//!   key the typed parse ignored, for `vtopctl validate --strict`.
//!
//! The ignored keys come from the parse itself (`serde_ignored`), so no list
//! of known keys is kept here that could drift from the structs.

use crate::config::{StreamsConfig, VtopConfig};
use crate::errors::VtopError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The newest `config_version` this binary reads, and the version a config
/// without the key is read as. Raise it only for a change an older engine
/// would misread (a rename, a removal, a changed meaning), never for a new
/// key with a safe default: those need no version.
pub const CONFIG_VERSION: u32 = 1;

/// A key the typed parse ignored, with a suggestion when a known key at the
/// same level is close to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownKey {
    /// Dotted path from the document root, such as `engine.state_stor` or
    /// `streams[1].tenent`.
    pub path: String,
    /// The full path of the nearest known key at the same level, if one is
    /// close enough to be a likely misspelling.
    pub suggestion: Option<String>,
}

impl fmt::Display for UnknownKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path)?;
        if let Some(suggestion) = &self.suggestion {
            write!(f, " (did you mean `{suggestion}`?)")?;
        }
        Ok(())
    }
}

/// Refuse a `config_version` this binary does not read. Run before the typed
/// parse, so a config written for a newer engine is refused for its version
/// and not for whichever new key the older parse trips over first. A document
/// that is not a mapping is left to the typed parse, which reports it.
pub(crate) fn check_declared_version(text: &str) -> Result<(), VtopError> {
    #[derive(Deserialize)]
    struct Probe {
        #[serde(default)]
        config_version: Option<serde_yaml::Value>,
    }
    let Ok(Probe {
        config_version: Some(declared),
    }) = serde_yaml::from_str::<Probe>(text)
    else {
        return Ok(());
    };
    match declared {
        serde_yaml::Value::Null => Ok(()),
        serde_yaml::Value::Number(n) => match n.as_u64() {
            Some(0) => Err(VtopError::Config(format!(
                "config_version: 0 is not a version; versions start at 1 and this build \
                 reads up to {CONFIG_VERSION}"
            ))),
            Some(v) if v > u64::from(CONFIG_VERSION) => Err(VtopError::Config(format!(
                "config_version: {v} is newer than this build supports (it reads up to \
                 {CONFIG_VERSION}); upgrade vtopctl and the engine, or remove the key if \
                 the file is meant for this version"
            ))),
            Some(_) => Ok(()),
            None => Err(not_a_version(&n.to_string())),
        },
        other => {
            let found = serde_yaml::to_string(&other).unwrap_or_default();
            Err(not_a_version(found.trim()))
        }
    }
}

fn not_a_version(found: &str) -> VtopError {
    VtopError::Config(format!(
        "config_version must be a whole number from 1 to {CONFIG_VERSION}, found {found}"
    ))
}

/// Every key of a `config.yaml` document that [`VtopConfig`] would ignore.
/// The document must already load: a document the typed parse refuses has an
/// error to report first, and that error is returned here unchanged.
pub fn unknown_config_keys(text: &str) -> Result<Vec<UnknownKey>, VtopError> {
    unknown_keys::<VtopConfig>(text)
}

/// Every key of a `streams.yaml` document that [`StreamsConfig`] would
/// ignore, as `streams[<index>].<key>`.
pub fn unknown_streams_keys(text: &str) -> Result<Vec<UnknownKey>, VtopError> {
    unknown_keys::<StreamsConfig>(text)
}

#[derive(Debug, Clone)]
enum Segment {
    Key(String),
    Index(usize),
}

fn segments(path: &serde_ignored::Path<'_>, out: &mut Vec<Segment>) {
    use serde_ignored::Path;
    match path {
        Path::Root => {}
        Path::Seq { parent, index } => {
            segments(parent, out);
            out.push(Segment::Index(*index));
        }
        Path::Map { parent, key } => {
            segments(parent, out);
            out.push(Segment::Key(key.to_string()));
        }
        Path::Some { parent }
        | Path::NewtypeStruct { parent }
        | Path::NewtypeVariant { parent } => segments(parent, out),
    }
}

fn render(segments: &[Segment]) -> String {
    let mut out = String::new();
    for segment in segments {
        match segment {
            Segment::Key(key) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            Segment::Index(index) => out.push_str(&format!("[{index}]")),
        }
    }
    out
}

fn unknown_keys<T>(text: &str) -> Result<Vec<UnknownKey>, VtopError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let mut ignored: Vec<Vec<Segment>> = Vec::new();
    let parsed: T = serde_ignored::deserialize(serde_yaml::Deserializer::from_str(text), |path| {
        let mut found = Vec::new();
        segments(&path, &mut found);
        ignored.push(found);
    })?;
    // The parsed value, serialized, names the keys each level knows: a
    // suggestion needs no list kept apart from the structs. A key a struct
    // leaves out when unset is not offered, which only costs a suggestion.
    let known = serde_yaml::to_value(&parsed).ok();
    let mut unknown: Vec<UnknownKey> = ignored
        .into_iter()
        .map(|found| {
            let suggestion = known
                .as_ref()
                .and_then(|root| suggest(root, &found))
                .map(|nearest| {
                    let mut at = found[..found.len() - 1].to_vec();
                    at.push(Segment::Key(nearest));
                    render(&at)
                });
            UnknownKey {
                path: render(&found),
                suggestion,
            }
        })
        .collect();
    unknown.sort_by(|a, b| a.path.cmp(&b.path));
    unknown.dedup();
    Ok(unknown)
}

/// The known key at the level of `found`'s last segment that is nearest to
/// it, when it is near enough to be a misspelling.
fn suggest(root: &serde_yaml::Value, found: &[Segment]) -> Option<String> {
    let (Segment::Key(unknown), parents) = found.split_last()? else {
        return None;
    };
    let mut level = root;
    for segment in parents {
        level = match (segment, level) {
            (Segment::Key(key), serde_yaml::Value::Mapping(map)) => map.get(key.as_str())?,
            (Segment::Index(index), serde_yaml::Value::Sequence(items)) => items.get(*index)?,
            _ => return None,
        };
    }
    let serde_yaml::Value::Mapping(map) = level else {
        return None;
    };
    let wanted = normalize(unknown);
    let allowed = if unknown.chars().count() >= 5 { 2 } else { 1 };
    map.keys()
        .filter_map(|key| key.as_str())
        .filter(|candidate| *candidate != unknown)
        .map(|candidate| {
            let distance = if normalize(candidate) == wanted {
                0
            } else {
                edit_distance(&wanted, &normalize(candidate))
            };
            (distance, candidate)
        })
        .filter(|(distance, _)| *distance <= allowed)
        .min()
        .map(|(_, candidate)| candidate.to_owned())
}

fn normalize(key: &str) -> String {
    key.to_ascii_lowercase().replace('-', "_")
}

/// Edit distance over chars, with a swap of two neighbours counted as one
/// edit (optimal string alignment): `nmae` is one slip from `name`.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut grid = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in grid.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in grid[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut best = (grid[i - 1][j] + 1)
                .min(grid[i][j - 1] + 1)
                .min(grid[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                best = best.min(grid[i - 2][j - 2] + 1);
            }
            grid[i][j] = best;
        }
    }
    grid[a.len()][b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_distance_counts_single_edits() {
        assert_eq!(edit_distance("tenant", "tenant"), 0);
        assert_eq!(edit_distance("tenent", "tenant"), 1);
        assert_eq!(edit_distance("state_stor", "state_store"), 1);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("nmae", "name"), 1);
    }

    #[test]
    fn version_probe_names_both_versions() {
        assert!(check_declared_version("engine: {}\n").is_ok());
        assert!(check_declared_version("config_version:\n").is_ok());
        assert!(check_declared_version("config_version: 1\n").is_ok());
        let newer = check_declared_version(&format!("config_version: {}\n", CONFIG_VERSION + 1))
            .unwrap_err()
            .to_string();
        assert!(
            newer.contains(&format!("{}", CONFIG_VERSION + 1)),
            "{newer}"
        );
        assert!(newer.contains("newer"), "{newer}");
        assert!(check_declared_version("config_version: 0\n").is_err());
        assert!(check_declared_version("config_version: \"1\"\n").is_err());
        assert!(check_declared_version("config_version: 1.5\n").is_err());
        assert!(check_declared_version("config_version: -1\n").is_err());
        // Not a mapping: the typed parse reports it.
        assert!(check_declared_version("- a\n- b\n").is_ok());
    }
}

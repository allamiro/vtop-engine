//! A server's capability file, `tests/s3-compat/servers/<id>.yaml` (#722,
//! decision D2): for every capability, what the server is expected to do.
//! The expectation is reviewed in a PR; a run is compared with it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::capability::{Capability, Expectation, RefusalAt};

/// The variable that names the server a run is against.
pub const SERVER_ENV: &str = "VTOP_TEST_S3_SERVER";
/// The server a run is against when [`SERVER_ENV`] is unset: the lab's
/// source-built MinIO, which every live test ran against before #722.
pub const DEFAULT_SERVER: &str = "minio";
/// Overrides where the capability files are read from (for test binaries
/// run from another checkout than the one they were built in).
pub const SERVERS_DIR_ENV: &str = "VTOP_TEST_S3_SERVERS_DIR";

/// One server's capability file, parsed and checked.
#[derive(Debug, Clone)]
pub struct Server {
    pub id: String,
    /// A display name for the report.
    pub name: Option<String>,
    /// A gating server's leg is required: an `unknown` capability a case
    /// needs is refused on it (D11).
    pub gating: bool,
    /// The SDK finds the endpoint itself (AWS): `VTOP_TEST_S3_ENDPOINT` is
    /// not required. False for every lab server.
    pub sdk_default_endpoint: bool,
    expectations: BTreeMap<Capability, Expectation>,
    notes: BTreeMap<Capability, String>,
    /// The file it came from, named in every unexpected outcome.
    pub file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    id: String,
    #[serde(default)]
    name: Option<String>,
    gating: bool,
    #[serde(default)]
    sdk_default_endpoint: bool,
    capabilities: BTreeMap<String, RawState>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawState {
    Short(String),
    Long(RawLong),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLong {
    state: String,
    #[serde(default)]
    refusal: Option<String>,
    #[serde(default)]
    issue: Option<u32>,
    #[serde(default)]
    note: Option<String>,
}

/// Where the capability files live: [`SERVERS_DIR_ENV`], or the repository's
/// `tests/s3-compat/servers`.
pub fn servers_dir() -> PathBuf {
    match std::env::var_os(SERVERS_DIR_ENV) {
        Some(dir) => PathBuf::from(dir),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/s3-compat/servers"),
    }
}

impl Server {
    /// The server [`SERVER_ENV`] names (default [`DEFAULT_SERVER`]). Panics
    /// with the file and the reason when it cannot be read: a live run with
    /// no expectation to compare against proves nothing.
    pub fn from_env() -> Server {
        let id = std::env::var(SERVER_ENV).unwrap_or_else(|_| DEFAULT_SERVER.to_owned());
        Server::load(&id).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Read `<servers_dir>/<id>.yaml`.
    pub fn load(id: &str) -> Result<Server, String> {
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(format!(
                "{SERVER_ENV}={id:?} is not a server id (lower-case letters, digits and '-')"
            ));
        }
        let file = servers_dir().join(format!("{id}.yaml"));
        let text = std::fs::read_to_string(&file).map_err(|e| {
            format!(
                "{SERVER_ENV}={id}: cannot read its capability file {}: {e}; add the file \
                 (tests/s3-compat/README.md says how) or name another server",
                file.display()
            )
        })?;
        let server = Server::parse(&text, &file)?;
        if server.id != id {
            return Err(format!(
                "{} says id: {}, but it was loaded as {id}; the id and the file name must agree",
                file.display(),
                server.id
            ));
        }
        Ok(server)
    }

    /// Parse and check one file's text. Every capability must be listed,
    /// so a file states each expectation rather than leaving one implied.
    pub fn parse(text: &str, file: &Path) -> Result<Server, String> {
        let at = file.display();
        let raw: RawFile =
            serde_yaml::from_str(text).map_err(|e| format!("{at}: not a capability file: {e}"))?;
        let mut expectations = BTreeMap::new();
        let mut notes = BTreeMap::new();
        for (key, state) in raw.capabilities {
            let capability: Capability = key.parse().map_err(|e| format!("{at}: {e}"))?;
            let (expectation, note) =
                parse_state(state).map_err(|e| format!("{at}: capability {capability}: {e}"))?;
            expectations.insert(capability, expectation);
            if let Some(note) = note {
                notes.insert(capability, note);
            }
        }
        let missing: Vec<&str> = Capability::ALL
            .into_iter()
            .filter(|c| !expectations.contains_key(c))
            .map(Capability::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "{at}: no expectation for {}; list every capability (use unknown while it is \
                 being discovered)",
                missing.join(", ")
            ));
        }
        Ok(Server {
            id: raw.id,
            name: raw.name,
            gating: raw.gating,
            sdk_default_endpoint: raw.sdk_default_endpoint,
            expectations,
            notes,
            file: file.to_path_buf(),
        })
    }

    /// What this server is expected to do for `capability`.
    pub fn expectation(&self, capability: Capability) -> Expectation {
        // Every capability is present: `parse` refuses a file without one.
        self.expectations[&capability]
    }

    /// The note the file gives for `capability`, if any.
    pub fn note(&self, capability: Capability) -> Option<&str> {
        self.notes.get(&capability).map(String::as_str)
    }

    /// The file named in an unexpected outcome, relative to the repository
    /// when it is under it.
    pub fn file_hint(&self) -> String {
        format!("tests/s3-compat/servers/{}.yaml", self.id)
    }
}

fn parse_state(state: RawState) -> Result<(Expectation, Option<String>), String> {
    let (state, refusal, issue, note) = match state {
        RawState::Short(state) => (state, None, None, None),
        RawState::Long(l) => (l.state, l.refusal, l.issue, l.note),
    };
    let expectation = match state.as_str() {
        "pass" => Expectation::Pass,
        "unknown" => Expectation::Unknown,
        "not_applicable" => Expectation::NotApplicable,
        "unsupported" => {
            let refusal =
                match refusal.as_deref() {
                    Some("startup") => RefusalAt::Startup,
                    Some("first_batch") => RefusalAt::FirstBatch,
                    Some(other) => {
                        return Err(format!("refusal {other:?} is not startup or first_batch"))
                    }
                    None => return Err(
                        "unsupported needs refusal: startup or first_batch (where VTOP refuses \
                         it); a capability VTOP has no refusal for is not_applicable"
                            .into(),
                    ),
                };
            Expectation::Unsupported { refusal }
        }
        "xfail" => match issue {
            Some(issue) => Expectation::Xfail { issue },
            None => return Err("xfail needs the issue that tracks it (issue: N)".into()),
        },
        other => {
            return Err(format!(
                "state {other:?} is not pass, unsupported, xfail, unknown or not_applicable"
            ))
        }
    };
    if refusal.is_some() && !matches!(expectation, Expectation::Unsupported { .. }) {
        return Err("refusal is only for unsupported".into());
    }
    if issue.is_some() && !matches!(expectation, Expectation::Xfail { .. }) {
        return Err("issue is only for xfail".into());
    }
    Ok((expectation, note))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A complete file with every capability `pass` but the overrides.
    pub(crate) fn file_with(gating: bool, overrides: &[(&str, &str)]) -> String {
        let mut text = format!("id: test\ngating: {gating}\ncapabilities:\n");
        for c in Capability::ALL {
            let state = overrides
                .iter()
                .find(|(k, _)| *k == c.as_str())
                .map(|(_, v)| *v)
                .unwrap_or("pass");
            text.push_str(&format!("  {c}: {state}\n"));
        }
        text
    }

    fn parse(text: &str) -> Result<Server, String> {
        Server::parse(text, Path::new("tests/s3-compat/servers/test.yaml"))
    }

    #[test]
    fn a_complete_file_parses_every_form() {
        let server = parse(&file_with(
            false,
            &[
                (
                    "versioning",
                    "{ state: unsupported, refusal: startup, note: \"a stub\" }",
                ),
                (
                    "object_lock",
                    "{ state: unsupported, refusal: first_batch }",
                ),
                ("conditional_put", "{ state: xfail, issue: 722 }"),
                ("checksum_sha256_enforced", "unknown"),
                ("virtual_host", "not_applicable"),
            ],
        ))
        .unwrap();
        assert!(!server.gating);
        assert_eq!(
            server.expectation(Capability::Versioning),
            Expectation::Unsupported {
                refusal: RefusalAt::Startup
            }
        );
        assert_eq!(server.note(Capability::Versioning), Some("a stub"));
        assert_eq!(
            server.expectation(Capability::ObjectLock),
            Expectation::Unsupported {
                refusal: RefusalAt::FirstBatch
            }
        );
        assert_eq!(
            server.expectation(Capability::ConditionalPut),
            Expectation::Xfail { issue: 722 }
        );
        assert_eq!(
            server.expectation(Capability::ChecksumSha256Enforced),
            Expectation::Unknown
        );
        assert_eq!(
            server.expectation(Capability::VirtualHost),
            Expectation::NotApplicable
        );
        assert_eq!(server.expectation(Capability::Multipart), Expectation::Pass);
    }

    #[test]
    fn a_file_that_leaves_a_capability_out_is_refused_naming_it() {
        let text = file_with(true, &[]).replace("  conditional_put: pass\n", "");
        let err = parse(&text).unwrap_err();
        assert!(err.contains("no expectation for conditional_put"), "{err}");
    }

    #[test]
    fn a_misspelt_capability_or_state_is_refused() {
        let err = parse(&format!("{}  object_locks: pass\n", file_with(true, &[]))).unwrap_err();
        assert!(err.contains("unknown capability \"object_locks\""), "{err}");
        let err = parse(&file_with(true, &[("multipart", "passes")])).unwrap_err();
        assert!(err.contains("state \"passes\""), "{err}");
        let err = parse(&file_with(true, &[]).replace("gating", "gate")).unwrap_err();
        assert!(err.contains("not a capability file"), "{err}");
    }

    #[test]
    fn unsupported_needs_its_refusal_moment_and_xfail_its_issue() {
        let err = parse(&file_with(true, &[("versioning", "unsupported")])).unwrap_err();
        assert!(err.contains("unsupported needs refusal"), "{err}");
        let err = parse(&file_with(true, &[("versioning", "xfail")])).unwrap_err();
        assert!(err.contains("xfail needs the issue"), "{err}");
        let err = parse(&file_with(
            true,
            &[("versioning", "{ state: pass, refusal: startup }")],
        ))
        .unwrap_err();
        assert!(err.contains("refusal is only for unsupported"), "{err}");
    }

    #[test]
    fn the_id_and_the_file_name_must_agree() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("other.yaml"), file_with(true, &[])).unwrap();
        // Through the variable, as a test binary run elsewhere would.
        std::env::set_var(SERVERS_DIR_ENV, dir.path());
        let err = Server::load("other").unwrap_err();
        let missing = Server::load("nope").unwrap_err();
        std::env::remove_var(SERVERS_DIR_ENV);
        assert!(err.contains("id: test"), "{err}");
        assert!(
            missing.contains("cannot read its capability file"),
            "{missing}"
        );
        assert!(Server::load("../etc")
            .unwrap_err()
            .contains("not a server id"));
    }

    /// Every committed file parses, and its id is its file name.
    #[test]
    fn every_committed_capability_file_is_valid() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/s3-compat/servers");
        let mut seen = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            let server = Server::parse(&text, &path).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(
                Some(server.id.as_str()),
                path.file_stem().and_then(|s| s.to_str()),
                "{}",
                path.display()
            );
            seen += 1;
        }
        assert!(seen >= 1, "no capability file under {}", dir.display());
    }
}

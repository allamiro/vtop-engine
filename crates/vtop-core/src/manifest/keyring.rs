//! Manifest MAC keys by id (#685): which key signs new manifests, and which
//! key verifies a stored one.
//!
//! Manifests are immutable and pinned, so a rotation cannot re-sign the
//! archive: after one, the archive holds manifests signed by several keys for
//! as long as it is retained. Signing stays simple (one active key) and
//! verification is key-aware: a version 0.4 manifest names its key in
//! `manifest.mac_key_id`, and a reader verifies it with that key and no
//! other. It never tries every key: that would make retiring a key
//! meaningless and hide which key signed what.
//!
//! The key id exists only in version 0.4. A 0.3 reader re-encodes the decoded
//! struct before hashing, so it would drop the field and report the manifest
//! as tampered; a keyring therefore requires `manifest.write_version: "0.4"`
//! (refused at config load otherwise). A manifest with no id (every 0.1-0.3
//! manifest, and a 0.4 one written with `manifest_mac_key_env`) is verified
//! with the entry marked `legacy`.

use super::{ManifestMacKey, VtopManifest};
use crate::errors::VtopError;

/// The `key_id` label of a key that has no id (`manifest_mac_key_env`), and
/// of a manifest that names no id where no entry is marked `legacy`. Reserved:
/// no configured key may use it.
pub const KEY_LABEL_NONE: &str = "none";
/// The `key_id` label of a manifest naming an id this keyring does not hold.
/// The id itself comes from storage, so it is never a label. Reserved.
pub const KEY_LABEL_OTHER: &str = "other";
/// The longest key id accepted.
pub const MAX_KEY_ID_LEN: usize = 64;

/// Check a configured key id: 1 to [`MAX_KEY_ID_LEN`] characters from
/// `[A-Za-z0-9._-]`, and not one of the two reserved labels. Ids appear in
/// errors, runbooks and metric labels, so they stay short and printable.
pub fn validate_key_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > MAX_KEY_ID_LEN {
        return Err(format!(
            "key id {} must be 1 to {MAX_KEY_ID_LEN} characters",
            shown_id(id)
        ));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "key id {} may only contain letters, digits, '.', '_' and '-'",
            shown_id(id)
        ));
    }
    if id == KEY_LABEL_NONE || id == KEY_LABEL_OTHER {
        return Err(format!(
            "key id {id:?} is reserved: it is the metric label for {}",
            if id == KEY_LABEL_NONE {
                "a key with no id"
            } else {
                "an id this keyring does not hold"
            }
        ));
    }
    Ok(())
}

/// An id as it may appear in a message: quoted with control characters
/// escaped, and cut at [`MAX_KEY_ID_LEN`] characters. A manifest's id comes
/// from storage and lands in logs, so it is never printed raw.
pub(crate) fn shown_id(id: &str) -> String {
    if id.chars().count() > MAX_KEY_ID_LEN {
        let cut: String = id.chars().take(MAX_KEY_ID_LEN).collect();
        format!("{cut:?}...")
    } else {
        format!("{id:?}")
    }
}

/// Where a key was read from. Printed by `validate --print-effective` and the
/// startup line; the variable's name and the file's path are configuration,
/// and the value never leaves the [`ManifestMacKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySourceKind {
    /// An environment variable.
    Env,
    /// A file, such as a mounted secret.
    File,
}

impl KeySourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::File => "file",
        }
    }
}

/// One key the keyring holds.
#[derive(Clone)]
struct Entry {
    /// `None` only for the `manifest_mac_key_env` key, which writes no id.
    id: Option<String>,
    key: ManifestMacKey,
    source: KeySourceKind,
    legacy: bool,
}

/// A configured key, as [`ManifestKeyring::new`] takes it.
pub struct KeyringKey {
    pub id: String,
    pub key: ManifestMacKey,
    pub source: KeySourceKind,
    pub legacy: bool,
}

/// The manifest keys this process signs and verifies with.
///
/// Three shapes:
/// - **empty** ([`ManifestKeyring::default`]): no key. Manifests are written
///   unsigned and MACs are not checked, as with no `manifest_mac_key_env`;
/// - **one key with no id** ([`ManifestKeyring::single`]),
///   `manifest_mac_key_env`: it signs, writes no id, and verifies manifests
///   that name no id. Exactly the behaviour before #685;
/// - **a keyring** ([`ManifestKeyring::new`]), `manifest.mac`: the active key
///   signs and its id is written; every key verifies the manifests that name
///   it; the `legacy` key verifies manifests that name none; a retired
///   (tombstoned) id is refused by name.
///
/// No `Serialize`, and `Debug` prints ids only: the key material can never
/// reach a config dump, a log line or a metric.
#[derive(Clone, Default)]
pub struct ManifestKeyring {
    entries: Vec<Entry>,
    /// Index of the signing key in `entries`.
    active: Option<usize>,
    retired: Vec<String>,
}

impl std::fmt::Debug for ManifestKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestKeyring")
            .field("summary", &self.summary())
            .finish()
    }
}

impl ManifestKeyring {
    /// The `manifest_mac_key_env` shorthand: one key, active and legacy, that
    /// writes no key id.
    pub fn single(key: ManifestMacKey) -> Self {
        Self {
            entries: vec![Entry {
                id: None,
                key,
                source: KeySourceKind::Env,
                legacy: true,
            }],
            active: Some(0),
            retired: Vec::new(),
        }
    }

    /// The keyring from `Option<&ManifestMacKey>`: empty, or
    /// [`ManifestKeyring::single`]. For callers that hold one key.
    pub fn from_key(key: Option<&ManifestMacKey>) -> Self {
        key.cloned().map(Self::single).unwrap_or_default()
    }

    /// A keyring of named keys. Checked again here, whatever checked the
    /// config: `active` names a key; ids are well formed and unique across
    /// keys and tombstones; at most one key is `legacy`.
    pub fn new(
        active: &str,
        keys: Vec<KeyringKey>,
        retired: Vec<String>,
    ) -> Result<Self, VtopError> {
        let refuse = |why: String| VtopError::Config(format!("manifest.mac: {why}"));
        let mut seen = std::collections::BTreeSet::new();
        for id in keys
            .iter()
            .map(|k| k.id.as_str())
            .chain(retired.iter().map(String::as_str))
        {
            validate_key_id(id).map_err(&refuse)?;
            if !seen.insert(id) {
                return Err(refuse(format!(
                    "key id {id:?} appears more than once (a retired id may not also be a key)"
                )));
            }
        }
        if keys.iter().filter(|k| k.legacy).count() > 1 {
            return Err(refuse(
                "at most one key may be marked legacy: true (it verifies the manifests that \
                 name no key id)"
                    .into(),
            ));
        }
        let active_index = keys.iter().position(|k| k.id == active).ok_or_else(|| {
            refuse(if retired.iter().any(|r| r == active) {
                format!("active key id {active:?} is retired; a retired key cannot sign")
            } else {
                format!("active key id {active:?} is not one of manifest.mac.keys")
            })
        })?;
        Ok(Self {
            entries: keys
                .into_iter()
                .map(|k| Entry {
                    id: Some(k.id),
                    key: k.key,
                    source: k.source,
                    legacy: k.legacy,
                })
                .collect(),
            active: Some(active_index),
            retired,
        })
    }

    /// Whether any key is configured. Without one, manifests are unsigned and
    /// MACs are not checked.
    pub fn is_configured(&self) -> bool {
        !self.entries.is_empty()
    }

    /// The signing key and the id to write with it (`None` for the
    /// `manifest_mac_key_env` key).
    pub fn signing_key(&self) -> Option<(&ManifestMacKey, Option<&str>)> {
        self.active
            .map(|i| &self.entries[i])
            .map(|e| (&e.key, e.id.as_deref()))
    }

    /// The id of the signing key, when it has one.
    pub fn active_id(&self) -> Option<&str> {
        self.signing_key().and_then(|(_, id)| id)
    }

    /// Keys that verify but never sign, by id.
    pub fn verify_only_ids(&self) -> Vec<&str> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != self.active)
            .filter_map(|(_, e)| e.id.as_deref())
            .collect()
    }

    /// The tombstoned ids.
    pub fn retired_ids(&self) -> &[String] {
        &self.retired
    }

    /// Every `key_id` label this keyring can produce for a manifest it
    /// verified: each key's id, or [`KEY_LABEL_NONE`] for a key with none.
    /// Bounded by configuration, never by what storage holds.
    pub fn key_labels(&self) -> Vec<String> {
        self.entries.iter().map(entry_label).collect()
    }

    /// One line naming every key by id and source kind, which signs, which is
    /// legacy, and the tombstones. Never a key, a variable's value, or (for
    /// the keyring) a variable's name. Logged at startup.
    pub fn summary(&self) -> String {
        if self.entries.is_empty() {
            return "manifest MAC: no key configured; manifests are written unsigned and MACs \
                    are not checked (set manifest.mac or manifest_mac_key_env to sign them)"
                .into();
        }
        if self.entries.iter().all(|e| e.id.is_none()) {
            return "manifest MAC: one key from manifest_mac_key_env; it signs new manifests \
                    without a key id and verifies manifests that name none"
                .into();
        }
        let describe = |e: &Entry| {
            let mut traits = vec![e.source.as_str()];
            if e.legacy {
                traits.push("legacy");
            }
            format!("{} ({})", entry_label(e), traits.join(", "))
        };
        let active = self
            .active
            .map(|i| describe(&self.entries[i]))
            .unwrap_or_else(|| "none".into());
        let verify_only: Vec<String> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != self.active)
            .map(|(_, e)| describe(e))
            .collect();
        let list = |items: &[String]| {
            if items.is_empty() {
                "none".to_owned()
            } else {
                items.join(", ")
            }
        };
        format!(
            "manifest MAC keyring: active key {active}; verify-only: {}; retired: {}",
            list(&verify_only),
            list(&self.retired)
        )
    }

    /// What `validate --print-effective` prints for the keyring: ids and
    /// source kinds, never values or variable names.
    pub fn describe_config(&self) -> String {
        self.summary()
            .trim_start_matches("manifest MAC keyring: ")
            .to_owned()
    }

    /// The key an audit-chain link (#694) names: the key with that id, or,
    /// for a link that names none, the `manifest_mac_key_env` key (or the
    /// legacy key that took its place).
    pub fn chain_key(&self, id: Option<&str>) -> Option<&ManifestMacKey> {
        match id {
            Some(id) => self.by_id(id).map(|e| &e.key),
            None => self
                .entries
                .iter()
                .find(|e| e.id.is_none())
                .or_else(|| self.legacy())
                .map(|e| &e.key),
        }
    }

    fn by_id(&self, id: &str) -> Option<&Entry> {
        self.entries.iter().find(|e| e.id.as_deref() == Some(id))
    }

    fn legacy(&self) -> Option<&Entry> {
        self.entries.iter().find(|e| e.legacy)
    }

    /// Check `manifest`'s MAC over `bytes` (its version's authenticated
    /// bytes) with the key it names. See [`KeyResult`] for the outcomes.
    pub(crate) fn check(&self, manifest: &VtopManifest, bytes: &[u8]) -> Authentication {
        if self.entries.is_empty() {
            return Authentication {
                key: None,
                result: Ok(()),
            };
        }
        let named = manifest.manifest.mac_key_id.as_deref();
        if let (Some(id), [only]) = (named, self.entries.as_slice()) {
            if only.id.is_none() {
                return self.check_with_the_one_key(only, id, manifest, bytes);
            }
        }
        let entry = match named {
            Some(id) => match self.by_id(id) {
                Some(entry) => entry,
                None if self.retired.iter().any(|r| r == id) => {
                    return Authentication::failed(
                        id.to_owned(),
                        KeyResult::RetiredKey,
                        format!(
                            "manifest is unauthenticated: it names key id {}, which is retired \
                             (manifest.mac.retired); manifests signed with a withdrawn key no \
                             longer authenticate. Expected if the key was withdrawn; otherwise \
                             move the id back to manifest.mac.keys",
                            shown_id(id)
                        ),
                    )
                }
                None => {
                    return Authentication::failed(
                        KEY_LABEL_OTHER.to_owned(),
                        KeyResult::UnknownKey,
                        format!(
                            "manifest is unauthenticated: it names key id {}, which is not in \
                             this keyring (it holds {}); if the key is yours, add it to \
                             manifest.mac.keys as a verify-only entry",
                            shown_id(id),
                            self.held_ids()
                        ),
                    )
                }
            },
            None => match self.legacy() {
                Some(entry) => entry,
                None => {
                    return Authentication::failed(
                        KEY_LABEL_NONE.to_owned(),
                        KeyResult::UnknownKey,
                        "manifest is unauthenticated: it names no key id and no key in \
                         manifest.mac.keys is marked legacy: true; mark the key that signed \
                         manifests before key ids (the manifest_mac_key_env key) as legacy"
                            .to_owned(),
                    )
                }
            },
        };
        let label = entry_label(entry);
        // Which key the message names: the id the manifest gave, the legacy
        // key standing in for a manifest that gave none, or nothing for the
        // `manifest_mac_key_env` key, whose messages predate key ids.
        let which = match (named, entry.id.as_deref()) {
            (Some(id), _) => format!(" for key id {}", shown_id(id)),
            (None, Some(id)) => format!(
                " for the legacy key {} (the manifest names no key id)",
                shown_id(id)
            ),
            (None, None) => String::new(),
        };
        let Some(actual_hex) = manifest.manifest.mac.as_deref() else {
            let why = match entry.id {
                None => {
                    "manifest MAC is required because manifest_mac_key_env is configured".to_owned()
                }
                Some(_) => format!("manifest MAC is required{which}, but the manifest has none"),
            };
            return Authentication::failed(label, KeyResult::Failed, why);
        };
        let result = match decode_mac(actual_hex) {
            Err(why) => Err(why),
            Ok(actual) if constant_time_eq(entry.key.authenticate(bytes).as_bytes(), &actual) => {
                Ok(())
            }
            Ok(_) => Err(format!("manifest MAC verification failed{which}")),
        };
        match result {
            Ok(()) => Authentication {
                key: Some(KeyOutcome {
                    key_id: label,
                    result: KeyResult::Ok,
                }),
                result: Ok(()),
            },
            Err(why) => Authentication::failed(label, KeyResult::Failed, why),
        }
    }

    /// The `manifest_mac_key_env` key meets a manifest that names a key id
    /// (review of #703). It holds one key and no ids, so it verifies with
    /// that key and ignores the id, exactly as v0.8.0 does: an engine still
    /// on the single-key setting mid-rotation keeps verifying what keyring
    /// engines sign with the same key. One key is not a search. When it
    /// does not verify, the likely cause is another key, so it is reported
    /// as an unknown key, naming the id.
    fn check_with_the_one_key(
        &self,
        only: &Entry,
        id: &str,
        manifest: &VtopManifest,
        bytes: &[u8],
    ) -> Authentication {
        let verified = manifest
            .manifest
            .mac
            .as_deref()
            .ok_or(())
            .and_then(|hex| decode_mac(hex).map_err(drop))
            .is_ok_and(|actual| constant_time_eq(only.key.authenticate(bytes).as_bytes(), &actual));
        if verified {
            return Authentication {
                key: Some(KeyOutcome {
                    key_id: KEY_LABEL_NONE.to_owned(),
                    result: KeyResult::Ok,
                }),
                result: Ok(()),
            };
        }
        Authentication::failed(
            KEY_LABEL_OTHER.to_owned(),
            KeyResult::UnknownKey,
            format!(
                "manifest is unauthenticated: it names key id {}, and the one \
                 manifest_mac_key_env key does not verify it; if {} is another of your keys, \
                 configure manifest.mac with it as a verify-only entry",
                shown_id(id),
                shown_id(id)
            ),
        )
    }

    fn held_ids(&self) -> String {
        let ids: Vec<String> = self
            .entries
            .iter()
            .map(|e| {
                e.id.as_deref()
                    .map_or_else(|| "a key with no id".into(), shown_id)
            })
            .collect();
        ids.join(", ")
    }
}

fn entry_label(entry: &Entry) -> String {
    entry
        .id
        .clone()
        .unwrap_or_else(|| KEY_LABEL_NONE.to_owned())
}

fn decode_mac(actual_hex: &str) -> Result<Vec<u8>, String> {
    let actual = hex::decode(actual_hex).map_err(|_| "manifest MAC is not valid hex".to_owned())?;
    if actual.len() != 32 {
        return Err("manifest MAC must be exactly 32 bytes".into());
    }
    Ok(actual)
}

/// Compare without an early exit so a remote verification path does not leak
/// how many prefix bytes were correct.
fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
            == 0
}

/// What a MAC check found, as the `result` label of
/// `vtop_manifest_authentications_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyResult {
    /// The key the manifest names (or the legacy key) authenticated it.
    Ok,
    /// The key is held, and the MAC is missing, malformed or wrong.
    Failed,
    /// The manifest names an id the keyring does not hold, or names none
    /// where no key is `legacy`. Unauthenticated, not corrupt: the bytes may
    /// be intact and signed by a key this process was not given.
    UnknownKey,
    /// The manifest names a tombstoned id (`manifest.mac.retired`).
    RetiredKey,
}

impl KeyResult {
    pub const ALL: [KeyResult; 4] = [
        KeyResult::Ok,
        KeyResult::Failed,
        KeyResult::UnknownKey,
        KeyResult::RetiredKey,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::UnknownKey => "unknown_key",
            Self::RetiredKey => "retired_key",
        }
    }
}

/// Which key a MAC check used, and what it found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOutcome {
    /// A configured id, [`KEY_LABEL_NONE`] or [`KEY_LABEL_OTHER`]: never an
    /// id read from a manifest that the keyring does not hold.
    pub key_id: String,
    pub result: KeyResult,
}

/// The outcome of authenticating a manifest against a keyring.
#[derive(Debug)]
pub struct Authentication {
    /// The key used, when a MAC check ran: `None` with no key configured, or
    /// when the self-hash failed first.
    pub key: Option<KeyOutcome>,
    pub result: Result<(), VtopError>,
}

impl Authentication {
    fn failed(key_id: String, result: KeyResult, why: String) -> Self {
        Self {
            key: Some(KeyOutcome { key_id, result }),
            result: Err(VtopError::Manifest(why)),
        }
    }

    /// Count this check in `vtop_manifest_authentications_total`, where a
    /// metrics registry exists.
    pub fn count(&self) {
        if let (Some(key), Some(mx)) = (&self.key, crate::telemetry::metrics()) {
            mx.manifest_authentications_total
                .with_label_values(&[key.key_id.as_str(), key.result.as_str()])
                .inc();
        }
    }

    /// Whether the manifest authenticated.
    pub fn is_ok(&self) -> bool {
        self.result.is_ok()
    }

    pub fn into_result(self) -> Result<(), VtopError> {
        self.result
    }
}

//! The audit trail: one attributable record of every command that changes
//! engine or cluster state (#694).
//!
//! Logs explain behaviour and metrics show trends; an audit record proves who
//! changed state, when, from where, and what changed. Before this module the
//! evidence was spread across the ledger, the Raft log, the admin endpoint's
//! caller identity and ordinary log lines that can be filtered, sampled or
//! rotated away. Every process now builds the same record, [`AuditEvent`],
//! with a stable `audit_version`, and sends it to the same sinks:
//!
//! * **The log stream, always.** The event is written on the tracing target
//!   [`TARGET`] (`vtop::audit`). The log layer's filter admits that target
//!   whatever `RUST_LOG` or `--log-level` say ([`crate::logging`]), so an
//!   operator's own filter cannot silence it, and nothing here is sampled or
//!   rate-limited.
//! * **An append-only JSONL file, when [`LOG_ENV`] names one.** One record per
//!   line, synced before [`record`] returns. A set but unwritable path fails
//!   startup naming the path ([`init_file_sink_from_env`]); a write that fails
//!   later is counted and returned to the caller, which refuses a destructive
//!   action rather than run it unrecorded. The file is written here rather
//!   than by a second tracing layer because a layer cannot tell the caller its
//!   write failed, and that refusal needs to know.
//! * **The state store's `audit_events` table**, for the ledger commands; the
//!   caller writes that row (`vtop-state` owns the table, and this crate
//!   depends on neither the state store nor the metadata plane).
//!
//! `params` is an allowlist per action ([`Action::params`]), never a dump of a
//! command's fields, and every value and reason passes through [`redact`], so
//! a connection string, a key or a URL with a query cannot reach a record.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use prometheus::IntCounterVec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The record's version. Fields are only ever added to version 1, and a
/// reader ignores what it does not know; a change an older reader could
/// misread raises it.
pub const AUDIT_VERSION: u32 = 1;

/// The tracing target every audit event is written on.
pub const TARGET: &str = "vtop::audit";

/// Environment variable naming the append-only JSONL audit file.
pub const LOG_ENV: &str = "VTOP_AUDIT_LOG";

/// What was done. A closed set: a new action is an addition, never a rename,
/// and `vtop_audit_events_total{action}` takes exactly these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Action {
    /// `vtopctl replay`: recovery that moves incomplete batches to
    /// REPLAY_REQUIRED (or commits VERIFIED ones).
    #[serde(rename = "ledger.replay")]
    LedgerReplay,
    /// `vtopctl prune-ledger`: deletes committed ledger rows.
    #[serde(rename = "ledger.prune")]
    LedgerPrune,
    /// `vtopctl migrate`: applies the state-store schema.
    #[serde(rename = "ledger.migrate")]
    LedgerMigrate,
    /// `vtopctl scrub --cursor-file`: moves the scrub's resume cursor.
    #[serde(rename = "scrub.cursor")]
    ScrubCursor,
    /// `vtopctl meta init`: bootstraps the metadata group.
    #[serde(rename = "cluster.init")]
    ClusterInit,
    /// `vtopctl meta add-learner` / `change-membership`, and the membership
    /// entry every replica applies.
    #[serde(rename = "cluster.membership")]
    ClusterMembership,
    /// `vtopctl meta enable-features`: raises the cluster feature level.
    #[serde(rename = "cluster.enable_features")]
    ClusterEnableFeatures,
    /// `vtopctl meta create-topic`.
    #[serde(rename = "cluster.create_topic")]
    ClusterCreateTopic,
    /// Any other audited cluster-scoped command; `params.command` names it.
    #[serde(rename = "cluster.propose")]
    ClusterPropose,
    /// `vtopctl node repair`, and the serving leader's side of a transfer.
    #[serde(rename = "node.repair")]
    NodeRepair,
    /// `vtopctl node reconfigure-range`: rewrites a range's roll thresholds.
    #[serde(rename = "node.reconfigure_range")]
    NodeReconfigureRange,
    /// `vtopctl node promote-unproven`: the operator's attestation (#636).
    #[serde(rename = "promotion.attest")]
    PromotionAttest,
}

impl Action {
    /// Every action, for label sets and tests.
    pub const ALL: [Action; 12] = [
        Action::LedgerReplay,
        Action::LedgerPrune,
        Action::LedgerMigrate,
        Action::ScrubCursor,
        Action::ClusterInit,
        Action::ClusterMembership,
        Action::ClusterEnableFeatures,
        Action::ClusterCreateTopic,
        Action::ClusterPropose,
        Action::NodeRepair,
        Action::NodeReconfigureRange,
        Action::PromotionAttest,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Action::LedgerReplay => "ledger.replay",
            Action::LedgerPrune => "ledger.prune",
            Action::LedgerMigrate => "ledger.migrate",
            Action::ScrubCursor => "scrub.cursor",
            Action::ClusterInit => "cluster.init",
            Action::ClusterMembership => "cluster.membership",
            Action::ClusterEnableFeatures => "cluster.enable_features",
            Action::ClusterCreateTopic => "cluster.create_topic",
            Action::ClusterPropose => "cluster.propose",
            Action::NodeRepair => "node.repair",
            Action::NodeReconfigureRange => "node.reconfigure_range",
            Action::PromotionAttest => "promotion.attest",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.as_str() == value)
    }

    /// The parameters an event of this action may carry: the allowlist.
    /// Anything else handed to [`AuditEvent::param`] is dropped. Each name
    /// here is a value an auditor needs and that cannot carry a secret: a
    /// connection string, a key, a key variable's value or a URL never
    /// appears on any list.
    pub fn params(self) -> &'static [&'static str] {
        match self {
            Action::LedgerReplay => &["batch_id"],
            Action::LedgerPrune => &["older_than_days", "limit", "digest_dir"],
            // The backend kind only: the connection string carries the
            // database password.
            Action::LedgerMigrate => &["backend"],
            Action::ScrubCursor => &["cursor_file", "limit", "metrics_file"],
            Action::ClusterInit => &["members"],
            Action::ClusterMembership => &[
                "operation",
                "node_id",
                "voters",
                "retain_removed_as_learners",
            ],
            Action::ClusterEnableFeatures => &["level", "command"],
            Action::ClusterCreateTopic => &["name", "topic_uuid", "root_range_uuid", "command"],
            Action::ClusterPropose => &[
                "command",
                "topic_uuid",
                "range_uuid",
                "segment_uuid",
                "node_uuid",
                "state",
                "from_node",
                "to_node",
                "members",
                "fencing_epoch",
                "key_id",
                "failure_domain",
                "unarchived_deletion_allowed",
            ],
            Action::NodeRepair => &[
                "from",
                "into",
                "fencing_epoch",
                "seal_tail",
                "side",
                "request",
            ],
            Action::NodeReconfigureRange => &[
                "data_dir",
                "max_record_bytes",
                "max_group_bytes",
                "max_segment_bytes",
                "max_segment_records",
            ],
            Action::PromotionAttest => &[
                "topic_uuid",
                "range_uuid",
                "replica_node_uuid",
                "operator",
                "command",
            ],
        }
    }

    /// Whether the action writes an `attempt` record before it acts and is
    /// refused when that record cannot be written: the ledger commands that
    /// delete, rewind or reshape what the engine will do (D8).
    pub fn writes_attempt_first(self) -> bool {
        matches!(
            self,
            Action::LedgerReplay | Action::LedgerPrune | Action::LedgerMigrate
        )
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How it ended. `vtop_audit_events_total{outcome}` takes exactly these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Written before a destructive action runs; its terminal record follows
    /// with `correlation.attempt_id` naming it.
    Attempt,
    Success,
    Failure,
    /// Refused before it ran: by an authorization policy, by a check, or
    /// because its attempt record could not be written.
    Refused,
}

impl Outcome {
    pub const ALL: [Outcome; 4] = [
        Outcome::Attempt,
        Outcome::Success,
        Outcome::Failure,
        Outcome::Refused,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Attempt => "attempt",
            Outcome::Success => "success",
            Outcome::Failure => "failure",
            Outcome::Refused => "refused",
        }
    }
}

/// Where in the command's path the record was written. A cluster command is
/// recorded by the client that sent it, by the admin endpoint that
/// authorized it (which knows the certificate identity) and by every replica
/// that applied it (which knows the log index); the request id joins them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// A command that acted itself: the ledger and local node commands.
    Local,
    /// A client that asked a cluster to act.
    Client,
    /// The endpoint that authorized (or refused) the request.
    Edge,
    /// A replica applying the committed command.
    Applied,
}

/// Who the actor is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    /// A local process: its numeric uid, user name and host.
    Os,
    /// A verified client certificate on the admin endpoint (an operator).
    Certificate,
    /// A node, by the identity its certificate (or a replica plane) carries.
    Node,
    /// No identity at all: a plaintext endpoint.
    Anonymous,
}

/// The actor, with how each part was learnt. `source` says how strong the
/// identity is: `os` is the process's uid as the kernel reports it (as strong
/// as the host's logins, and not authentication), `certificate` a verified
/// mTLS identity, `claimed` a value the caller asserted, `raft` the replica
/// that applied a command (the requester is in the edge record), and `none`
/// no identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub kind: ActorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// How `name` was learnt: `passwd` (the uid's entry in /etc/passwd),
    /// `env` (`$USER` or `$LOGNAME`, which any process can set),
    /// `certificate`, `node_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// `$SUDO_USER` when set: who ran sudo, as sudo claims it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sudo_user: Option<String>,
    pub source: String,
}

impl Actor {
    /// The local process's actor: uid from the kernel, the uid's user name
    /// from /etc/passwd (else `$USER` / `$LOGNAME`, labelled `env`), the host
    /// name, and `$SUDO_USER` when present.
    pub fn local() -> Self {
        Self::local_from(&|name| std::env::var(name).ok(), passwd_name)
    }

    fn local_from(env: &dyn Fn(&str) -> Option<String>, passwd: fn(u32) -> Option<String>) -> Self {
        let uid = rustix::process::getuid().as_raw();
        let (name, name_source) = match passwd(uid) {
            Some(name) => (Some(name), Some("passwd")),
            None => match env("USER").or_else(|| env("LOGNAME")) {
                Some(name) if !name.trim().is_empty() => (Some(name), Some("env")),
                _ => (None, None),
            },
        };
        Actor {
            kind: ActorKind::Os,
            uid: Some(uid),
            name: name.map(|n| clip(&redact(&n))),
            name_source: name_source.map(str::to_owned),
            host: Some(clip(&host_name())),
            sudo_user: env("SUDO_USER")
                .filter(|s| !s.trim().is_empty())
                .map(|s| clip(&redact(&s))),
            source: "os".to_owned(),
        }
    }

    /// A caller identified by a verified certificate's Common Name.
    pub fn certificate(kind: ActorKind, name: &str) -> Self {
        Actor {
            kind,
            uid: None,
            name: Some(clip(&redact(name))),
            name_source: Some("certificate".to_owned()),
            host: None,
            sudo_user: None,
            source: "certificate".to_owned(),
        }
    }

    /// A node that only declared who it is (a plaintext plane).
    pub fn claimed_node(name: &str) -> Self {
        Actor {
            kind: ActorKind::Node,
            uid: None,
            name: Some(clip(&redact(name))),
            name_source: Some("node_id".to_owned()),
            host: None,
            sudo_user: None,
            source: "claimed".to_owned(),
        }
    }

    /// A replica applying a committed command: the record's actor is the
    /// replica; the requester is in the edge record with the same request id.
    pub fn replica(name: &str) -> Self {
        Actor {
            kind: ActorKind::Node,
            uid: None,
            name: Some(clip(&redact(name))),
            name_source: Some("node_id".to_owned()),
            host: None,
            sudo_user: None,
            source: "raft".to_owned(),
        }
    }

    /// No identity: a request on a plaintext endpoint.
    pub fn anonymous() -> Self {
        Actor {
            kind: ActorKind::Anonymous,
            uid: None,
            name: None,
            name_source: None,
            host: None,
            sudo_user: None,
            source: "none".to_owned(),
        }
    }

    /// A short description for a log line: `uid 1000 (alice@h1)`,
    /// `certificate "ops-bob"`, `anonymous`.
    pub fn describe(&self) -> String {
        match self.kind {
            ActorKind::Os => {
                let who = match (&self.name, &self.host) {
                    (Some(name), Some(host)) => format!(" ({name}@{host})"),
                    (Some(name), None) => format!(" ({name})"),
                    (None, Some(host)) => format!(" (@{host})"),
                    (None, None) => String::new(),
                };
                let uid = self.uid.map_or_else(|| "?".to_owned(), |u| u.to_string());
                let sudo = self
                    .sudo_user
                    .as_deref()
                    .map(|s| format!(" via sudo from {s}"))
                    .unwrap_or_default();
                format!("uid {uid}{who}{sudo}")
            }
            ActorKind::Certificate => {
                format!("certificate {:?}", self.name.as_deref().unwrap_or(""))
            }
            ActorKind::Node => format!(
                "node {}{}",
                self.name.as_deref().unwrap_or("?"),
                match self.source.as_str() {
                    "claimed" => " (declared, not verified)",
                    "raft" => " (applying replica)",
                    _ => "",
                }
            ),
            ActorKind::Anonymous => "an unidentified caller (plaintext endpoint)".to_owned(),
        }
    }
}

/// What was acted on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    /// `ledger`, `batch`, `cluster`, `topic`, `range`, `node`, `cursor`,
    /// `data_dir`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl Target {
    pub fn new(kind: &str, id: Option<String>) -> Self {
        Target {
            kind: kind.to_owned(),
            id: id.map(|i| clip(&redact(&i))),
        }
    }
}

/// What ties this record to the others for the same command.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Correlation {
    /// The command's request id: a cluster command's envelope id, or a local
    /// command's own id. The client, edge and applied records of one cluster
    /// command share it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// On a terminal record, the `audit_id` of its `attempt` record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// The OpenTelemetry trace the command ran in (#683), when one was
    /// exported; the same id the JSON log lines of the command carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    /// The metadata log index of an applied (or edge-committed) command.
    /// Dedupe applied records on it (one per replica that applied the entry
    /// through its state machine; none for entries a snapshot brought).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_index: Option<u64>,
}

/// One audit record, `audit_version` 1.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub audit_version: u32,
    pub audit_id: String,
    /// RFC 3339, UTC, milliseconds.
    pub time: String,
    pub actor: Actor,
    pub action: Action,
    pub stage: Stage,
    pub target: Target,
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    pub outcome: Outcome,
    /// Why, for `failure` and `refused`; redacted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub before: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub after: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub correlation: Correlation,
    /// The engine's name, or the node that wrote the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
}

impl AuditEvent {
    /// A new record, timed now, with a fresh `audit_id`.
    pub fn new(
        action: Action,
        stage: Stage,
        actor: Actor,
        target: Target,
        outcome: Outcome,
    ) -> Self {
        AuditEvent {
            audit_version: AUDIT_VERSION,
            audit_id: Uuid::new_v4().to_string(),
            time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            actor,
            action,
            stage,
            target,
            params: BTreeMap::new(),
            outcome,
            reason: None,
            before: BTreeMap::new(),
            after: BTreeMap::new(),
            correlation: Correlation::default(),
            engine: None,
        }
    }

    /// Add a parameter, if `name` is on the action's allowlist; the value is
    /// redacted. A name off the list is dropped (and fails a debug build's
    /// tests, so a new parameter is a deliberate edit of the list).
    pub fn param(mut self, name: &str, value: impl std::fmt::Display) -> Self {
        if self.action.params().contains(&name) {
            self.params
                .insert(name.to_owned(), clip(&redact(&value.to_string())));
        } else {
            debug_assert!(
                false,
                "audit parameter {name:?} is not on {}'s allowlist",
                self.action
            );
        }
        self
    }

    /// The reason a record failed or was refused; redacted.
    pub fn reason(mut self, reason: impl std::fmt::Display) -> Self {
        self.reason = Some(clip(&redact(&reason.to_string())));
        self
    }

    pub fn before(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.before
            .insert(name.to_owned(), redact_json(value.into()));
        self
    }

    pub fn after(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.after
            .insert(name.to_owned(), redact_json(value.into()));
        self
    }

    pub fn request_id(mut self, id: impl std::fmt::Display) -> Self {
        self.correlation.request_id = Some(clip(&id.to_string()));
        self
    }

    pub fn log_index(mut self, index: u64) -> Self {
        self.correlation.log_index = Some(index);
        self
    }

    pub fn engine(mut self, engine: impl std::fmt::Display) -> Self {
        self.engine = Some(clip(&redact(&engine.to_string())));
        self
    }

    /// The terminal record for this attempt: a new id and time, the same
    /// actor, action, target, parameters and request id, and
    /// `correlation.attempt_id` naming this record.
    pub fn terminal(&self, outcome: Outcome) -> Self {
        let mut next = AuditEvent::new(
            self.action,
            self.stage,
            self.actor.clone(),
            self.target.clone(),
            outcome,
        );
        next.params = self.params.clone();
        next.engine = self.engine.clone();
        next.correlation.request_id = self.correlation.request_id.clone();
        if self.outcome == Outcome::Attempt {
            next.correlation.attempt_id = Some(self.audit_id.clone());
        }
        next
    }

    /// The same record with another outcome: for a record built before its
    /// outcome was known (a client's, filled in from the reply).
    pub fn with_outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// The record as one JSON line (no newline): the exact text the file
    /// sink and the ledger row hold.
    pub fn to_json(&self) -> String {
        // Serialising plain data with string keys cannot fail.
        serde_json::to_string(self).unwrap_or_else(|e| {
            format!(r#"{{"audit_version":{AUDIT_VERSION},"encode_error":"{e}"}}"#)
        })
    }

    /// One line for an operator: what happened, by whom, on what.
    pub fn summary(&self) -> String {
        let target = match &self.target.id {
            Some(id) => format!("{} {id}", self.target.kind),
            None => self.target.kind.clone(),
        };
        let mut line = format!(
            "audit: {} {} by {} on {target}",
            self.action,
            self.outcome.as_str(),
            self.actor.describe()
        );
        if let Some(reason) = &self.reason {
            line.push_str(": ");
            line.push_str(reason);
        }
        line
    }
}

/// Which sink a write failed on; `vtop_audit_write_failures_total{sink}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sink {
    Log,
    File,
    Ledger,
}

impl Sink {
    pub const ALL: [Sink; 3] = [Sink::Log, Sink::File, Sink::Ledger];

    pub fn as_str(self) -> &'static str {
        match self {
            Sink::Log => "log",
            Sink::File => "file",
            Sink::Ledger => "ledger",
        }
    }
}

/// An audit sink that could not be opened or written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError {
    /// `VTOP_AUDIT_LOG` names a file that cannot be opened for appending.
    Open { path: PathBuf, error: String },
    /// A record could not be appended to the file.
    Write { path: PathBuf, error: String },
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditError::Open { path, error } => write!(
                f,
                "{LOG_ENV}={} cannot be opened for appending ({error}). Nothing was run. Fix \
                 the path or its permissions, or unset {LOG_ENV} to keep the audit trail in \
                 the log stream only",
                path.display()
            ),
            AuditError::Write { path, error } => write!(
                f,
                "the audit record could not be appended to {LOG_ENV}={} ({error}); it is in \
                 the log stream (target {TARGET})",
                path.display()
            ),
        }
    }
}

impl std::error::Error for AuditError {}

/// What [`record`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded {
    pub audit_id: String,
    /// The JSON line written, for a ledger row.
    pub json: String,
}

struct FileSink {
    path: PathBuf,
    file: File,
}

/// The process's sinks and counters. One global ([`record`]); tests build
/// their own so they neither see nor disturb each other.
pub struct AuditSink {
    file: Mutex<Option<FileSink>>,
    counters: OnceLock<(IntCounterVec, IntCounterVec)>,
}

impl Default for AuditSink {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditSink {
    pub const fn new() -> Self {
        Self {
            file: Mutex::new(None),
            counters: OnceLock::new(),
        }
    }

    /// Append to `path` from now on. Fails, naming the path, when it cannot
    /// be opened for appending; created with owner-only permissions.
    pub fn open_file(&self, path: &Path) -> Result<(), AuditError> {
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|e| AuditError::Open {
            path: path.to_owned(),
            error: e.to_string(),
        })?;
        *self.file.lock().unwrap_or_else(|e| e.into_inner()) = Some(FileSink {
            path: path.to_owned(),
            file,
        });
        Ok(())
    }

    /// The file this sink appends to, if any.
    pub fn file_path(&self) -> Option<PathBuf> {
        self.file
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|f| f.path.clone())
    }

    /// Count events and write failures into these (both registered by the
    /// process: `vtop_audit_events_total{action,outcome}` and
    /// `vtop_audit_write_failures_total{sink}`). The first call wins.
    pub fn set_counters(&self, events: IntCounterVec, failures: IntCounterVec) {
        let _ = self.counters.set((events, failures));
    }

    /// Count a write failure on `sink` (the ledger's is reported by the
    /// caller that writes the row).
    pub fn count_failure(&self, sink: Sink) {
        if let Some((_, failures)) = self.counters.get() {
            failures.with_label_values(&[sink.as_str()]).inc();
        }
    }

    /// Write `event` to the log stream and, when configured, the file.
    ///
    /// The log event is always written. `Err` means the file write failed:
    /// the record is in the log stream but not the file; the caller decides
    /// whether the action may go on.
    pub fn record(&self, event: &AuditEvent) -> Result<Recorded, AuditError> {
        let mut event = event.clone();
        if event.correlation.trace_id.is_none() {
            if let Some((trace_id, span_id)) = crate::otel::current_ids() {
                event.correlation.trace_id = Some(trace_id);
                event.correlation.span_id = Some(span_id);
            }
        }
        let json = event.to_json();
        emit_log(&event, &json);
        if let Some((events, _)) = self.counters.get() {
            events
                .with_label_values(&[event.action.as_str(), event.outcome.as_str()])
                .inc();
        }
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sink) = guard.as_mut() {
            let written = sink
                .file
                .write_all(format!("{json}\n").as_bytes())
                .and_then(|()| sink.file.sync_data());
            if let Err(e) = written {
                let path = sink.path.clone();
                drop(guard);
                self.count_failure(Sink::File);
                tracing::error!(
                    target: "vtop::audit",
                    audit_sink_error = true,
                    audit_id = %event.audit_id,
                    action = event.action.as_str(),
                    path = %path.display(),
                    error = %e,
                    "the audit record could not be appended to the audit file; it is in this \
                     log stream only. Fix the file (disk space, permissions) and check {LOG_ENV}"
                );
                return Err(AuditError::Write {
                    path,
                    error: e.to_string(),
                });
            }
        }
        Ok(Recorded {
            audit_id: event.audit_id,
            json,
        })
    }
}

/// The record on the log stream: WARN for a refusal or a failure, INFO
/// otherwise, never sampled or rate-limited. The full record is in `record`,
/// the fields an operator filters on beside it, and the one-line account in
/// `summary`. The message itself is only the action and the outcome, both
/// from closed sets: a log pipeline that promotes the message to an index
/// label (the lab's Alloy does) must not get one value per actor or target.
fn emit_log(event: &AuditEvent, json: &str) {
    let summary = event.summary();
    let (action, outcome) = (event.action.as_str(), event.outcome.as_str());
    match event.outcome {
        Outcome::Refused | Outcome::Failure => tracing::warn!(
            target: "vtop::audit",
            audit_version = event.audit_version,
            audit_id = %event.audit_id,
            action,
            outcome,
            actor = %event.actor.describe(),
            summary = %summary,
            record = %json,
            "audit: {action} {outcome}"
        ),
        Outcome::Attempt | Outcome::Success => tracing::info!(
            target: "vtop::audit",
            audit_version = event.audit_version,
            audit_id = %event.audit_id,
            action,
            outcome,
            actor = %event.actor.describe(),
            summary = %summary,
            record = %json,
            "audit: {action} {outcome}"
        ),
    }
}

static GLOBAL: AuditSink = AuditSink::new();

/// The process's sink.
pub fn sink() -> &'static AuditSink {
    &GLOBAL
}

/// Write `event` through the process's sink. See [`AuditSink::record`].
pub fn record(event: &AuditEvent) -> Result<Recorded, AuditError> {
    GLOBAL.record(event)
}

/// Open the file [`LOG_ENV`] names, if it names one. A set but empty value is
/// refused: it is a deployment that meant to name a file and did not.
pub fn init_file_sink_from_env() -> Result<Option<PathBuf>, AuditError> {
    init_file_sink_from(std::env::var_os(LOG_ENV))
}

fn init_file_sink_from(value: Option<std::ffi::OsString>) -> Result<Option<PathBuf>, AuditError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty() {
        return Err(AuditError::Open {
            path,
            error: "the variable is set but empty".to_owned(),
        });
    }
    if GLOBAL.file_path().as_deref() == Some(path.as_path()) {
        return Ok(Some(path));
    }
    GLOBAL.open_file(&path)?;
    Ok(Some(path))
}

/// Count into these; see [`AuditSink::set_counters`].
pub fn set_counters(events: IntCounterVec, failures: IntCounterVec) {
    GLOBAL.set_counters(events, failures);
}

/// Count a write failure on `sink` in the process's counters.
pub fn count_failure(sink: Sink) {
    GLOBAL.count_failure(sink);
}

/// The two counters, built and registered into `registry` and installed as
/// the process's: for a process whose catalogue does not already define them
/// (`vtop-node`; the engine's is `vtop_core::telemetry`).
pub fn register_counters(registry: &prometheus::Registry) -> Result<(), prometheus::Error> {
    let events = IntCounterVec::new(
        prometheus::Opts::new("audit_events_total", EVENTS_HELP),
        &["action", "outcome"],
    )?;
    let failures = IntCounterVec::new(
        prometheus::Opts::new("audit_write_failures_total", FAILURES_HELP),
        &["sink"],
    )?;
    registry.register(Box::new(events.clone()))?;
    registry.register(Box::new(failures.clone()))?;
    set_counters(events, failures);
    Ok(())
}

/// Help text of `vtop_audit_events_total`, shared with the engine's catalogue.
pub const EVENTS_HELP: &str = "Audit records written, by action (the closed set in docs/operate/auditing.md) and outcome (attempt, success, failure, refused)";
/// Help text of `vtop_audit_write_failures_total`.
pub const FAILURES_HELP: &str = "Audit records that could not be written to a sink (log, file, ledger); a destructive ledger command is refused when its attempt record cannot be written";

/// A line of an audit file (or a ledger row), as read back.
#[derive(Debug)]
pub enum ReadRecord {
    /// A version 1 record this build understands.
    V1(Box<AuditEvent>),
    /// Valid JSON this build cannot fully read: a newer `audit_version`, or a
    /// version 1 record with an action or value from a newer release. Shown
    /// raw with a warning, never dropped.
    Unknown { raw: serde_json::Value, why: String },
    /// Not JSON at all.
    Malformed { line: String, error: String },
}

/// Read one record.
pub fn read_record(line: &str) -> ReadRecord {
    let raw: serde_json::Value = match serde_json::from_str(line) {
        Ok(raw) => raw,
        Err(e) => {
            return ReadRecord::Malformed {
                line: line.to_owned(),
                error: e.to_string(),
            }
        }
    };
    let version = raw.get("audit_version").and_then(serde_json::Value::as_u64);
    match version {
        Some(v) if v == u64::from(AUDIT_VERSION) => {
            match serde_json::from_value::<AuditEvent>(raw.clone()) {
                Ok(event) => ReadRecord::V1(Box::new(event)),
                Err(e) => ReadRecord::Unknown {
                    raw,
                    why: format!(
                        "audit_version 1 with a value this build does not know ({e}); written by \
                         a newer release"
                    ),
                },
            }
        }
        Some(v) if v > u64::from(AUDIT_VERSION) => ReadRecord::Unknown {
            raw,
            why: format!(
                "audit_version {v} is newer than this build reads (up to {AUDIT_VERSION}); \
                 upgrade vtopctl to read it fully"
            ),
        },
        _ => ReadRecord::Unknown {
            raw,
            why: "no audit_version this build reads".to_owned(),
        },
    }
}

/// A value fit for an audit record: [`crate::otel::redact`] (URLs cut to
/// their host, long hex runs, access key ids), and the value of every
/// `name=value` pair whose name says it is a secret (a libpq keyword
/// connection string's `password=`, a `token=`).
pub fn redact(value: &str) -> String {
    let value = crate::otel::redact(value);
    value
        .split_inclusive(char::is_whitespace)
        .map(|token| {
            let (word, space) = match token.find(char::is_whitespace) {
                Some(at) => token.split_at(at),
                None => (token, ""),
            };
            match word.split_once('=') {
                Some((name, _)) if secret_name(name) => format!("{name}=<redacted>{space}"),
                _ => token.to_owned(),
            }
        })
        .collect()
}

fn secret_name(name: &str) -> bool {
    let name = name
        .trim_start_matches(['-', '?', '&'])
        .to_ascii_lowercase();
    [
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "key",
        "credential",
        "signature",
    ]
    .iter()
    .any(|s| name.contains(s))
}

fn redact_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => serde_json::Value::String(clip(&redact(&s))),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(redact_json).collect())
        }
        serde_json::Value::Object(map) => {
            serde_json::Value::Object(map.into_iter().map(|(k, v)| (k, redact_json(v))).collect())
        }
        other => other,
    }
}

/// The longest value a record keeps, in bytes: a record is evidence, not a
/// dump, and an unbounded value (a path, a topic name from a client) must not
/// make one line unbounded.
pub const MAX_VALUE_BYTES: usize = 1024;

fn clip(value: &str) -> String {
    if value.len() <= MAX_VALUE_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_VALUE_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
}

fn host_name() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

/// The user name /etc/passwd gives `uid`, read without libc's resolver (no
/// `unsafe`); `None` where the file has no entry (a directory service).
fn passwd_name(uid: u32) -> Option<String> {
    let text = std::fs::read_to_string("/etc/passwd").ok()?;
    passwd_lookup(&text, uid)
}

fn passwd_lookup(text: &str, uid: u32) -> Option<String> {
    text.lines().find_map(|line| {
        let mut fields = line.split(':');
        let name = fields.next()?;
        let _password = fields.next()?;
        let entry_uid: u32 = fields.next()?.parse().ok()?;
        (entry_uid == uid && !name.is_empty() && !name.starts_with('#')).then(|| name.to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    /// Log lines captured in memory.
    #[derive(Clone, Default)]
    struct Captured(std::sync::Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn prune_event() -> AuditEvent {
        AuditEvent::new(
            Action::LedgerPrune,
            Stage::Local,
            Actor::local(),
            Target::new("ledger", None),
            Outcome::Attempt,
        )
        .param("older_than_days", 30)
        .param("limit", 500)
    }

    /// The production log layer (the filter `logging` installs), JSON, into
    /// memory, with `rust_log` as `RUST_LOG`.
    fn production_logs(rust_log: &str) -> (Captured, tracing::subscriber::DefaultGuard) {
        let captured = Captured::default();
        let layer = crate::logging::log_layer_with_writer(
            "info",
            Some(rust_log.to_owned()),
            true,
            false,
            captured.clone(),
        );
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        (captured, guard)
    }

    /// The plan's subscriber test: under `RUST_LOG=error` an INFO line is
    /// dropped, and every one of ten audit records still reaches the log.
    #[test]
    fn audit_records_survive_an_operator_filter_of_error() {
        let (captured, _guard) = production_logs("error");
        let sink = AuditSink::new();
        tracing::info!("an ordinary INFO line the filter drops");
        for _ in 0..10 {
            sink.record(&prune_event()).unwrap();
        }
        let text = captured.text();
        assert_eq!(
            text.matches("\"target\":\"vtop::audit\"").count(),
            10,
            "every audit record must pass RUST_LOG=error; got:\n{text}"
        );
        assert!(!text.contains("an ordinary INFO line"), "{text}");
    }

    /// The message is the action and the outcome only (a pipeline may index
    /// it); who and what are in the fields.
    #[test]
    fn the_message_carries_only_bounded_values() {
        let (captured, _guard) = production_logs("info");
        AuditSink::new().record(&prune_event().reason("x")).unwrap();
        let line: serde_json::Value =
            serde_json::from_str(captured.text().lines().next().unwrap()).unwrap();
        assert_eq!(line["fields"]["message"], "audit: ledger.prune attempt");
        assert!(line["fields"]["summary"]
            .as_str()
            .unwrap()
            .contains("by uid "));
        assert!(line["fields"]["record"]
            .as_str()
            .unwrap()
            .contains("\"audit_version\":1"));
    }

    /// Nothing in the audit path samples or rate-limits: a thousand identical
    /// records are a thousand lines.
    #[test]
    fn identical_records_are_never_sampled_or_rate_limited() {
        let (captured, _guard) = production_logs("warn");
        let sink = AuditSink::new();
        let event = prune_event()
            .terminal(Outcome::Refused)
            .reason("store down");
        for _ in 0..1000 {
            sink.record(&event).unwrap();
        }
        assert_eq!(captured.text().matches("vtop::audit").count(), 1000);
    }

    #[test]
    fn a_record_is_one_versioned_json_line_in_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = AuditSink::new();
        sink.open_file(&path).unwrap();
        let attempt = prune_event().request_id("r-1").engine("edge-1");
        let first = sink.record(&attempt).unwrap();
        let done = attempt
            .terminal(Outcome::Success)
            .after("deleted", 12)
            .after("digest", "/var/lib/vtop/digests/prune-1.jsonl");
        sink.record(&done).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2, "{text}");
        let ReadRecord::V1(read) = read_record(lines[1]) else {
            panic!("a v1 record must read back: {}", lines[1]);
        };
        assert_eq!(read.audit_version, 1);
        assert_eq!(read.action, Action::LedgerPrune);
        assert_eq!(read.outcome, Outcome::Success);
        assert_eq!(
            read.correlation.attempt_id.as_deref(),
            Some(first.audit_id.as_str())
        );
        assert_eq!(read.correlation.request_id.as_deref(), Some("r-1"));
        assert_eq!(read.params["older_than_days"], "30");
        assert_eq!(read.after["deleted"], 12);
        assert_eq!(read.actor.kind, ActorKind::Os);
        assert_eq!(read.actor.source, "os");
        assert!(read.actor.uid.is_some() && read.actor.host.is_some());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the audit file is the owner's only");
        }
    }

    #[test]
    fn an_unwritable_audit_file_is_refused_naming_the_path() {
        let err = init_file_sink_from(Some("/nonexistent-dir-694/audit.jsonl".into()))
            .expect_err("a path in a missing directory cannot be opened");
        let message = err.to_string();
        assert!(
            message.contains("VTOP_AUDIT_LOG=/nonexistent-dir-694/audit.jsonl")
                && message.contains("Nothing was run")
                && message.contains("unset VTOP_AUDIT_LOG"),
            "{message}"
        );
        let empty = init_file_sink_from(Some("".into()))
            .unwrap_err()
            .to_string();
        assert!(empty.contains("set but empty"), "{empty}");
        assert_eq!(init_file_sink_from(None).unwrap(), None);
    }

    /// A write that fails after startup is returned and counted on
    /// `sink="file"`, and the record still reaches the log.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_file_write_is_returned_and_counted() {
        let (captured, _guard) = production_logs("error");
        let sink = AuditSink::new();
        let registry = prometheus::Registry::new();
        let events =
            IntCounterVec::new(prometheus::Opts::new("e", "e"), &["action", "outcome"]).unwrap();
        let failures = IntCounterVec::new(prometheus::Opts::new("f", "f"), &["sink"]).unwrap();
        registry.register(Box::new(events.clone())).unwrap();
        sink.set_counters(events.clone(), failures.clone());
        // /dev/full accepts the open and fails every write with ENOSPC.
        sink.open_file(Path::new("/dev/full")).unwrap();
        let err = sink.record(&prune_event()).unwrap_err();
        assert!(matches!(err, AuditError::Write { .. }), "{err:?}");
        assert!(err.to_string().contains("it is in the log stream"), "{err}");
        assert_eq!(failures.with_label_values(&["file"]).get(), 1);
        assert_eq!(
            events.with_label_values(&["ledger.prune", "attempt"]).get(),
            1
        );
        let text = captured.text();
        assert!(
            text.contains("could not be appended to the audit file"),
            "{text}"
        );
        // Marked, so a query for records (they carry `record`) can tell the
        // sink's own error line apart (review of #705).
        let error_line = text
            .lines()
            .find(|l| l.contains("could not be appended"))
            .unwrap();
        assert!(
            error_line.contains("\"audit_sink_error\":true"),
            "{error_line}"
        );
    }

    /// The plan's redaction rule: a value that looks like a secret never
    /// reaches a record, whatever parameter carries it.
    #[test]
    fn secret_shaped_values_are_redacted_in_every_allowed_parameter() {
        let secrets = [
            (
                "postgres://vtop:hunter2@db.example:5432/vtop?sslmode=verify-full",
                "hunter2",
            ),
            ("host=db user=vtop password=hunter2 dbname=vtop", "hunter2"),
            (
                "https://bucket.s3.example/obj?X-Amz-Signature=deadbeef0123",
                "X-Amz-Signature",
            ),
            (
                "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                "9f86d081884c7d659a2feaa0c55ad015",
            ),
            ("AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"),
            ("token=abc123secret", "abc123secret"),
        ];
        for action in Action::ALL {
            for name in action.params() {
                for (value, leaked) in secrets {
                    let event = AuditEvent::new(
                        action,
                        Stage::Local,
                        Actor::local(),
                        Target::new("ledger", Some(value.to_owned())),
                        Outcome::Failure,
                    )
                    .param(name, value)
                    .reason(format!("failed: {value}"))
                    .after("detail", value)
                    .engine(value);
                    let json = event.to_json();
                    assert!(
                        !json.contains(leaked),
                        "{action} param {name} leaked {leaked:?}: {json}"
                    );
                }
            }
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "not on ledger.migrate's allowlist")]
    fn a_parameter_off_the_allowlist_is_refused() {
        let _ = AuditEvent::new(
            Action::LedgerMigrate,
            Stage::Local,
            Actor::local(),
            Target::new("ledger", None),
            Outcome::Attempt,
        )
        .param("connection_string", "postgres://u:p@h/db");
    }

    #[test]
    fn a_newer_or_unknown_record_is_kept_raw_with_a_reason() {
        match read_record(r#"{"audit_version":2,"action":"ledger.prune","new":true}"#) {
            ReadRecord::Unknown { raw, why } => {
                assert_eq!(raw["new"], true);
                assert!(why.contains("audit_version 2 is newer"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        let mut v1: serde_json::Value = serde_json::from_str(&prune_event().to_json()).unwrap();
        v1["action"] = "ledger.compact".into();
        match read_record(&v1.to_string()) {
            ReadRecord::Unknown { why, .. } => assert!(why.contains("newer release"), "{why}"),
            other => panic!("{other:?}"),
        }
        // A field a later version 1 adds is ignored.
        let mut later: serde_json::Value = serde_json::from_str(&prune_event().to_json()).unwrap();
        later["added_later"] = "x".into();
        assert!(matches!(read_record(&later.to_string()), ReadRecord::V1(_)));
        assert!(matches!(
            read_record("not json"),
            ReadRecord::Malformed { .. }
        ));
    }

    /// Correlation with #693: a record written inside an exported span
    /// carries that span's trace id, the id its JSON log lines carry.
    #[test]
    fn a_record_carries_the_trace_id_of_the_active_span() {
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();
        let captured = Captured::default();
        let logs =
            crate::logging::log_layer_with_writer("info", None, true, true, captured.clone());
        let subscriber = tracing_subscriber::registry()
            .with(crate::otel::layer(&provider))
            .with(logs);
        let _guard = tracing::subscriber::set_default(subscriber);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let sink = AuditSink::new();
        sink.open_file(&path).unwrap();
        let (trace_id, _) =
            tracing::info_span!(target: "vtop_cli", "prune_ledger").in_scope(|| {
                sink.record(&prune_event()).unwrap();
                crate::otel::current_ids().expect("inside an exported span")
            });
        let line = std::fs::read_to_string(&path).unwrap();
        let ReadRecord::V1(event) = read_record(line.trim()) else {
            panic!("{line}");
        };
        assert_eq!(
            event.correlation.trace_id.as_deref(),
            Some(trace_id.as_str())
        );
        let logged = captured.text();
        assert!(
            logged.contains(&format!("\"trace_id\":\"{trace_id}\"")),
            "the log line must carry the same trace id: {logged}"
        );
        // Outside a span there is nothing to correlate with.
        sink.record(&prune_event()).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let ReadRecord::V1(outside) = read_record(text.lines().nth(1).unwrap()) else {
            panic!("{text}");
        };
        assert_eq!(outside.correlation.trace_id, None);
    }

    #[test]
    fn the_local_actor_labels_how_each_part_was_learnt() {
        let env = |name: &str| match name {
            "USER" => Some("mallory".to_owned()),
            "SUDO_USER" => Some("alice".to_owned()),
            _ => None,
        };
        let from_env = Actor::local_from(&env, |_| None);
        assert_eq!(from_env.name.as_deref(), Some("mallory"));
        assert_eq!(from_env.name_source.as_deref(), Some("env"));
        assert_eq!(from_env.sudo_user.as_deref(), Some("alice"));
        assert_eq!(from_env.source, "os");
        assert_eq!(from_env.uid, Some(rustix::process::getuid().as_raw()));
        let from_passwd = Actor::local_from(&env, |_| Some("root".to_owned()));
        assert_eq!(from_passwd.name.as_deref(), Some("root"));
        assert_eq!(
            from_passwd.name_source.as_deref(),
            Some("passwd"),
            "$USER is a claim; the passwd entry for the kernel's uid wins"
        );
        assert!(from_passwd.describe().contains("via sudo from alice"));
        assert_eq!(
            passwd_lookup(
                "root:x:0:0::/root:/bin/sh\nops:x:1000:1000::/home/ops:/bin/sh\n",
                1000
            )
            .as_deref(),
            Some("ops")
        );
        assert_eq!(passwd_lookup("root:x:0:0::/root:/bin/sh\n", 7), None);
    }

    #[test]
    fn values_are_bounded() {
        let long = "x".repeat(MAX_VALUE_BYTES * 3);
        let event = prune_event().param("digest_dir", &long);
        assert!(event.params["digest_dir"].len() <= MAX_VALUE_BYTES + 3);
    }

    #[test]
    fn every_action_round_trips_its_name() {
        for action in Action::ALL {
            assert_eq!(Action::parse(action.as_str()), Some(action));
            let json = serde_json::to_string(&action).unwrap();
            assert_eq!(json, format!("\"{}\"", action.as_str()));
        }
    }
}

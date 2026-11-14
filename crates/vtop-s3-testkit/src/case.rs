//! Running one live case against a server's expectations (#722, decisions
//! D3 to D6), and the results line every case writes.
//!
//! A case names the capabilities it needs. The most constraining
//! expectation among them decides what runs:
//!
//! | expectation      | runs                    | expected outcome                  |
//! |------------------|-------------------------|-----------------------------------|
//! | `pass`           | the positive body       | it passes                         |
//! | `unsupported`    | the refusal body        | it passes (VTOP refused, in words)|
//! | `xfail`          | the positive body       | it fails                          |
//! | `unknown`        | the positive body       | recorded, never judged; refused on a gating server |
//! | `not_applicable` | nothing                 | recorded as such                  |
//!
//! An unexpected pass and an unexpected fail both fail the test, naming the
//! case, the capability, the expectation and the file to edit (D4). The
//! failure is raised when the test's [`LiveS3`] is dropped, after every case
//! has run and written its line (review of #764), so one unexpected case does
//! not hide the outcomes of the cases after it.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::time::Instant;

use futures::FutureExt;

use crate::capability::{Capability, Expectation, RefusalAt};
use crate::server::Server;
use crate::settings::S3Settings;

/// `VTOP_TEST_S3_RESULTS`: the file every case appends its results line to.
/// Unset: no line is written (a laptop run).
pub const RESULTS_ENV: &str = "VTOP_TEST_S3_RESULTS";
/// `VTOP_TEST_S3_SERVER_VERSION`: the server's release, for the line.
pub const SERVER_VERSION_ENV: &str = "VTOP_TEST_S3_SERVER_VERSION";
/// `VTOP_TEST_S3_IMAGE_DIGEST`: the image digest the leg ran, for the line.
pub const IMAGE_DIGEST_ENV: &str = "VTOP_TEST_S3_IMAGE_DIGEST";
/// `VTOP_TEST_COMMIT` (else `GITHUB_SHA`): the VTOP commit under test.
pub const COMMIT_ENV: &str = "VTOP_TEST_COMMIT";

/// What a case's run came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The positive body passed.
    Pass,
    /// The positive body failed (an `xfail`, or an `unknown` recorded).
    Fail,
    /// The refusal body passed: VTOP refused the missing capability.
    Refused,
    /// Nothing ran: the capability does not apply.
    NotApplicable,
    /// The test harness cannot run this case against this server (not the
    /// server's gap: the fault-injecting forwarder needs plain HTTP, say).
    /// Shown as "not tested", never as a pass.
    NotRun,
    /// An `xfail` passed.
    UnexpectedPass,
    /// A `pass` failed, a refusal body failed, a case had no refusal for an
    /// `unsupported` capability, or a gating server left one `unknown`.
    UnexpectedFail,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "fail",
            Outcome::Refused => "refused",
            Outcome::NotApplicable => "not_applicable",
            Outcome::NotRun => "not_run",
            Outcome::UnexpectedPass => "unexpected_pass",
            Outcome::UnexpectedFail => "unexpected_fail",
        }
    }

    /// Whether the outcome is what the server's file says.
    pub fn expected(self) -> bool {
        !matches!(self, Outcome::UnexpectedPass | Outcome::UnexpectedFail)
    }
}

/// What a case will do, decided from the file before anything runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Plan {
    /// The expectation that decides the case, and the capability it is for.
    pub(crate) deciding: Option<(Capability, Expectation)>,
    pub(crate) step: Step,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Positive,
    /// The capability the server lacks, and where VTOP refuses it.
    Refusal(Capability, RefusalAt),
    Skip,
    /// The case cannot run as the file stands: why.
    Refuse(String),
}

impl Plan {
    /// The combined expectation, for the results line.
    pub(crate) fn expectation(&self) -> Expectation {
        self.deciding.map_or(Expectation::Pass, |(_, e)| e)
    }
}

/// Decide a case from the server's file. Pure, so the rules are unit-tested.
pub(crate) fn plan(server: &Server, needs: &[Capability], has_refusal: bool) -> Plan {
    // On a gating server any unknown capability a case needs is refused,
    // whatever else decides the case (review of #764).
    if server.gating {
        if let Some(&c) = needs
            .iter()
            .find(|&&c| server.expectation(c) == Expectation::Unknown)
        {
            return Plan {
                deciding: Some((c, Expectation::Unknown)),
                step: Step::Refuse(format!(
                    "{c} is unknown on {}, whose leg gates; settle it (pass, unsupported, \
                     xfail or not_applicable) before the leg is required",
                    server.id
                )),
            };
        }
    }
    let deciding = needs
        .iter()
        .map(|&c| (c, server.expectation(c)))
        .filter(|(_, e)| *e != Expectation::Pass)
        // The most constraining; on a tie, the first named.
        .fold(None::<(Capability, Expectation)>, |best, next| match best {
            Some(b) if b.1.rank() >= next.1.rank() => Some(b),
            _ => Some(next),
        });
    let step = match deciding {
        None | Some((_, Expectation::Xfail { .. })) => Step::Positive,
        Some((_, Expectation::NotApplicable)) => Step::Skip,
        Some((c, Expectation::Unsupported { refusal })) => {
            if has_refusal {
                Step::Refusal(c, refusal)
            } else {
                Step::Refuse(format!(
                    "{c} is unsupported on {}, and this case has no refusal path to assert; \
                     a capability VTOP does not refuse is not_applicable",
                    server.id
                ))
            }
        }
        // A gating server's unknown was refused above.
        Some((_, Expectation::Unknown)) => Step::Positive,
        Some((_, Expectation::Pass)) => unreachable!("filtered out above"),
    };
    Plan { deciding, step }
}

/// Judge what ran against the plan.
pub(crate) fn judge(plan: &Plan, body_passed: Option<bool>) -> Outcome {
    match (&plan.step, plan.expectation(), body_passed) {
        (Step::Skip, _, _) => Outcome::NotApplicable,
        (Step::Refuse(_), _, _) => Outcome::UnexpectedFail,
        (Step::Refusal(..), _, Some(true)) => Outcome::Refused,
        (Step::Refusal(..), _, _) => Outcome::UnexpectedFail,
        (Step::Positive, Expectation::Xfail { .. }, Some(true)) => Outcome::UnexpectedPass,
        (Step::Positive, Expectation::Xfail { .. }, _) => Outcome::Fail,
        (Step::Positive, Expectation::Unknown, Some(true)) => Outcome::Pass,
        (Step::Positive, Expectation::Unknown, _) => Outcome::Fail,
        (Step::Positive, _, Some(true)) => Outcome::Pass,
        (Step::Positive, _, _) => Outcome::UnexpectedFail,
    }
}

/// Where a case's results line goes, and what it carries besides the case.
#[derive(Debug, Clone, Default)]
pub struct Recorder {
    pub path: Option<PathBuf>,
    pub server_version: Option<String>,
    pub image_digest: Option<String>,
    pub vtop_commit: Option<String>,
}

impl Recorder {
    pub fn from_env() -> Recorder {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Recorder {
            path: var(RESULTS_ENV).map(PathBuf::from),
            server_version: var(SERVER_VERSION_ENV),
            image_digest: var(IMAGE_DIGEST_ENV),
            vtop_commit: var(COMMIT_ENV).or_else(|| var("GITHUB_SHA")),
        }
    }
}

/// One results line (D6).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ResultLine {
    pub server: String,
    pub server_version: Option<String>,
    pub image_digest: Option<String>,
    pub vtop_commit: Option<String>,
    pub case: String,
    pub capabilities: Vec<String>,
    /// The capability whose expectation decided the case, if any was not
    /// `pass`.
    pub deciding_capability: Option<String>,
    pub expectation: String,
    pub outcome: String,
    pub expected: bool,
    /// The verification method the case saw (`store_checksum` or
    /// `read_back`), when it recorded one.
    pub method: Option<String>,
    pub seconds: f64,
    /// For an unexpected outcome or a failed body: the first line of why.
    pub detail: Option<String>,
}

thread_local! {
    /// The method the running case recorded. Per thread (review of #764): a
    /// case's body is polled on the thread that runs the case (the tests'
    /// runtime is current-thread), so parallel tests cannot take each
    /// other's.
    static METHOD: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Record the verification method the running case observed, for its
/// results line. Call it from the case's body, on the case's own task.
pub fn record_method(method: &str) {
    METHOD.with(|m| *m.borrow_mut() = Some(method.to_owned()));
}

fn take_method() -> Option<String> {
    METHOD.with(|m| m.borrow_mut().take())
}

fn panic_text(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with a non-text payload".to_owned())
}

/// A server, the settings to reach it, and where results go: what every live
/// test starts from. Unexpected outcomes are collected and fail the test when
/// it is dropped, after every case has run.
#[derive(Debug)]
pub struct LiveS3 {
    pub server: Server,
    pub settings: S3Settings,
    pub recorder: Recorder,
    unexpected: RefCell<Vec<String>>,
}

impl LiveS3 {
    /// The server `VTOP_TEST_S3_SERVER` names, its settings and the results
    /// file, all from the environment.
    pub fn from_env() -> LiveS3 {
        let server = Server::from_env();
        let settings = S3Settings::from_env(&server);
        LiveS3::new(server, settings, Recorder::from_env()).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Refuses settings that would let a run test nothing (review of #764):
    /// an addressing style the server's file marks not_applicable or
    /// unsupported decides every case Skip, and the run would pass having
    /// run nothing.
    pub fn new(server: Server, settings: S3Settings, recorder: Recorder) -> Result<LiveS3, String> {
        let addressing = addressing(&settings);
        if let state @ (Expectation::NotApplicable | Expectation::Unsupported { .. }) =
            server.expectation(addressing)
        {
            return Err(format!(
                "{} marks {addressing} {state}, so every case would be skipped and the run \
                 would test nothing; set {} to the addressing the server supports",
                server.file_hint(),
                crate::settings::PATH_STYLE_ENV
            ));
        }
        Ok(LiveS3 {
            server,
            settings,
            recorder,
            unexpected: RefCell::new(Vec::new()),
        })
    }

    /// The addressing capability every case needs, from the settings.
    fn addressing(&self) -> Capability {
        addressing(&self.settings)
    }

    /// The unexpected outcomes so far, each as its failure line.
    pub fn unexpected(&self) -> Vec<String> {
        self.unexpected.borrow().clone()
    }

    /// Record a case the test harness cannot run against this server, and
    /// why (#722, the AWS leg: the multipart tests' forwarder speaks plain
    /// HTTP). Its line says `not_run` with the reason, and the report shows
    /// the capability as not tested; it never counts as a pass.
    pub fn not_run(&self, name: &str, needs: &[Capability], why: &str) {
        let mut needs = needs.to_vec();
        if !needs.contains(&self.addressing()) {
            needs.push(self.addressing());
        }
        let line = ResultLine {
            server: self.server.id.clone(),
            server_version: self.recorder.server_version.clone(),
            image_digest: self.recorder.image_digest.clone(),
            vtop_commit: self.recorder.vtop_commit.clone(),
            case: name.to_owned(),
            capabilities: needs.iter().map(|c| c.to_string()).collect(),
            deciding_capability: None,
            expectation: plan(&self.server, &needs, false).expectation().to_string(),
            outcome: Outcome::NotRun.as_str().to_owned(),
            expected: true,
            method: None,
            seconds: 0.0,
            detail: Some(why.to_owned()),
        };
        if let Some(path) = &self.recorder.path {
            write_line(path, &line);
        }
        eprintln!("s3-compat {} {name}: not_run ({why})", self.server.id);
    }

    /// Run a case with no refusal path: a server that lacks one of `needs`
    /// must mark it `not_applicable`, or the case fails saying so.
    pub async fn case<P, PF>(&self, name: &str, needs: &[Capability], positive: P)
    where
        P: FnOnce() -> PF,
        PF: Future<Output = ()>,
    {
        self.run(
            name,
            needs,
            positive,
            None::<fn(Capability, RefusalAt) -> PF>,
        )
        .await
    }

    /// Run a case whose refusal body asserts VTOP's refusal of a missing
    /// capability (D5): what is wrong, what the engine did, what to do. The
    /// body is told which capability the server lacks and where VTOP
    /// refuses it.
    pub async fn case_with_refusal<P, PF, R, RF>(
        &self,
        name: &str,
        needs: &[Capability],
        positive: P,
        refusal: R,
    ) where
        P: FnOnce() -> PF,
        PF: Future<Output = ()>,
        R: FnOnce(Capability, RefusalAt) -> RF,
        RF: Future<Output = ()>,
    {
        self.run(name, needs, positive, Some(refusal)).await
    }

    async fn run<P, PF, R, RF>(
        &self,
        name: &str,
        needs: &[Capability],
        positive: P,
        refusal: Option<R>,
    ) where
        P: FnOnce() -> PF,
        PF: Future<Output = ()>,
        R: FnOnce(Capability, RefusalAt) -> RF,
        RF: Future<Output = ()>,
    {
        let mut needs = needs.to_vec();
        if !needs.contains(&self.addressing()) {
            needs.push(self.addressing());
        }
        if let Err(failure) = run_case(
            &self.server,
            &self.recorder,
            name,
            &needs,
            positive,
            refusal,
        )
        .await
        {
            eprintln!("{failure}");
            self.unexpected.borrow_mut().push(failure);
        }
    }
}

fn addressing(settings: &S3Settings) -> Capability {
    if settings.force_path_style {
        Capability::PathStyle
    } else {
        Capability::VirtualHost
    }
}

impl Drop for LiveS3 {
    /// Fail the test with every unexpected outcome, once all its cases have
    /// written their lines. Not while already panicking: that failure is the
    /// test's own, and a second panic would abort.
    fn drop(&mut self) {
        let unexpected = self.unexpected.take();
        if !unexpected.is_empty() && !std::thread::panicking() {
            panic!(
                "{} unexpected outcome(s):\n{}",
                unexpected.len(),
                unexpected.join("\n")
            );
        }
    }
}

/// The runner, over an explicit server and recorder (what the unit tests
/// drive). Writes the results line, then returns an unexpected outcome as
/// its failure line.
pub(crate) async fn run_case<P, PF, R, RF>(
    server: &Server,
    recorder: &Recorder,
    name: &str,
    needs: &[Capability],
    positive: P,
    refusal: Option<R>,
) -> Result<(), String>
where
    P: FnOnce() -> PF,
    PF: Future<Output = ()>,
    R: FnOnce(Capability, RefusalAt) -> RF,
    RF: Future<Output = ()>,
{
    let plan = plan(server, needs, refusal.is_some());
    take_method();
    let started = Instant::now();
    let ran = match (&plan.step, refusal) {
        (Step::Positive, _) => Some(AssertUnwindSafe(positive()).catch_unwind().await),
        (Step::Refusal(c, at), Some(refusal)) => {
            Some(AssertUnwindSafe(refusal(*c, *at)).catch_unwind().await)
        }
        _ => None,
    };
    let seconds = started.elapsed().as_secs_f64();
    let body_failure = match &ran {
        Some(Err(payload)) => Some(panic_text(payload.as_ref())),
        _ => None,
    };
    let outcome = judge(&plan, ran.as_ref().map(Result::is_ok));
    let expectation = plan.expectation();
    let capability = plan
        .deciding
        .map_or_else(|| "every capability".to_owned(), |(c, _)| c.to_string());
    let detail = match (&plan.step, outcome, &body_failure) {
        (Step::Refuse(why), _, _) => Some(why.clone()),
        (_, Outcome::UnexpectedPass, _) => Some("the body passed".to_owned()),
        (_, _, Some(why)) => why.lines().next().map(str::to_owned),
        _ => None,
    };
    let line = ResultLine {
        server: server.id.clone(),
        server_version: recorder.server_version.clone(),
        image_digest: recorder.image_digest.clone(),
        vtop_commit: recorder.vtop_commit.clone(),
        case: name.to_owned(),
        capabilities: needs.iter().map(|c| c.to_string()).collect(),
        deciding_capability: plan.deciding.map(|(c, _)| c.to_string()),
        expectation: expectation.to_string(),
        outcome: outcome.as_str().to_owned(),
        expected: outcome.expected(),
        method: take_method(),
        seconds: (seconds * 1000.0).round() / 1000.0,
        detail,
    };
    if let Some(path) = &recorder.path {
        write_line(path, &line);
    }
    eprintln!(
        "s3-compat {} {name}: {} (expected {expectation} for {capability})",
        server.id,
        outcome.as_str()
    );
    if !outcome.expected() {
        let label = match outcome {
            Outcome::UnexpectedPass => "UNEXPECTED PASS",
            _ => "UNEXPECTED FAIL",
        };
        let why = match (&plan.step, &body_failure) {
            (Step::Refuse(why), _) => why.clone(),
            (_, Some(why)) => why.clone(),
            _ => "the body passed".to_owned(),
        };
        return Err(format!(
            "{label} {name} ({capability}, expected {expectation}) on {}: edit {} or fix the \
             cause: {why}",
            server.id,
            server.file_hint()
        ));
    }
    Ok(())
}

fn write_line(path: &std::path::Path, line: &ResultLine) {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
    }
    let mut text = serde_json::to_string(line).expect("a results line serializes");
    text.push('\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| f.write_all(text.as_bytes()))
        .unwrap_or_else(|e| panic!("cannot append to {}: {e}", path.display()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::file_with;
    use std::path::Path;

    fn server(gating: bool, overrides: &[(&str, &str)]) -> Server {
        Server::parse(&file_with(gating, overrides), Path::new("test.yaml")).unwrap()
    }

    fn lines(path: &Path) -> Vec<ResultLine> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Run one case and return its failure line, if it was unexpected.
    async fn run(
        server: &Server,
        recorder: &Recorder,
        needs: &[Capability],
        positive_passes: bool,
        refusal: Option<bool>,
    ) -> Option<String> {
        let positive = move || async move {
            record_method("store_checksum");
            assert!(positive_passes, "the positive body failed");
        };
        match refusal {
            Some(passes) => run_case(
                server,
                recorder,
                "kit.case",
                needs,
                positive,
                Some(move |_c: Capability, _at: RefusalAt| async move {
                    assert!(passes, "the refusal body failed");
                }),
            )
            .await
            .err(),
            None => run_case(
                server,
                recorder,
                "kit.case",
                needs,
                positive,
                None::<fn(Capability, RefusalAt) -> std::future::Ready<()>>,
            )
            .await
            .err(),
        }
    }

    fn recorder(dir: &Path) -> Recorder {
        Recorder {
            path: Some(dir.join("results/test.jsonl")),
            server_version: Some("1.0".into()),
            image_digest: Some("sha256:abc".into()),
            vtop_commit: Some("deadbeef".into()),
        }
    }

    #[test]
    fn the_most_constraining_expectation_decides() {
        let s = server(
            false,
            &[
                ("versioning", "{ state: unsupported, refusal: startup }"),
                ("conditional_put", "{ state: xfail, issue: 1 }"),
                ("checksum_sha256_enforced", "unknown"),
                ("virtual_host", "not_applicable"),
            ],
        );
        use Capability::*;
        assert_eq!(plan(&s, &[PutGetHead], false).step, Step::Positive);
        assert_eq!(
            plan(&s, &[ConditionalPut, Versioning], true).step,
            Step::Refusal(Versioning, RefusalAt::Startup)
        );
        assert_eq!(
            plan(&s, &[ChecksumSha256Enforced, ConditionalPut], false).expectation(),
            Expectation::Xfail { issue: 1 }
        );
        assert_eq!(plan(&s, &[Versioning, VirtualHost], true).step, Step::Skip);
        assert!(matches!(
            plan(&s, &[Versioning], false).step,
            Step::Refuse(why) if why.contains("no refusal path")
        ));
    }

    #[tokio::test]
    async fn a_pass_that_passes_is_recorded_with_its_method() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let s = server(true, &[]);
        assert_eq!(
            run(&s, &r, &[Capability::PutGetHead], true, None).await,
            None
        );
        let got = lines(r.path.as_ref().unwrap());
        assert_eq!(got.len(), 1);
        let l = &got[0];
        assert_eq!(
            (
                l.server.as_str(),
                l.case.as_str(),
                l.outcome.as_str(),
                l.expected
            ),
            ("test", "kit.case", "pass", true)
        );
        assert_eq!(l.expectation, "pass");
        assert_eq!(l.method.as_deref(), Some("store_checksum"));
        assert_eq!(l.capabilities, vec!["put_get_head"]);
        assert_eq!(
            (
                l.server_version.as_deref(),
                l.image_digest.as_deref(),
                l.vtop_commit.as_deref()
            ),
            (Some("1.0"), Some("sha256:abc"), Some("deadbeef"))
        );
    }

    #[tokio::test]
    async fn a_pass_that_fails_is_an_unexpected_fail_naming_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let s = server(true, &[]);
        let why = run(&s, &r, &[Capability::Multipart], false, None)
            .await
            .expect("an unexpected fail fails the test");
        for part in [
            "UNEXPECTED FAIL kit.case",
            "expected pass",
            "edit tests/s3-compat/servers/test.yaml",
            "the positive body failed",
        ] {
            assert!(why.contains(part), "{why:?} should say {part:?}");
        }
        let l = &lines(r.path.as_ref().unwrap())[0];
        assert_eq!((l.outcome.as_str(), l.expected), ("unexpected_fail", false));
        assert_eq!(l.detail.as_deref(), Some("the positive body failed"));
    }

    #[tokio::test]
    async fn an_unsupported_capability_runs_the_refusal_and_its_failure_is_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let s = server(
            false,
            &[("object_lock", "{ state: unsupported, refusal: startup }")],
        );
        // The positive body would fail; it must not run.
        assert_eq!(
            run(&s, &r, &[Capability::ObjectLock], false, Some(true)).await,
            None
        );
        let why = run(&s, &r, &[Capability::ObjectLock], true, Some(false))
            .await
            .expect("a refusal that does not hold is a failure");
        assert!(
            why.contains("UNEXPECTED FAIL kit.case (object_lock, expected unsupported"),
            "{why}"
        );
        let why = run(&s, &r, &[Capability::ObjectLock], true, None)
            .await
            .expect("an unsupported capability with no refusal path is a failure");
        assert!(why.contains("no refusal path"), "{why}");
        let got: Vec<_> = lines(r.path.as_ref().unwrap())
            .into_iter()
            .map(|l| l.outcome)
            .collect();
        assert_eq!(got, vec!["refused", "unexpected_fail", "unexpected_fail"]);
    }

    #[tokio::test]
    async fn an_xfail_that_passes_is_an_unexpected_pass() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let s = server(false, &[("conditional_put", "{ state: xfail, issue: 9 }")]);
        assert_eq!(
            run(&s, &r, &[Capability::ConditionalPut], false, None).await,
            None,
            "an xfail that fails is expected"
        );
        let why = run(&s, &r, &[Capability::ConditionalPut], true, None)
            .await
            .expect("an xfail that passes fails the test");
        assert!(
            why.contains("UNEXPECTED PASS kit.case (conditional_put, expected xfail (#9))"),
            "{why}"
        );
        let got: Vec<_> = lines(r.path.as_ref().unwrap())
            .into_iter()
            .map(|l| (l.outcome, l.expected))
            .collect();
        assert_eq!(
            got,
            vec![("fail".into(), true), ("unexpected_pass".into(), false)]
        );
    }

    #[tokio::test]
    async fn unknown_is_recorded_on_a_new_server_and_refused_on_a_gating_one() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let new = server(false, &[("checksum_sha256_enforced", "unknown")]);
        assert_eq!(
            run(&new, &r, &[Capability::ChecksumSha256Enforced], false, None).await,
            None
        );
        assert_eq!(
            run(&new, &r, &[Capability::ChecksumSha256Enforced], true, None).await,
            None
        );
        let gating = server(true, &[("checksum_sha256_enforced", "unknown")]);
        let why = run(
            &gating,
            &r,
            &[Capability::ChecksumSha256Enforced],
            true,
            None,
        )
        .await
        .expect("unknown on a gating server is refused");
        assert!(why.contains("whose leg gates"), "{why}");
        let got: Vec<_> = lines(r.path.as_ref().unwrap())
            .into_iter()
            .map(|l| l.outcome)
            .collect();
        assert_eq!(got, vec!["fail", "pass", "unexpected_fail"]);
    }

    #[tokio::test]
    async fn not_applicable_runs_nothing_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let s = server(false, &[("virtual_host", "not_applicable")]);
        assert_eq!(
            run(&s, &r, &[Capability::VirtualHost], false, Some(false)).await,
            None
        );
        let l = &lines(r.path.as_ref().unwrap())[0];
        assert_eq!((l.outcome.as_str(), l.expected), ("not_applicable", true));
        assert_eq!(l.method, None, "nothing ran, so no method");
    }

    #[tokio::test]
    async fn without_a_results_file_cases_are_still_judged() {
        let s = server(true, &[]);
        let r = Recorder::default();
        assert_eq!(
            run(&s, &r, &[Capability::PutGetHead], true, None).await,
            None
        );
        assert!(run(&s, &r, &[Capability::PutGetHead], false, None)
            .await
            .is_some());
    }

    /// Review of #764: on a gating server an unknown capability is refused
    /// even when another capability of the case would decide it.
    #[test]
    fn an_unknown_on_a_gating_server_is_refused_whatever_else_the_case_needs() {
        let s = server(
            true,
            &[
                ("checksum_sha256_enforced", "unknown"),
                ("conditional_put", "{ state: xfail, issue: 1 }"),
                ("virtual_host", "not_applicable"),
            ],
        );
        use Capability::*;
        for needs in [
            &[ConditionalPut, ChecksumSha256Enforced][..],
            &[VirtualHost, ChecksumSha256Enforced][..],
        ] {
            let p = plan(&s, needs, true);
            assert!(
                matches!(&p.step, Step::Refuse(why) if why.contains("checksum_sha256_enforced is unknown")),
                "{needs:?}: {p:?}"
            );
        }
    }

    /// The refusal body is told which capability decided the case.
    #[tokio::test]
    async fn the_refusal_body_is_told_which_capability_the_server_lacks() {
        let s = server(
            false,
            &[
                ("versioning", "{ state: unsupported, refusal: startup }"),
                (
                    "object_lock",
                    "{ state: unsupported, refusal: first_batch }",
                ),
            ],
        );
        let seen = std::cell::Cell::new(None);
        let outcome = run_case(
            &s,
            &Recorder::default(),
            "kit.lock",
            &[Capability::ObjectLock, Capability::Versioning],
            || async { panic!("the positive body must not run") },
            Some(|c: Capability, at: RefusalAt| {
                seen.set(Some((c, at)));
                async {}
            }),
        )
        .await;
        assert_eq!(outcome, Ok(()));
        assert_eq!(
            seen.get(),
            Some((Capability::ObjectLock, RefusalAt::FirstBatch))
        );
    }

    fn settings(path_style: bool) -> S3Settings {
        S3Settings {
            endpoint: Some("http://127.0.0.1:1".into()),
            region: "us-east-1".into(),
            force_path_style: path_style,
            verify_tls: false,
            bucket: "b".into(),
            locked_bucket: "l".into(),
        }
    }

    /// Review of #764: an addressing style the server does not support would
    /// skip every case, and a run that tested nothing would pass.
    #[test]
    fn an_addressing_style_the_server_lacks_is_refused_before_any_case() {
        let s = server(true, &[("virtual_host", "not_applicable")]);
        let err = LiveS3::new(s.clone(), settings(false), Recorder::default()).unwrap_err();
        assert!(
            err.contains("marks virtual_host not_applicable")
                && err.contains("VTOP_TEST_S3_PATH_STYLE"),
            "{err}"
        );
        assert!(LiveS3::new(s, settings(true), Recorder::default()).is_ok());
    }

    /// Review of #764: an unexpected case does not stop the cases after it;
    /// every one writes its line, and the test fails once, at the end,
    /// listing them all.
    #[tokio::test]
    async fn every_case_runs_and_the_test_fails_at_the_end_listing_each_unexpected_one() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let path = r.path.clone().unwrap();
        let live = LiveS3::new(server(true, &[]), settings(true), r).unwrap();
        let ran = std::cell::Cell::new(0);
        for (name, passes) in [("a", false), ("b", true), ("c", false)] {
            live.case(name, &[Capability::PutGetHead], || async {
                ran.set(ran.get() + 1);
                assert!(passes, "case {name} failed");
            })
            .await;
        }
        assert_eq!(ran.get(), 3, "every case ran");
        assert_eq!(live.unexpected().len(), 2);
        let outcomes: Vec<_> = lines(&path).into_iter().map(|l| l.outcome).collect();
        assert_eq!(outcomes, vec!["unexpected_fail", "pass", "unexpected_fail"]);
        let panic = AssertUnwindSafe(async move { drop(live) })
            .catch_unwind()
            .await
            .expect_err("dropping it fails the test");
        let text = panic_text(panic.as_ref());
        assert!(
            text.starts_with("2 unexpected outcome(s)")
                && text.contains("UNEXPECTED FAIL a ")
                && text.contains("UNEXPECTED FAIL c "),
            "{text}"
        );
    }

    /// A case the harness cannot run is recorded as such, with why.
    #[test]
    fn a_case_the_harness_cannot_run_is_recorded_not_run_with_why() {
        let dir = tempfile::tempdir().unwrap();
        let r = recorder(dir.path());
        let path = r.path.clone().unwrap();
        let live = LiveS3::new(server(false, &[]), settings(true), r).unwrap();
        live.not_run(
            "multipart.forwarded",
            &[Capability::Multipart],
            "the forwarder needs a plain-HTTP endpoint",
        );
        let l = &lines(&path)[0];
        assert_eq!((l.outcome.as_str(), l.expected), ("not_run", true));
        assert_eq!(
            l.detail.as_deref(),
            Some("the forwarder needs a plain-HTTP endpoint")
        );
        assert_eq!(l.capabilities, vec!["multipart", "path_style"]);
        assert!(live.unexpected().is_empty());
    }
}

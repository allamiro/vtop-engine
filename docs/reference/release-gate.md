# 1.0 release gate

Where each criterion of the production-readiness bar (#714) stands, with the
date it was last checked, the evidence for it, and the issue that closes the
gap. It is the table #741 asks for: release candidates are judged against it,
and the 1.0.0 release notes link it.

The criteria, their pass/fail tests and the ladder from 0.10 to 1.0 are
defined in #714 and are not repeated here. This page only says where each one
stands. The [component status](component-status.md) page says which
component is production, preview or experimental; the
[data-safety register](data-safety-register.md) holds the bugs that A1 counts.

## The rule

- **1.0.0-rc.1** is cut only when every criterion except A1 is **met**. The
  rc series is the A1 quiet period: a data-safety bug opened during it
  restarts the window and produces the next rc.
- **1.0.0** is cut only when A1 is met as well, with the campaigns of A2 to
  A5 running inside the window.
- A row changes only in a commit that cites the evidence for the change.
  Re-evaluate every row, and update the *Checked* dates and the verdict, at
  each minor release and each release candidate.

## Verdict

<!-- verdict:begin -->

| Item | Value |
|---|---|
| Evaluated | 2026-11-22 |
| Met | 12 |
| Partial | 12 |
| Not met | 6 |
| 1.0.0-rc.1 may be cut | no |
| 1.0.0 may be cut | no |

<!-- verdict:end -->

## The table

*Status* is **met**, **partial** or **not met**, the words #714 uses.
*Evidence* names what shows it: a file or a test (`path` or `path::name`), a
CI job (`.github/workflows/FILE.yml::job`), a merged pull request (`#N`) or a
CI run (its link). *Open* names the issues that close the gap; it is "none"
only for a met row.

<!-- criteria:begin -->

| ID | Criterion | Status | Evidence | Open | Checked |
|---|---|---|---|---|---|
| A1 | Data-safety quiet period | not met | `docs/reference/data-safety-register.md`; `.github/workflows/data-safety-register.yml::check`; #759. The newest bug in the register was opened on 2026-11-15, and the window opens at rc.1, so it is held by this gate. | #741 | 2026-11-22 |
| A2 | Fault and crash testing | met | (a) `.github/workflows/engine-crash-sweep.yml::sweep` kills `vtopctl run` at every failpoint on SQLite and PostgreSQL, Kafka, file and spool sources, a store outage and ENOSPC (`scripts/live-chaos/engine/run-sweep.sh`; #836, #871); [nightly of 2026-11-21](https://github.com/allamiro/vtop-engine/actions/runs/44916357611)). (b) `crates/vtop-log/tests/sim_crash_sweep.rs`; `.github/workflows/ci.yml::live-chaos`. | none | 2026-11-22 |
| A3 | Long soak, engine 72 h and cluster 7 days | not met | The engine soak's driver and evaluator are built (`.github/workflows/soak.yml::soak-72h`; `docs/SOAK_VALIDATION.md`); no 72-hour or 7-day run has completed. | #726, #727 | 2026-11-22 |
| A4 | Jepsen-style history check of the cluster plane | partial | Recorder and data-plane checker (`scripts/live-chaos/history/check_history.py`; #878), fed by scenarios 04 and 09 in `.github/workflows/ci.yml::live-chaos`. Lease and metadata histories, the 24-hour campaign and the live bug re-introductions are open. | #728 | 2026-11-22 |
| A5 | Fuzz and mutation | partial | `.github/workflows/fuzz-nightly.yml::fuzz` over 12 decoders with a persisted corpus; `.github/workflows/ci.yml::mutants-gate` (`scripts/mutants-gate.py`). | #729 | 2026-11-22 |
| A6 | Every bug lands with a test that failed first | met | The rule in `CONTRIBUTING.md`; the release notes name the tests. | none | 2026-11-22 |
| B1 | AWS S3 and one other S3 provider | not met | MinIO only (`.github/workflows/ci.yml::minio-integration`; `crates/vtop-upload/tests/live_s3_verification.rs`). A store without Object Lock or versioning is refused at startup (#770). | #722 | 2026-11-22 |
| B2 | Real Kafka versions | met | `.github/workflows/kafka-matrix.yml::kafka` (Apache Kafka 3.7 to 4.4, TLS and SASL listeners, Redpanda; #834); `.github/workflows/kafka-cluster.yml::cluster` (three brokers, a leader stopped mid-read; #874). The [nightly of 2026-11-20](https://github.com/allamiro/vtop-engine/actions/runs/44730486247) passed; the nightly of 2026-11-21 failed one leg on a paused-partition race that #877 fixed the same day. | none | 2026-11-22 |
| B3 | State store versions | met | `.github/workflows/postgres-matrix.yml::postgres` on PostgreSQL 14 to 18 (#761; [nightly of 2026-11-21](https://github.com/allamiro/vtop-engine/actions/runs/44881713701)); `crates/vtop-state/tests/pg_battery.rs`. | none | 2026-11-22 |
| B4 | Platforms | partial | Release archives smoked on every Linux, with a glibc 2.17 and a static musl build (`.github/workflows/release.yml::binaries`; #842); the workspace's tests on macOS arm64, macOS x86_64 and aarch64 Linux nightly (`.github/workflows/platform-tests.yml::test`; #876). | #724 | 2026-11-22 |
| C1 | Persisted and wire formats inventoried and frozen | partial | `scripts/compat/formats.yaml` and `scripts/check-format-registry.py` (#783); `docs/reference/compatibility.md`; `.github/workflows/compat.yml::cell` (#846). The 1.0 freeze is not declared. | #743 | 2026-11-22 |
| C2 | Config keys | partial | `scripts/check-config-reference.py`; `scripts/config-keys-since.py` lists the keys an older engine ignores (#858). Config carries no version and has no strict mode. | #743 | 2026-11-22 |
| C3 | CLI and metric names | not met | No snapshot gate on `main`; one is in review. | #719 | 2026-11-22 |
| C4 | Upgrade and downgrade guarantees | partial | `.github/workflows/ci.yml::upgrade-compat` (`scripts/upgrade-compat.sh`); downgrade is replaced by the drilled restore of E3 (#829). N to N+2 rolling upgrade under load is not tested. | #743 | 2026-11-22 |
| C5 | Versioning, deprecation and support window | met | `docs/SUPPORT.md` (#777). | none | 2026-11-22 |
| D1 | External security review | not met | No independent review has been commissioned. | #730 | 2026-11-22 |
| D2 | Threat model coverage | met | Section 17 of `docs/SECURITY_MODEL.md`, checked by `scripts/check-threat-map.py` in `.github/workflows/ci.yml::claims` (#828). | none | 2026-11-22 |
| D3 | Supply chain | partial | Archive and image provenance verified before publishing (#825); a report-only rebuild compares the binaries (`.github/workflows/release.yml::reproduce`; #823); advisory review dates (`scripts/check-advisory-review.py`; `.github/workflows/advisory-review.yml::check`). | #731 | 2026-11-22 |
| D4 | Secret handling | met | `crates/vtop-core/src/secret_file.rs`; `crates/vtop-cli/tests/secret_file_permissions.rs`; `docs/operate/secrets.md` (#775). | none | 2026-11-22 |
| D5 | Disclosure policy | met | `SECURITY.md`, checked by `scripts/check-security-policy.py` (#760). | none | 2026-11-22 |
| E1 | Runbooks completed by someone who did not write them | partial | `docs/operate/runbooks.md`; no runbook has been run by a second person. | #735 | 2026-11-22 |
| E2 | Alerts and dashboards | met | `observability/prometheus/vtop-rules.yaml` with a unit test per alert in `.github/workflows/ci.yml::prometheus-rules`; `docs/operate/slos.md` (#772). | none | 2026-11-22 |
| E3 | Backup and restore of the state store | met | `scripts/restore-drill.sh` in `.github/workflows/ci.yml::restore-drill-sqlite` and `.github/workflows/postgres-matrix.yml::restore-drill`, with RPO and RTO in `docs/operate/backup-and-recovery.md` (#829). | none | 2026-11-22 |
| E4 | Disaster-recovery drills | partial | A lost ledger restored from a backup is drilled (E3, #829); a lost ledger with no backup, a lost bucket and a lost region are not. | #735 | 2026-11-22 |
| E5 | Capacity guidance | partial | Starting points in `docs/PRODUCTION_HA.md`; no table tied to measurements. | #736 | 2026-11-22 |
| E6 | Supported topology and engine host loss | partial | One engine per ledger, enforced at startup; the crash sweep kills the engine process (`.github/workflows/engine-crash-sweep.yml::sweep`). The host-loss drill and the deployment examples are in review. | #739 | 2026-11-22 |
| F1 | Published, reproducible benchmarks with limits stated | partial | Charts redrawn from committed data (`.github/workflows/ci.yml::benchmark-charts`; `docs/benchmarks`); one laptop, no dedicated hardware. | #737, #736 | 2026-11-22 |
| G1 | A real deployment | not met | No deployment outside the lab has reported. | #738 | 2026-11-22 |
| H1 | Docs agree with the code and with each other | met | Every status claim re-read (#776); `scripts/check-release-versions.py` in the docs workflow. | none | 2026-11-22 |
| H2 | The 1.0 scope is explicit | met | `docs/reference/component-status.md`, checked by `scripts/check-component-status.py` (#826). | none | 2026-11-22 |

<!-- criteria:end -->

## How this page is kept honest

`scripts/check-release-gate.py` runs in the docs workflow. It fails when:

- the table does not list exactly the 30 criteria of #714, in order;
- a status is not `met`, `partial` or `not met`, a `met` row names an open
  issue or no evidence, or a row that is not met names no issue;
- a file or test it names does not exist (a Rust or Python test must still be
  a function, and a Rust test must not be `#[ignore]`d), a CI job is not a job
  of that workflow, or a pull request is not merged into the branch;
- a *Checked* date is not a date, is later than the *Evaluated* date, or is
  earlier than #714 (2026-11-14), or *Evaluated* is in the future;
- the counts or either verdict line disagree with the rows.

With `--online` (as the docs workflow runs it) it also checks every CI run
link against the GitHub API (a run cited by a met row must have succeeded),
that every *Open* issue exists, and that a row
not met is not held open by issues that are all closed: that row needs
re-evaluating.

It cannot tell whether evidence that exists is enough. That is the
re-evaluation at each release, recorded by the *Checked* dates.

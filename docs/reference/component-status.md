# Component status

Which parts of VTOP are production, preview or experimental, why, and which
criteria are still open. It is the component table criterion H2 of the
production-readiness bar (#714) asks for, and the scope decision of #740.

Decision status: CONFIRMED

> [!IMPORTANT]
> **The maintainer confirmed every label on this page on 2026-11-21**, with
> the proposed answer to each of D1 to D9 below. The labels were derived from
> the code, CI and issues as they stood on 2026-11-20. A label is what the
> evidence supports, not a promise of support: VTOP is still a **prototype /
> reference implementation** until 1.0, as the
> [README](https://github.com/allamiro/vtop-engine#status-and-limits) and the
> [support policy](../SUPPORT.md) say. A label changes only in a commit that
> records the evidence for the change.

## What the labels mean

| Label | Meaning |
|---|---|
| **production** | In the 1.0 promise. Every criterion of #714 that applies to it is met, dated and linked from the release notes, so the *Open criteria* line says "none". |
| **preview** | Built, tested and meant to ship, but at least one criterion that applies to it is open, so it may still change in a minor release and is used with the source data retained and a scheduled scrub. |
| **experimental** | May change or be removed in any minor release, or is proven only in the lab or against one store. Outside every guarantee, as the [compatibility policy](compatibility.md#status-words) already says of its experimental formats. |

These labels say how much evidence a component has. They are not the status
words of the [format inventory](compatibility.md#inventory) (stable, evolving,
experimental), which say what a persisted or wire format promises; a format
can be stable while the component that writes it is still a preview.

The ladder in #714 allows no `production` label before 1.0. So the *Today*
column says what the evidence supports now (v0.11.0), and the *At 1.0* column
is the scope the maintainer is asked to commit to. A component is only
labelled `production` at 1.0 if the criteria in its *Open criteria* line are
met by then.

## The status table

<!-- status:begin -->

| Component | Today | At 1.0 | Decision |
|---|---|---|---|
| Archive engine core | preview | production | CONFIRMED |
| Kafka source | preview | production | CONFIRMED |
| File source | preview | production | CONFIRMED |
| Syslog spool source | preview | production | CONFIRMED |
| S3-compatible upload (`s3_native`) | preview | production | CONFIRMED |
| Local directory upload (`localfs`) | experimental | preview | CONFIRMED |
| Command-line upload backends (`awscli`, `s3cmd`, `minio`) | experimental | experimental | CONFIRMED |
| SQLite state store | preview | production | CONFIRMED |
| PostgreSQL state store | preview | production | CONFIRMED |
| Manifests 0.1 to 0.3 | preview | production | CONFIRMED |
| Manifest 0.4 and MAC key rotation | preview | production | CONFIRMED |
| Verification, scrub and coverage audit | preview | production | CONFIRMED |
| S3 Object Lock profile | experimental | preview | CONFIRMED |
| Audit trail | preview | production | CONFIRMED |
| Prometheus metrics, alert rules and dashboards | preview | production | CONFIRMED |
| OpenTelemetry export | experimental | preview | CONFIRMED |
| Metadata plane (`vtop-meta`) | preview | preview | CONFIRMED |
| Data node (`vtop-node`) | preview | preview | CONFIRMED |
| Replicated broker (`vtop-broker`) | preview | preview | CONFIRMED |
| Kafka gateway | experimental | preview | CONFIRMED |
| Helm chart | preview | preview | CONFIRMED |

<!-- status:end -->

Not in the table because no code exists for them: the opt-in datagram egress
transport, engine fleet mode and an engine Helm chart. See
[Not built](#not-built).

What the proposal says in one paragraph: **1.0 is the archive engine and the
formats it writes.** The cluster plane (metadata plane, data node, broker) and
the Kafka gateway ship at 1.0 labelled preview, as #714 recommends, because
making them production needs the 7-day soak (A3, #727) and the history
checker (A4, #728) for them. Today nothing is production, because the
quiet period (A1), the 72-hour soak (A3, #726), the real-S3 evidence (B1,
#722) and a pilot (G1, #738) are still open for the engine too.

## What the maintainer decides

Each was a question on #740. The maintainer confirmed the proposed answer to
all nine on 2026-11-21; the other answer is kept for the record.

| # | Decision | Confirmed answer | The other reasonable answer |
|---|---|---|---|
| D1 | What a label states | Two columns: what the evidence supports today, and the scope committed to at 1.0 | One column, the 1.0 target only, with "prototype" as the project-wide word until then |
| D2 | The 1.0 scope | The archive engine, its sources, upload, ledger, formats and operator surface. The cluster plane and the Kafka gateway are preview at 1.0 | Add the cluster plane to 1.0, which needs #727 and #728 and the external review (#730) for it |
| D3 | The Kafka gateway | `experimental` today, `preview` at 1.0. The compatibility policy already marks its advertised APIs `experimental` (inventory row 22) and #714 recommends `preview` | `preview` today, matching #714, once #751 lets a client follow a failover (it now does in live-chaos scenarios 27 and 28; the label stays the maintainer's decision) |
| D4 | The cluster plane today | `preview`. Ten of the 24 data-safety bugs found so far were in it, five of them on 2026-11-10 and 11, but it has deterministic crash sweeps and 26 live-chaos scenario scripts | `experimental` until the history checker (#728) exists |
| D5 | The syslog spool source | `preview` today and `production` at 1.0, if the crash sweep is extended to it | Treat it as preview at 1.0, since it has the thinnest evidence of the three sources |
| D6 | The PostgreSQL state store | `production` at 1.0, with a release asset or an image that enables the feature | `preview` at 1.0 if it stays a source build (`--features postgres`) |
| D7 | The local directory backend | `experimental` today: it copies and renames with no `fsync` of the file or its directory, and nothing tests power loss for it | `preview` after a fix and a power-loss test, or document it as a lab and air-gapped convenience |
| D8 | The S3 Object Lock profile | `experimental` today, `preview` at 1.0. It is proven against MinIO only, and a compliance-mode retention cannot be undone | `preview` now, with the AWS result (B1, #722) as the exit criterion |
| D9 | The datagram egress transport | Remove it from the 1.0 scope and the criteria. Nothing is built; the measured TCP path was kept (#483) | Keep it as a stated non-goal under #475 |

## Evidence per component

Every claim below was read from the repository, the CI workflows or an issue
on 2026-11-20. "Data-safety bugs" counts the rows of the
[data-safety register](data-safety-register.md) (24 in all, the most recent
opened 2026-11-15, #782). The criterion IDs (A1, B1 and so on) are those of
#714.

### Archive engine core

- **Why:** `preview` today because the commit rule is well covered but not yet shown over time or on real stores beyond MinIO, Kafka and SQLite; `production` at 1.0 because it is the product.
- **Tests and CI:** `crates/vtop-core/tests/prop_state_machine.rs`; `tests/integration_replay.rs` and `integration_state_recovery.rs`; `crates/vtop-cli/tests/ledger_power_loss.rs`; the `e2e smoke` job over the full Compose lab; the `engine crash sweep` workflow, which kills the engine with `SIGKILL` at every failpoint against real Kafka, MinIO and SQLite (the commit-boundary points on pull requests, every point nightly); the `mutation testing (commit path)` gate over `state_machine.rs` and `replay.rs`; nightly fuzzing of the decoders.
- **Open criteria:** A1 (quiet period; four data-safety bugs in the engine rows, #552, #622, #639 and #782, the last opened 2026-11-15); A3 engine soak, #726; the crash sweep does not yet cover PostgreSQL, the file source, a store outage or a full disk (#725); B4 platform matrix, #724; E6 engine host loss, #739; deterministic object naming is narrowed, not met (#773, #774).
- **Docs:** README and `limitations.md` say prototype, list the narrowed naming rule and point to the register. They claim no more than this row.

### Kafka source

- **Why:** `preview`: tested on one plaintext broker at a time across 3.7 to 4.4, not on a secured or multi-broker cluster; `production` at 1.0 for the tested configurations the page names.
- **Tests and CI:** `crates/vtop-adapters/src/kafka_source.rs`; the live-broker suite `tests/integration_kafka_to_minio.rs` including the stale-commit and stall-recovery tests; `kafka matrix` nightly on every Apache Kafka line from 3.7 to 4.4, and on pull requests that change the Kafka path; `engine crash sweep` (Kafka is its source).
- **Open criteria:** B2 (#721): SASL and TLS cases and a non-Apache broker are not tested; mutual TLS to the broker is configured and tested on one `mtls` leg (#873); multi-broker clusters are not tested; Kafka fleet mode (several engines in one group) is not built. Two data-safety bugs (#614, #623).
- **Docs:** `install/kafka.md` states exactly what is and is not tested.

### File source

- **Why:** `preview`: the source with the most data-safety bugs (six rows with the spool source), all fixed, and the one the crash sweep does not reach yet.
- **Tests and CI:** `crates/vtop-adapters/src/file_source.rs` and `continuity.rs`; `tests/integration_file_to_minio.rs`; the `file source (macOS)` job; the e2e smoke. `delete_after_commit` is off by default and keeps a file whenever it cannot prove nothing else holds it.
- **Open criteria:** A1 (#65, #558, #578, #619, #620, #621, with the source-side residuals listed in `limitations.md`); A2(a) for the file source (#725, follow-up); B4 macOS and Linux archive matrix (#724).
- **Docs:** `limitations.md` and `ROADMAP.md` list the known residuals.

### Syslog spool source

- **Why:** `preview`: shares the file source's continuity code but has the thinnest evidence of the three sources.
- **Tests and CI:** `crates/vtop-adapters/src/syslog_spool_source.rs`; the e2e smoke archives a seeded spool. No live test delivers through rsyslog or syslog-ng, and the crash sweep does not include it.
- **Open criteria:** A2(a) for the spool (#725 follow-up); a line whose time is RFC 3164 (no year or zone) gives the marker no time; shares #558 and #578 with the file source.
- **Docs:** `README.md` and `when-to-use.md` describe it as reading spool files a collector has written, and say data lost before the spool is out of reach.

### S3-compatible upload (`s3_native`)

- **Why:** `preview` because every live S3 test runs against one MinIO; `production` at 1.0 for the providers B1 names.
- **Tests and CI:** `crates/vtop-upload/src/s3_native.rs`; `crates/vtop-upload/tests` (`live_s3_verification`, `live_s3_multipart_resume`, `live_s3_object_lock`, `request_deadline`, `slow_link_deadline`, `transport_conformance`); the `minio integration` job against MinIO built from release 2025-09-07; the README's lossy-link and policed-uplink measurements.
- **Open criteria:** B1 (#722): the live tests run against RustFS, Garage, SeaweedFS, Versity and CloudServer beside MinIO with a generated compatibility table (#764, #837, #841), and an opt-in AWS S3 leg exists (#841), but no AWS S3 result is recorded yet, so AWS has no row in the table.
- **Docs:** `limitations.md` says "any S3-compatible store"; ROADMAP says Object Lock behaviour on AWS is taken from its documentation.

### Local directory upload (`localfs`)

- **Why:** `experimental`: the code copies to a staging name and renames, with no `fsync` of the file or its directory, and verification reads the page cache; a power loss after the commit is not covered by any test (`crates/vtop-upload/src/localfs_backend.rs`).
- **Tests and CI:** five unit tests; the release workflow runs a file-to-local-directory round trip and a scrub on Debian 12, Ubuntu 22.04 and AlmaLinux 8.
- **Open criteria:** a durability decision and a power-loss test for this backend; D7 above.
- **Docs:** README: "for local or air-gapped storage"; `limitations.md`: "available".

### Command-line upload backends (`awscli`, `s3cmd`, `minio`)

- **Why:** `experimental`: explicit opt-in, run an external client, download every object to re-hash it, and have unit tests against fake binaries only.
- **Tests and CI:** `crates/vtop-upload/src/command.rs` and `tests/verification_method.rs`; no live suite.
- **Open criteria:** outside the 1.0 scope unless a live suite is added.
- **Docs:** `configure/reference.md` lists them; `limitations.md` states the verification cost.

### SQLite state store

- **Why:** `preview`, `production` at 1.0: the default single-engine ledger, with the shared battery and power-loss tests; no restore drill yet.
- **Tests and CI:** `crates/vtop-state/src/test_battery.rs`; `ledger_power_loss.rs`, `ledger_v0_9_0.rs` (a ledger a released binary wrote opens under this build); the crash sweep uses it.
- **Open criteria:** E3 backup and restore drill (#734); a SQLite one-way schema step is taken only by `vtopctl migrate` (#866): an existing ledger below a step refuses to open and names the command, and the step list is empty today, so no step has been needed yet; one engine per ledger.
- **Docs:** `deploy/single-node.md`.

### PostgreSQL state store

- **Why:** `preview`, `production` at 1.0: the best-matrixed backend (five majors nightly) and the container image has it from v0.11.0 (#867), but the release binaries are built without it.
- **Tests and CI:** `crates/vtop-state/tests/pg_battery.rs`; `postgres_migrate_restore.rs`; the `postgres matrix` workflow on PostgreSQL 14 to 18 (14 and 18 on pull requests, all five nightly); an opt-in managed-service smoke job.
- **Open criteria:** the release binaries need a source build with `--features postgres` for it (the image has it from v0.11.0), so D6 is half met; managed services and `pg_upgrade` are outside the matrix; PostgreSQL 14 reached end of life on 2026-11-12 and is dropped at the next minor; E3 (#734); the crash sweep does not cover it (#725).
- **Docs:** `POSTGRES_DEPLOYMENT.md` is precise about versions; `when-to-use.md` offers PostgreSQL as an option without saying it is a source build.

### Manifests 0.1 to 0.3

- **Why:** `preview`, `production` at 1.0: read forever, written as 0.3 by default.
- **Tests and CI:** `crates/vtop-core/tests/manifest_evolution.rs`, `manifest_writer.rs` and golden fixtures; the `manifest compat` job (released readers verify an archive); `tests/integration_manifest_rollback.rs`; the `manifest_parse` fuzz target; the format registry check.
- **Open criteria:** C1 (#743): the registry exists, the tested upgrade and downgrade matrix is not complete; one data-safety bug (#630) and one in `verify-manifest` (#68).
- **Docs:** `compatibility.md` row 1 ("stable reader").

### Manifest 0.4 and MAC key rotation

- **Why:** `preview`, `production` at 1.0 once it is the default writer (planned for v0.12.0): opt-in, a one-way step for readers before v0.8.0.
- **Tests and CI:** `manifest_keyring.rs`, `manifest_write_version.rs`; the `manifest compat` job writes 0.4 and verifies it with released readers.
- **Open criteria:** a mixed fleet must run the keyring release everywhere before a key changes; public-key signatures are not built (MAC only, so an auditor needs the secret); #743.
- **Docs:** `operate/manifest-key-rotation.md`; `limitations.md`.

### Verification, scrub and coverage audit

- **Why:** `preview`, `production` at 1.0: the evidence tooling for the promise; tested against MinIO and local stores only.
- **Tests and CI:** `crates/vtop-cli/tests/integration_scrub.rs`, `verification_receipts.rs`, `live_s3_verification_receipts.rs`, `size_only_verification.rs`, `verification_class.rs`.
- **Open criteria:** B1 (#722); scrub and audit cost at scale is not measured (E5, #736; the million-file benchmark is not built); data-safety bug #64 (strong verification passing on metadata).
- **Docs:** `operate/scrub-and-audit.md`; size-only verification is documented as an opt-out that accepts same-size corruption.

### S3 Object Lock profile

- **Why:** `experimental`: proven against MinIO only, and a compliance-mode retention cannot be removed by anyone, so a wrong assumption about a provider is not recoverable.
- **Tests and CI:** `crates/vtop-upload/tests/live_s3_object_lock.rs`, `multipart_object_lock.rs`, `object_lock_wire.rs`; `crates/vtop-cli/tests/live_s3_object_lock_engine.rs`, `object_lock.rs`.
- **Open criteria:** B1 (#722) on AWS; cold-tier copies are written under the lock and their retention is checked (#868).
- **Docs:** `OBJECT_LOCK_DEPLOYMENT.md`; ROADMAP and `limitations.md` say MinIO only.

### Audit trail

- **Why:** `preview`, `production` at 1.0: one attributable record for every state-changing command, with a hash chain in the ledger's `audit_events` table; the cluster-command half is not durable yet.
- **Tests and CI:** `crates/vtop-cli/tests/audit_trail.rs`, `audit_cluster_client.rs`; the `audit_events` trigger in `pg_battery`; the `audit_record` fuzz target.
- **Open criteria:** #706 (below feature level 9 the actor of a cluster command is in the admin endpoint's record only, from level 9 it is in the replicated log, #850; the applied records are delivered at least once, #767, but do not yet carry `requested_by`); a mixed fleet's trail is only as complete as its least-upgraded node.
- **Docs:** `operate/auditing.md`, `operate/logs-and-auditing.md`.

### Prometheus metrics, alert rules and dashboards

- **Why:** `preview`, `production` at 1.0 for a stable subset of metric names (the compatibility policy plans one).
- **Tests and CI:** `crates/vtop-cli/tests/integration_metrics_endpoint.rs`; the `prometheus rules` job (`promtool` tests, a runbook per alert); the `dashboards` job (generators, no drift).
- **Open criteria:** C3 (#719): no snapshot gate on metric names and labels yet; no store write-latency SLO.
- **Docs:** `operate/monitoring.md`, `operate/runbooks.md`, `operate/slos.md`.

### OpenTelemetry export

- **Why:** `experimental`: opt-in, HTTP/protobuf only, and its span names and attributes are under no stability gate.
- **Tests and CI:** `crates/vtop-cli/tests/otel_off.rs`, `otel_messages.rs`, `otel_hung_collector.rs` (a hung collector does not block the batch path).
- **Open criteria:** #798 (an intermittent failure of `otel_messages` whose cause is not found); gRPC is not built.
- **Docs:** `limitations.md`, `observability/README.md`.

### Metadata plane (`vtop-meta`)

- **Why:** `preview` at both stages: Raft metadata with deterministic crash sweeps and live scenarios, but without a history checker or a soak.
- **Tests and CI:** `crates/vtop-meta/tests` (`raft_three_node`, `storage_crash_sweep`, `recovery_e2e`, `state_machine_matrix`, `codec_golden`); live-chaos scenarios 01 to 03, 06, 08, 22 (rolling upgrade); fuzz targets `meta_storage_files`, `vtpm_frame`.
- **Open criteria:** A3 7-day soak (#727); A4 history checker (#728); D1 external review (#730); one data-safety bug (#657); its formats "follow #740" in the inventory.
- **Docs:** `deploy/cluster-plane.md` carries a prototype warning.

### Data node (`vtop-node`)

- **Why:** `preview` at both stages: leases, fencing epochs and verified promotion are exercised, and promotion has had three data-safety bugs.
- **Tests and CI:** `crates/vtop-node` unit and crash-sweep tests (`promotion_crash_sweep`); live-chaos scenarios 04, 09, 10, 12, 14, 23, 27, 28.
- **Open criteria:** A3 (#727), A4 (#728); group commit is wired into the data node and off by default until its A/B gates hold (#752); data-safety bugs #449, #611, #641.
- **Docs:** `NATIVE_BROKER_ARCHITECTURE.md`, `NODE_OBSERVABILITY.md`.

### Replicated broker (`vtop-broker`)

- **Why:** `preview` at both stages: quorum replication with fenced epochs and the segment log (`vtop-log`), with the most crash-sweep coverage in the repository, and the most bugs.
- **Tests and CI:** `crates/vtop-broker/tests` (`quorum_replication`, `replica_fencing`, `divergence_truncation`, `sealed_segment_transfer`, the committed-floor, kept-dispute, epoch-journal and group-commit crash sweeps); `crates/vtop-log/tests` (`sim_crash_sweep`, `startup_crash_matrix`); the `loom model (group commit)` job; live-chaos scenarios 04, 05, 05b.
- **Open criteria:** A3, A4, D1; six data-safety bugs (#261, #264, #315, #553, #635, #640); segment format v2 is evolving.
- **Docs:** `LIVE_CHAOS_VALIDATION.md`.

### Kafka gateway

- **Why:** `experimental` today, `preview` at 1.0: its advertised APIs are `experimental` in the compatibility inventory, and a client following a failover is measured only on one host with debug binaries and a 6 s lease (#751).
- **Tests and CI:** `crates/vtop-kafka` tests; the `kafka_request` fuzz target; live-chaos scenarios 16 to 21 and 24 with `kcat` (librdkafka) as the stock client; scenarios 27 (`kcat`) and 28 (the JVM client of Apache Kafka 4.4.0) kill the range leader under a client served from candidate nodes, which resumed 5.5 s to 6.6 s later with every acknowledged record read back once and in order; the README's comparison against Apache Kafka.
- **Open criteria:** #751's remainder: the failover is measured on one host with debug binaries and the harness's 6 s lease, not with the chart's 15 s default lease, not in the co-located `vtop-node node` shape, and a consumer group's coordinator across a failover is out of its scope (members rejoin); #752; 12.0x to 13.9x lower throughput than Apache Kafka at saturation on one laptop (#596 to #600, #610, #737); transactions are out of scope (a transactional `InitProducerId` is refused by name); A3, A4.
- **Docs:** README, `when-to-use.md` ("a prototype, measured well below Apache Kafka's throughput") and the benchmark pages.

### Helm chart

- **Why:** `preview` at both stages: it deploys the cluster plane, so it cannot be above it.
- **Tests and CI:** the `helm (lint + render + refusals)` job and `helm (deploy to kind)` job; values schema; ci value sets.
- **Open criteria:** there is no engine chart (Production HA Phase 7, not started), so the supported engine deployment is a process or a container; chart values are "classified at the freeze" (compatibility inventory row 27).
- **Docs:** `helm/vtop/README.md`, `install/helm.md`, `deploy/kubernetes.md`.

## Not built

| What | State | Proposed |
|---|---|---|
| Opt-in datagram egress transport | Nothing in the tree; `SECURITY_MODEL.md` says it is planned, and the measured TCP path was kept (#483). | Take it out of the 1.0 scope, D9 |
| Several engines sharing a ledger (fleet mode) | Not started (Production HA Phase 5); the engine refuses a second writer. | A stated limit, not a labelled component |
| An engine Helm chart | Not started (Production HA Phase 7). | As above |
| Public-key manifest signatures | Not built (#714 section 6). | As above |

## How this page is kept honest

`scripts/check-component-status.py` runs in the docs workflow. It fails when:

- the table is missing a component, lists an unknown one, or uses a word other
  than `production`, `preview` or `experimental`;
- a row is `PROPOSED` and the page's *Decision status* line is not, or the
  reverse;
- a component has no evidence section, or a `production` label sits beside an
  *Open criteria* line that is not "none";
- the README or any page of the site says a component is production-ready, or
  "production grade", "battle-tested" and the like, and the table
  labels it lower, or says it of the project as a whole while any row is not
  `production`. A sentence that denies, conditions or plans the claim ("not
  production-ready", "until ...", "the bar for ...") is allowed;
- the README does not link to this page.

The labels are confirmed: the rows read `CONFIRMED` and the status line
follows. The README and the other pages must keep saying no more than the
table does.

# Egress transport research

The measurement record for the egress-transport milestone (#475): what each
transport actually does on links that bite, recorded under conditions stated
completely enough that every number can be re-taken. The lab this document
reports from is built by the milestone's Phase 1 issues (#476–#478); until
those land, the table below is a contract with no rows, and that is the
honest state.

This document is linted (`scripts/check-egress-claims.sh`, run in CI): a
measurement row missing any required condition fails the build naming the
missing column. The rules the whole document is written under are in the
standing section at the end — read them before adding a row or a sentence.

## Measurements

Every row carries all of: the hardware it ran on; the round-trip time; the
loss rate AND the loss model (random, burst, policer-induced — they behave
nothing alike); the bottleneck rate; the bottleneck buffer depth; the
competitor state (whether a competing flow shared the link, and what kind);
the transport under test; what actually limited throughput; and the date.
A number without its conditions is not a measurement, it is a rumor.

| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |
|----------|-----|-----------|------------|-----------------|--------------|------------------|-----------|------------|------|

## The tuned-TCP baseline, and the gate (#483)

### The gate, fixed before the sweep was read

This subsection was committed before any sweep run was read, and the results
below are judged against it unchanged. The commit that introduced it is the
evidence of the order.

**Definitions.**

- **VTOP goodput `G`** (Mbit/s): the committed object bytes of a run × 8,
  divided by the wall time of the engine cycles that committed them
  (`cycle_metrics.csv`, one row per `process-once`). Object bytes only:
  manifests, HTTP framing and retransmissions are excluded, so `G` understates
  the wire.
- **Link goodput `C`** (Mbit/s): the same run's `emulator_validation_mbps` —
  one iperf3 TCP flow through the shape's impairment-free twin (same rate,
  buffer and mean delay; no loss), measured by the harness immediately before
  that run. On the lossy profile `C` is therefore the link's goodput without
  the loss, which is the ceiling a loss-tolerant transport is being asked to
  approach.
- **Unused fraction `U`** = 1 − `G`/`C`.
- **Best-tuned configuration, per profile**: the grid point (below) with the
  highest median `G`/`C` across its repetitions on that profile. Ties within
  two percentage points go to the configuration with fewer connections
  (`max_concurrency` × `parts_in_flight`), because it costs a neighbour less.
- **Confirmation runs.** The best-tuned point is selected on the grid's
  repetitions and then re-measured; the gate is evaluated on the confirmation
  runs alone, so the configuration that won by noise is not also the one
  whose noise is reported.

**The thresholds.** Two single numbers. The datagram issues (#485, #486
and #488) open if **either** is crossed; if neither is, they do not open, and
that is recorded here as the outcome.

1. **Goodput: `U` > 0.20** — the best-tuned TCP configuration leaves more than
   20% of the link's goodput unused, by the median of its confirmation runs, on
   **either** 100 ms profile (10 Mbit/s with 0% loss, or 10 Mbit/s with 1%
   `gemodel` loss).
2. **Tail: `R` > 2.0** — where `R` is `object_upload` p95 on the 1% `gemodel`
   profile divided by `object_upload` p95 on the 0% profile, both at the
   configuration that is best-tuned on the 1% profile, each p95 taken over
   every object of that profile's confirmation runs pooled.

**Noise rule, also fixed now.** A verdict is stated on the medians. If a
threshold lies inside the range (minimum to maximum) of the repetitions that
decide it, the verdict is reported as within noise, and the document says the
gate should be re-measured before #485 is scheduled either way.

**Not part of the gate.** The clean-LAN profile (no RTT to speak of) and the
policed-uplink row. The policed row has no calibrated `C` — a policer has no
queue for a single flow to fill, and the harness's probe deliberately runs on
the twin without it — so it is reported against the policer's configured
token rate and read as a finding, not as a threshold.

### The sweep

- **Grid.** `upload.transports.tcp_tls.max_concurrency` ∈ {1, 2, 4, 8, 16}
  (with `batching.max_concurrent_batches` set to the same value, since the
  engine takes the minimum of the two) × `parts_in_flight` ∈ {1, 2, 4}, with
  `part_size_bytes` 5 MiB — the smallest part S3 and MinIO accept, so that a
  20 MiB object is four parts and every `parts_in_flight` value is
  distinguishable.
- **Untuned default**, for comparison and never as the baseline: no tuning
  block, so the shipped values — eight concurrent batches, 8 MiB parts, four
  parts in flight.
- **Profiles.** Clean LAN (the middlebox in the path, no qdisc); 100 ms RTT at
  10 Mbit/s, one BDP of buffer, 0% loss; the same with 1% `gemodel` loss
  (bursty, mean burst two packets) on the upload direction; and a policed
  uplink — a 10 Mbit/s policer and no queue, at 100 ms.
- **Workload.** Per run, a fixed backlog of `max(4, 2 × max_concurrency)`
  objects of 20 MiB each (JSONL, `compression: none`, so wire payload equals
  object size; SHA-256 checksums; `s3_native` to the lab MinIO), drained by
  `process-once`.
- **Repetitions.** At least two per grid point; the default and the
  best-tuned configuration re-measured to at least five on each 100 ms
  profile.

## Counterparty survey: which S3-compatible endpoints serve HTTP/3 (#484)

A transport needs something on the other end. The datagram path this
milestone contemplates is HTTP/3 over QUIC to an S3-compatible endpoint, so
before any transport code is written one fact has to be established and
written down: does anything reachable actually answer it?

**Method.** Each endpoint was sent an anonymous `HEAD /` over HTTPS and its
response headers read for `Alt-Svc` advertising `h3`. `Alt-Svc` is the
mechanism by which an origin tells a client it also serves HTTP/3, and it is
how a client discovers the option in the first place — a server that does not
advertise it is a server no ordinary client will reach over HTTP/3. No
credentials were sent and no object was read or written; the request line and
the response status are the whole interaction. This is an ADVERTISEMENT check,
not a completed HTTP/3 handshake: it establishes which endpoints offer the
protocol, not that a particular client library succeeds against them. Proving
negotiation end to end is the lab target's job, below.

**Date.** 2026-09-14. This is a moving surface and the result should be
re-taken before it is relied on again; the method above is cheap to repeat.

| Endpoint | HTTP status | Advertises HTTP/3 |
|---|---|---|
| `storage.googleapis.com` | 400 | **yes** — `alt-svc: h3=":443"; ma=2592000,h3-29=":443"; ma=2592000` |
| `s3.amazonaws.com` | 405 | no |
| `s3.us-east-1.amazonaws.com` | 405 | no |
| `s3.wasabisys.com` | 303 | no |
| `nyc3.digitaloceanspaces.com` | 200 | no |
| `s3.us-west-004.backblazeb2.com` | 405 | no |
| `s3.fr-par.scw.cloud` | 200 | no |
| `us-east-1.linodeobjects.com` | 200 | no |
| `play.min.io` | 400 | no |

The status column is recorded because it shows the endpoint answered: a `405`
or `400` to an anonymous `HEAD /` is a live S3-compatible service declining
that particular request, not an unreachable host. No endpoint in the list
failed to respond.

**What this decides.** One endpoint of nine advertises HTTP/3, so the
counterparty question does not close the datagram phase — a real target
exists. But the shape of the answer matters as much as the answer: the
protocol is not broadly available across S3-compatible storage, and it is
absent from the endpoint most deployments of this engine would point at. A
datagram path would therefore be usable by a minority of deployments, and any
decision to build it should be made in that knowledge rather than in the
expectation that the protocol is generally available. The measured value of
the path, if it is built, is still gated on the tuned-TCP baseline (#483).

This survey records what the probe found. It is not a recommendation of any
endpoint or vendor, it makes no comparative claim about any of them, and the
absence of `Alt-Svc` says only that the endpoint did not advertise HTTP/3 to
this probe on this date — not that the service is deficient.

### The lab's HTTP/3 target, and what the proxy hop costs

A counterparty that exists somewhere on the public internet is not a counterparty
this project can measure against. The lab therefore carries its own
HTTP/3-terminating target on the `h3` compose profile: an nginx reverse proxy
serving HTTP/3 over QUIC on 9443 and forwarding to the lab's MinIO.

**HTTP/3 is verified, not assumed** (`benchmarks/verify-h3.sh`). The check has
four parts, because a downgrade here would make every later measurement a
measurement of TCP under a different name: a QUIC client that offers only `h3`
as ALPN and reads the negotiated protocol off the handshake; the proxy's own
access log, which a client cannot make lie; a plain-TCP control through the same
proxy, which logs `HTTP/2.0` and so proves the log field discriminates; and the
same probe aimed at a port with no QUIC listener, which must fail — so the check
is known to be capable of failing rather than merely observed to pass.

**The topology confound, and the number for it.** A proxy in front of the store
changes the path, so a later transport comparison must be made against the TCP
path *through the same proxy*, not against the direct one. Scenario 17 is that
control. Measured on 2026-09-14, on one host over loopback with no shaping, the
matched TCP scenarios give:

| | direct to MinIO (scenario 12) | through the proxy (scenario 17) |
|---|---|---|
| `object_upload` p95 | 21.0 ms | 23.0 ms |
| `object_upload` p50 | 11.0 ms | 11.0 ms |
| objects committed | 15498 | 3321 |
| wall time | 92.6 s | 75.8 s |

**Only the per-object latency pair is a usable comparison, and the difference it
shows is +10% at p95.** The aggregate throughput figures from this pair are NOT
comparable and must not be quoted: the two runs did not process comparable
volumes in comparable wall time (both are backpressure soaks that drain by
cycle, and they completed different numbers of cycles), and the proxied run
recorded one error. A throughput price for the hop needs a matched-volume run
and is not established here.

Two further things this pair does not isolate, both inherent rather than
incidental: scenario 12 reaches MinIO over plaintext HTTP/1.1, while a proxy
that can serve HTTP/3 cannot be plaintext, so scenario 17 also pays TLS
termination and an ALPN change — the SDK negotiated HTTP/2 through it. That is
precisely why the datagram comparison, if it is ever made, belongs against
scenario 17 and not against scenario 12: both sides of that pair cross the same
proxy, and the wire is then the only difference.

## What may and may not be said

This section is standing policy for this document, the README, release
notes, and any surface that describes this project — not guidance, policy.
The naming half is enforced by `scripts/check-naming.sh` against the term
list in `scripts/naming-denylist.txt`; the substantiation half is enforced
on this document by `scripts/check-egress-claims.sh`; the rest is enforced
by review, under CONTRIBUTING.md's "Naming and claims".

**May be said:**

- Any number in the table above, quoted WITH its row's conditions.
- That the project offers two first-class egress transports, selected by
  configuration, with the TCP path as the default — once that is true and
  tested, and not a day before.
- That the datagram path is HTTP/3 over QUIC using a published, standardised
  protocol stack — a statement about protocols, not products — ONCE that
  path exists in the tree and is tested. Today no QUIC or HTTP/3
  implementation or dependency ships, and until the milestone's Phase 4
  lands, no sentence may say or imply the project has such a path; the
  permitted present-tense statement is that one is planned, behind the
  gate the umbrella (#475) defines.
- Comparisons between this project's own two paths, on the same link, same
  tuning effort, same table.

**May not be said:**

- No compatibility, equivalence, interoperability or performance-parity
  claim about any commercial product. A parity claim about a product with no
  public specification is unverifiable by construction, so it cannot be
  substantiated — and making one frames independent work as an attempted
  reproduction. Neither half of that is worth it.
- No commercial transport product's name anywhere the product describes
  itself: not in a crate, module, feature flag, config key, CLI flag, metric
  label, transport variant, scenario name, branch name, or document outside
  the two exempt prior-art files. Internal shorthand counts, because
  internal shorthand leaks into READMEs.
- No throughput claim without its conditions. If the sentence cannot carry
  the row, the sentence does not ship.
- No multiplier against an untuned baseline. The baseline is the TUNED TCP
  path (#483 defines it); "N× faster than defaults nobody would run" is an
  advertisement, not a result.
- No claim that anything here is a congestion control algorithm. What ships
  is rate and concurrency policy over transports that bring their own
  congestion control.
- No claim about a system we have not measured, and no restating another
  party's numbers as if they were ours.

## Relationship to the prior-art record

Published and commercial systems are cited by name where citing them is the
job: `docs/PRIOR_ART_SEARCH_PLAN.md` (and the disclosure draft it feeds).
Those two files are the explicit, complete exemption to the naming rule.
This document cites protocols and standards, never products.

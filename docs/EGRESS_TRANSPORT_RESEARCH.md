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

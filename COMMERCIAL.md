# Licensing: what is open, and what is not

VTOP Engine is licensed under the **GNU Affero General Public License v3.0
only** (AGPL-3.0-only) from v0.12.0, and the whole engine that exists today is
covered by that licence. Releases v0.1.0 through v0.11.2 were published under
the MIT License; those published copies stay under MIT. This document exists
so that the boundary is written down *before* there is anything on the other
side of it — a project that decides where the line runs after it has code to
put there tends to move the line instead.

## The two licences

| Scope | Licence | What you may do |
|---|---|---|
| Everything **except** the row below | [AGPL-3.0-only](LICENSE) | Run it, fork it, sell it, embed it. If you distribute it, or let users interact with a modified version over a network, you must offer them the corresponding source under the same licence. A commercial license without those conditions is available by agreement. |
| Directories named `ee/`, and files carrying the header `VTOP Engine Enterprise Edition` | [LICENSE-EE](LICENSE-EE) | Evaluate, develop, and test freely. Production use requires a commercial subscription. |

**There is no `ee/` directory today.** Nothing in the repository is currently
licensed under LICENSE-EE, so today the practical answer to "what licence is
this?" is: AGPL-3.0-only, all of it (MIT for v0.11.2 and earlier).

## The rules this boundary follows

These are constraints on the project, not on you.

1. **The core never becomes EE.** Core code stays under an open-source
   licence. The core moved from MIT to AGPL-3.0-only in v0.12.0, discussed in
   the open in the pull request that made the change before it merged, as this
   rule required. A licence change cannot be applied retroactively to a
   published version: v0.1.0 through v0.11.2 remain MIT. Any further change to
   the core's licence gets the same treatment.

2. **The core never depends on `ee/`.** The dependency arrow points one way:
   EE code may use the core, and the core must build, test, run, and pass its
   full CI matrix with `ee/` deleted. A feature that the core cannot work
   without does not belong in `ee/`.

3. **Safety and correctness are not EE.** Durability, fencing, replication,
   failover, verification, and anything whose absence could lose or corrupt
   acknowledged data stay in the core. Paying is for operating at scale, not
   for not losing data.

4. **EE code says so in its header.** A file under EE terms carries
   `VTOP Engine Enterprise Edition` at the top. **Either marker is
   sufficient** — a file inside `ee/` is EE Software whether or not it carries
   the header, and a header-marked file is EE Software wherever it sits. There
   is no precedence between them, because there is no case where one excuses
   the other: the practice is to use both, and the header exists so that a file
   read on its own still says what it is.

## What EE is plausibly for

Named so the boundary is concrete rather than a placeholder, and deliberately
short. Nothing here is a commitment or a roadmap:

- multi-tenancy and quota enforcement across many ranges;
- long-term retention tiers and their lifecycle management;
- integrations with commercial identity, audit, and key-management systems;
- operator tooling for fleets rather than a cluster.

## Contributing

Contributions to the core are accepted only with a written contributor
agreement, because the core is also offered under a commercial license and
the Licensor cannot offer someone else's contribution under other terms
without their agreement — see [CONTRIBUTING.md](CONTRIBUTING.md).
Contributions to `ee/` require the same.

## Getting commercial terms

For a commercial license to the core, or for EE terms, open an issue titled
`commercial licensing`, or contact the maintainer directly. Please say what you are deploying, roughly at what scale, and which
capability you need — it is a faster conversation than a generic enquiry.

---

*This document describes the project's intent in plain language. Where it and
the licence files differ, [LICENSE](LICENSE) and [LICENSE-EE](LICENSE-EE)
govern.*

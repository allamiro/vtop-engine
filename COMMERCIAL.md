# Licensing: what is open, and what is not

VTOP Engine is **MIT-licensed**, and the whole engine that exists today is
covered by that licence. This document exists so that the boundary is written
down *before* there is anything on the other side of it — a project that
decides where the line runs after it has code to put there tends to move the
line instead.

## The two licences

| Scope | Licence | What you may do |
|---|---|---|
| Everything **except** the row below | [MIT](LICENSE) | Anything MIT permits: run it, fork it, sell it, embed it. No conditions beyond the notice. |
| Directories named `ee/`, and files carrying the header `VTOP Engine Enterprise Edition` | [LICENSE-EE](LICENSE-EE) | Evaluate, develop, and test freely. Production use requires a commercial subscription. |

**There is no `ee/` directory today.** Nothing in the repository is currently
licensed under LICENSE-EE, so today the practical answer to "what licence is
this?" is: MIT, all of it.

## The rules this boundary follows

These are constraints on the project, not on you.

1. **The core never becomes EE.** Code that is MIT today stays MIT. A licence
   change cannot be applied retroactively to a published version anyway, and
   re-licensing the core going forward is not the plan here. If that ever
   changes it will be its own discussion, in the open, before any commit.

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

Contributions to the MIT portion are accepted under MIT — see
[CONTRIBUTING.md](CONTRIBUTING.md). Contributions to `ee/` require a separate
written agreement, because the Licensor cannot relicense someone else's MIT
contribution into a commercial edition without their agreement.

## Getting commercial terms

Open an issue titled `commercial licensing`, or contact the maintainer
directly. Please say what you are deploying, roughly at what scale, and which
capability you need — it is a faster conversation than a generic enquiry.

---

*This document describes the project's intent in plain language. Where it and
the licence files differ, [LICENSE](LICENSE) and [LICENSE-EE](LICENSE-EE)
govern.*

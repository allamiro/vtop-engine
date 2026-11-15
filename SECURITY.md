# Security policy

How to report a vulnerability in VTOP Engine, what happens after you do, and
which versions receive fixes. The security properties the engine claims, and
the ones it does not, are in the [security model](docs/SECURITY_MODEL.md).

## Reporting a vulnerability

Report it privately. Do not put any detail of it in a public issue, pull
request or discussion.

**Use GitHub's private vulnerability reporting:**
<https://github.com/allamiro/vtop-engine/security/advisories/new>. Only the
maintainers and you can read the report and the conversation that follows.

**If that page offers no form to fill in** (private reporting has been switched
off for the repository), open a public issue titled `Security contact request`
with the
[security contact request form](https://github.com/allamiro/vtop-engine/issues/new?template=security_contact.yml),
and write nothing about the problem in it: no component, no symptom, no
version. The maintainer replies there with a private channel, and the issue is
closed once you are in touch.

A useful report gives:

- the version (`vtopctl --version`) or the commit you built;
- the configuration involved, with secrets removed;
- the steps that show the problem, or a test that fails;
- what an attacker gains, and what access they need first.

## What happens next

These are targets, not guarantees. If one is going to be missed, you are told
why and given a new date.

| Step | Target |
|---|---|
| Acknowledgement of your report | within 3 working days |
| First assessment: confirmed or not, and a severity | within 10 days of the report |
| A fix released | by severity, counted from confirmation (table below) |

| Severity | Fix target |
|---|---|
| Critical | 30 days |
| High | 60 days |
| Medium | 90 days |
| Low | the next scheduled release |

Severity follows the CVSS v3.1 base score bands: critical 9.0 to 10.0, high 7.0
to 8.9, medium 4.0 to 6.9, low 0.1 to 3.9. One project rule raises it: a
vulnerability that lets an attacker cause any of the outcomes VTOP exists to
prevent is **at least high**, whatever its score. Those outcomes are a source
position committed without verified data, a lost or silently skipped range, an
acknowledged record lost, corruption that passes verification, and the deletion
of source bytes that were not archived.

## Supported versions

VTOP is in its 0.x series. Only the latest minor release receives security
fixes: today that is v0.10.x. A report against an older minor is answered with
the release that fixes it, and the advice to upgrade. Downgrades are not
supported; see [Versioning](docs/ROADMAP.md#versioning) in the roadmap.

| Version | Security fixes |
|---|---|
| 0.10.x | yes |
| older than 0.10 | no: upgrade to 0.10.x |

## Coordinated disclosure

- Keep the report private until a fix is released or until **90 days** after
  the report, whichever comes first.
- The maintainer asks for more time only with a reason and a new date.
- A fix is released with a GitHub security advisory that describes the problem
  and the fixed version, and a CVE is requested where one applies.
- You are credited in the advisory unless you ask not to be.
- Research done in good faith under this policy will not be pursued. Test
  against systems you own or are allowed to test, and do not access, change or
  delete other people's data.

## Scope

In scope:

- the archive engine (`vtopctl run`) and every `vtopctl` command;
- the cluster plane: `vtop-node`, its admin and replica planes and the metadata
  tier;
- the Kafka gateway;
- the release artifacts and their signatures: the archives, `SHA256SUMS`, the
  Sigstore bundles and the SBOM ([how to verify them](docs/RELEASE_VERIFICATION.md));
- the container image and the Helm chart.

Out of scope:

- the properties the security model lists as not provided
  ([section 14](docs/SECURITY_MODEL.md#14-security-properties-provided-vs-not-provided)).
  For example, size-only verification accepts same-size corruption when an
  operator opts out of strong verification, and the manifest MAC is a single
  shared key, not a public-key signature;
- findings that need an already-compromised host, or a credential the security
  model treats as trusted (storage credentials, the state-store URL, the
  manifest MAC key);
- a cluster-plane listener that its operator switched to plaintext.

A data loss that needs no attacker is a bug, not a vulnerability. Report it
with the [data-safety issue form](https://github.com/allamiro/vtop-engine/issues/new?template=data_safety.yml);
the bugs of that kind found so far carry the
[`data-safety` label](https://github.com/allamiro/vtop-engine/issues?q=label%3Adata-safety).

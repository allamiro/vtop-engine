# Rotate the manifest MAC key

With a manifest MAC key configured, every manifest carries a keyed BLAKE3 MAC,
and a manifest whose MAC does not verify is treated as unauthenticated:
recovery replays its batch and the scrub reports it. This page is the
procedure for changing that key without making the archive you already have
unauthenticated.

## How rotation works

Manifests are never re-signed. They are immutable, pinned by version, and
often under Object Lock, so after a rotation the archive holds manifests
signed by several keys for as long as it is retained. The engine therefore
keeps a **keyring** (`manifest.mac`, see the
[reference](../configure/reference.md#manifestmac)):

- **One key signs.** `manifest.mac.active` names it. A version 0.4 manifest
  records the signing key's id in `manifest.mac_key_id`, covered by the
  manifest's hash and MAC.
- **The key a manifest names verifies it.** A reader never tries its other
  keys. A manifest with no id (every manifest written before key ids, and any
  written with `manifest_mac_key_env`) is verified with the key marked
  `legacy: true`.
- **"Retire" means stop signing.** A rotated-out key stays in `keys` as a
  verify-only entry for as long as anyone audits its manifests. Only a key
  that was withdrawn or compromised goes in `retired`, and then its manifests
  fail authentication on purpose.

A key id is a label, not a secret: it appears in manifests, log messages and
metric labels. Pick ids an operator can read in a runbook, such as
`q4-2026`.

## Before you start

- **Every reader must run the keyring release** (v0.10.0 or later): every
  engine sharing the ledger, every `vtopctl scrub` and `verify-manifest`, and
  any auditor of the archive. A reader on v0.8.0 or earlier holds one key and
  ignores key ids: it verifies only the manifests its key signed and reports
  the rest as unauthenticated, and an engine on it replays those batches.
  The engine cannot see which releases read the archive, so this check is
  yours. v0.7.0 and earlier cannot read any version 0.4 manifest.
- **Manifests must be version 0.4.** A key id can only be recorded in a 0.4
  manifest, so `manifest.mac` requires `manifest.write_version: "0.4"`, and
  the config is refused otherwise. Enabling 0.4 has its own reader rule
  (every reader on v0.8.0 or later); see
  [`manifest`](../configure/reference.md#manifest).
- **Keys come from environment variables or files.** Each is 32 bytes, 64
  hex characters, for example `openssl rand -hex 32`. A key file other
  local users can write is refused, and so is one they can read, unless it
  is a Kubernetes Secret mounted with `defaultMode: 0400` and `fsGroup`, or
  sits in a directory closed to them. Mount each key readable by the
  engine's user only; the rule and the recipes are in
  [Secret files and their permissions](secrets.md).

To make the new key the example uses, on each host that needs it, as root or
through your secret manager (the same 64 characters on every host, so generate
it once and distribute it, never once per host):

```bash
( umask 077; openssl rand -hex 32 > /run/secrets/vtop-manifest-mac-q4 )
chown vtop:vtop /run/secrets/vtop-manifest-mac-q4   # the engine's user
chmod 0400 /run/secrets/vtop-manifest-mac-q4
```

The file holds only the 64 hex characters (a trailing newline is fine). Hosts
that only audit (`vtopctl scrub`) need the key readable by the user who runs
the scrub.

## The procedure

The example rotates from the key the fleet signs with today, held in
`VTOP_MANIFEST_MAC_KEY` by `manifest_mac_key_env`, to a new key, `q4-2026`.

### 1. Add the new key as verify-only, everywhere

Move the current key into a keyring under an id, keep it active and mark it
`legacy` (it verifies every manifest written before key ids). Add the new
key, which only verifies for now:

```yaml
manifest:
  write_version: "0.4"
  mac:
    active: q3-2026
    keys:
      - {id: q3-2026, env: VTOP_MANIFEST_MAC_KEY, legacy: true}
      - {id: q4-2026, file: /run/secrets/vtop-manifest-mac-q4}
```

Remove `manifest_mac_key_env`: setting both is refused. Roll this out to
every engine, scrub job and auditor, and **restart each engine** (the keyring
is read at startup; there is no reload: `systemctl restart vtop-engine`, or a
rolling restart of the Deployment). Nothing signs with the new key yet, so
an engine that has not been rolled out yet still verifies everything the
others write: a process still on `manifest_mac_key_env` holds one key and
ignores key ids, as v0.8.0 does, so it verifies any manifest that key signed,
with an id or without.

Check each process before going on. `validate` reads the config only: it
does not read the variables or files, so a missing or malformed key passes it
and stops the engine at startup, naming the key id and the variable or file.
Check the startup line too.

```bash
vtopctl validate --config /etc/vtop/config.yaml --print-effective | grep manifest_mac
```

It prints one such line for each stream and one for `default`:

```text
  manifest_mac: keyring: active key q3-2026 (env, legacy); verify-only: q4-2026 (file); retired: none (global)
```

The engine logs the same line at startup, at INFO:
`manifest MAC keyring: active key q3-2026 (env, legacy); verify-only: q4-2026 (file); retired: none`.
Neither prints a key, a variable's name or a file's path.

### 2. Confirm every reader runs the keyring release

This is the gate the engine cannot enforce. Any reader still on v0.8.0 or
earlier will report every manifest signed in step 3 as unauthenticated.

### 3. Switch the active key

On the engines, change `active`:

```yaml
manifest:
  write_version: "0.4"
  mac:
    active: q4-2026
    keys:
      - {id: q3-2026, env: VTOP_MANIFEST_MAC_KEY, legacy: true}
      - {id: q4-2026, file: /run/secrets/vtop-manifest-mac-q4}
```

New manifests carry `"mac_key_id": "q4-2026"`. Restart each engine again. The
[audit trail](auditing.md) records `migrate`, `replay` and `prune-ledger`, not
a change of key, so the startup line (`manifest MAC keyring: active key ...`)
is the record of when each engine began signing with the new one; keep the
log.

### 4. Scrub, and read the counts by key

Run it as a user who can read the key files and with the legacy key's
variable exported (`VTOP_MANIFEST_MAC_KEY` here); the scrub resolves the
keyring as the engine does, and a key it cannot read stops it before it
reports anything, naming the key id.

```bash
vtopctl scrub --config /etc/vtop/config.yaml
```

```text
scrubbed 6 of 6 committed batch(es) (6 ledger row(s), 0 digested row(s) from 0 digest file(s)); mode all
results: limited=0 mismatched=0 missing=0 ok=6 unauthenticated=0 unreadable=0 unsupported_version=0
manifests authenticated by key: q3-2026=4 q4-2026=2
scrub PASSED
```

Every manifest, old and new, must verify. `q3-2026` keeps verifying the
manifests it signed, and the ones with no key id, for as long as they are
retained, so leave it in `keys`. With `--metrics-file`, the same counts are
`vtop_scrub_manifest_keys_total{key_id}`.

### 5. Tombstone a key only if it was withdrawn

If a key was compromised or must no longer be trusted, move its id from
`keys` to `retired`:

```yaml
manifest:
  write_version: "0.4"
  mac:
    active: q4-2026
    keys:
      - {id: q4-2026, file: /run/secrets/vtop-manifest-mac-q4}
    retired: [q3-2026]
```

Manifests signed with it then fail authentication, by design:

- the scrub reports each as `unauthenticated`, with the detail
  `manifest is unauthenticated: it names key id "q3-2026", which is retired (manifest.mac.retired)`;
- recovery replays a VERIFIED batch signed with it instead of committing it,
  and archives it again under the active key (duplicates, never a gap);
- manifests with no key id now have no legacy key, and fail with
  `it names no key id and no key in manifest.mac.keys is marked legacy: true`.

Re-archiving data that was signed with a withdrawn key is a separate
decision: the engine never re-signs a stored manifest.

`vtopctl audit verify` does not fail because of it: audit-trail rows keyed
with a retired key are counted `NOT CHECKED`, their links to the rows around
them are still checked ([Auditing](auditing.md#tamper-evidence)). To undo the
withdrawal, move the id from `retired` back into `keys` and restart; no
manifest was changed, so the scrub is clean again.

## What the messages mean

| Message (in the scrub detail, `verify-manifest` and logs) | Cause | What the engine does | What to do |
|---|---|---|---|
| `MAC verification failed for key id "X"` | The manifest names a key this process holds, and the MAC does not match. | Unauthenticated: recovery replays, the scrub reports it. | Check that `X` holds the same key on every process. Otherwise the manifest was altered. |
| `it names key id "X", which is not in this keyring` | Signed with a key this process was not given. The bytes may be intact. | Unauthenticated, not corrupt: recovery replays, the scrub reports it. Recovery logs an ERROR with `key_id="other"`. | Add `X` to `manifest.mac.keys` as a verify-only entry on every process. |
| `it names key id "X", and the one manifest_mac_key_env key does not verify it` | A process on `manifest_mac_key_env` met a manifest signed by another key. | As above. | Move that process to `manifest.mac` with `X` as a verify-only entry. |
| `it names key id "X", which is retired` | `X` is in `manifest.mac.retired`. | Unauthenticated: recovery replays (WARN), the scrub reports it. | Expected if `X` was withdrawn. Otherwise move it back to `keys`. |
| `it names no key id and no key ... is marked legacy: true` | A manifest from before key ids, and no key is `legacy`. | Unauthenticated: recovery replays (ERROR with `key_id="none"`), the scrub reports it. | Mark the key that signed before key ids (the old `manifest_mac_key_env` key) `legacy: true`. |
| `manifest MAC is required ...` | A key is configured and the manifest has no MAC. | Refused, as before key ids. | Unsigned manifests from before MACs were enabled must be verified, or migrated, separately. |

None of these says the manifest is corrupt, and none of them adds a scrub
result: they are all `unauthenticated`, and the detail names the key.

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `vtop_manifest_authentications_total` | `key_id`, `result` | Every manifest MAC check by the engine: its read-back of each stored manifest, and recovery's re-check. Exported by `vtopctl run` with `VTOP_METRICS_ADDR` set, like the engine's other series. Each configured key's series, and `other`/`unknown_key`, exist at zero from startup, so `increase()` sees the first unknown key. `key_id` is a configured id, `none` (a key with no id, or no id and no legacy key) or `other` (an id this process does not hold). `result` is `ok`, `failed`, `unknown_key` or `retired_key`. |
| `vtop_scrub_manifest_keys_total` | `key_id` | In the scrub's `--metrics-file`: manifests the scrub authenticated, by the configured key that verified them. |

`key_id` is never an id read from a manifest: that comes from storage and
would be unbounded. Alert on any `result="unknown_key"`: it means a process
is missing a key that signed part of the archive. The rule below is an
example; it is not in the [shipped rule set](monitoring.md#alerts).

```yaml
- alert: VtopManifestKeyUnknown
  expr: increase(vtop_manifest_authentications_total{result="unknown_key"}[15m]) > 0
  labels: {severity: page}
  annotations:
    summary: "A manifest names a MAC key this engine does not hold"
    description: "Add the key to manifest.mac.keys as verify-only on every engine; until then recovery replays those batches."
```

## Engines that do not have the keyring

An engine before the keyring release ignores the `manifest.mac` section. If
it still has `manifest_mac_key_env` set to the old key, it keeps signing with
that key and no id, and those manifests verify on keyring engines through the
`legacy` key. But it cannot verify anything the new key signed. The rotation
is complete only when no engine without the keyring release remains.
Downgrading an engine is not supported.

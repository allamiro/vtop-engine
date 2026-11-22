# Secret files and their permissions

The engine reads its secrets from environment variables or from files, never
from `config.yaml` itself. A file is the usual way an external secret manager
hands a secret over: a Kubernetes Secret volume, a Vault Agent template, the
Secrets Store CSI driver, a Docker secret. This page says which files the
engine checks before it reads them, what it refuses, and how to mount each
kind of secret so that it starts.

## What is checked

| File | Named by | Holds |
|---|---|---|
| Manifest MAC key files | `manifest.mac.keys[].file` | A key that signs or verifies manifests. |
| The state-store secret file | `engine.state_store: { file: ... }` | The connection string; for PostgreSQL it carries the password. |
| The SQLite ledger, and its `-wal` and `-shm` files | `engine.state_store: "sqlite://..."` | The ledger: which batches were verified and committed. |
| The audit file | `VTOP_AUDIT_LOG` | The audit trail ([Auditing](auditing.md)). |
| TLS private keys of `vtop-node` | `tls.key`, `replica_tls.key`, `native_tls.key`, `lease.tls.key`, `observability.tls.key` in a node config | The node's identity on every plane that uses it. |

Each is checked by the process that reads it, before it is read: `vtopctl run`
and `process-once`, and every `vtopctl` command that opens the ledger or reads
the keys (`replay`, `migrate`, `prune-ledger`, `scrub`, `audit-coverage`,
`status`, `list-batches`, `discover`, `verify-manifest`, `audit list` and
`audit verify`). `vtopctl validate` runs the same check on every file at once
(below).

`vtop-node` checks its TLS private keys when it loads them, at startup,
before reading them (#732). It reads no engine config, so the override below
does not apply to it: a refused key must be fixed. The Helm chart mounts the
node's Secrets at `0440` with `fsGroup`, which is accepted.

Not checked yet: the other files `vtop-node` reads (its own
`VTOP_AUDIT_LOG`), the engine's `VTOP_METRICS_TLS_KEY`, and `vtopctl`'s
`client_key`.

The checks need Unix permission bits. On other platforms nothing is checked
and every file is accepted.

## The rule

The engine looks at the file's mode as it sees it, after following symlinks.

| Mode | Result |
|---|---|
| Writable by group or other (`0620`, `0602`, `0666`, ...) | **Refused. No setting accepts it.** Another local user could replace a key, point the engine at another database, or rewrite the ledger or the audit trail. |
| Readable by other (`0644`, `0604`, ...) | Refused, unless the directory holding the file is closed (below). |
| Readable by group, not other (`0640`, `0440`) | Refused, unless the directory is closed, or the file is a read-only Kubernetes secret mount (below). The audit file may always be group-readable: a log shipper reading the trail through a shared group is a normal setup, and the file holds no secret. |
| Owner only (`0600`, `0400`) | Accepted. |

**A closed directory** is one the engine's own user owns, with no group or
other permission bits (`0700`). Nobody else can reach a file inside it, so the
file's own read bits expose nothing. This is why a SQLite ledger created at
`0644` before this check existed, inside a `0700` data directory, keeps
starting. Write bits are refused even there.

**A Kubernetes secret mount** is recognised when all of these hold, and then
only the group read bit is accepted:

- the path leads, through symlinks, into the kubelet's layout for a Secret
  volume: a `..data` symlink in the mount directory naming the
  `..<timestamp>` directory that holds the file;
- the file is on `tmpfs` (Linux);
- the engine cannot write the file.

A path or a name alone is never enough.

## When a file is refused

The engine does not start, and does not read the file. The message names
what holds the file, the path, its mode and the fix:

```text
error: configuration error: manifest key "q4-2026": /etc/vtop/keys/q4 has mode 0644,
which lets its group and every other user on this host read the manifest signing key.
Refused: the file was not read and nothing was started. Run `chmod 0400 /etc/vtop/keys/q4`,
or close its directory to other users (`chmod 0700` on a directory the engine's user owns);
in Kubernetes mount the Secret with `defaultMode: 0400` (the kubelet makes it 0440 when
the pod sets `fsGroup`, which is accepted). To accept the exposure knowingly, set
`security.allow_loose_secret_permissions: true`
```

A writable file gets `chmod go-w` and a reminder to check who owns the file
and its directory (`ls -ln`), because a writable secret may already have been
replaced. Use `0400` for files the engine only reads, and `0600` for the
ledger and the audit file, which it writes. No message contains a key, a
connection string or any other value read from a file.

A new SQLite ledger is created `0600`, and SQLite gives its `-wal` and `-shm`
files the same mode. A new audit file is created `0600`. The engine never
changes the mode of a file that already exists: it refuses, and you decide.

## Check before you restart: `vtopctl validate`

```bash
vtopctl validate --config /etc/vtop/config.yaml
```

`validate` checks every file the config names, plus the audit file when
`VTOP_AUDIT_LOG` is set in its environment, and reports every refused file in
one run, with a non-zero exit. It reads no file's contents and contacts
nothing. A SQLite ledger named through `env:` or `file:` is not known until
the secret is read, so it is checked when the engine opens it, not here. Run
`validate` as the engine's user, with the same environment, on the same host
(or in the same pod): the modes it checks are the ones that host shows, and a
closed directory counts only for the user who owns it, so the same config
checked as root or as another account can report a file the engine accepts.

## The override

```yaml
security:
  allow_loose_secret_permissions: true
```

This accepts the readable cases above that would otherwise be refused. It
never accepts a file that group or other can write. Each file it accepts is
announced once per process:

- a WARN naming the file, its mode and who can read it;
- a count on `vtop_secret_file_permission_overrides_total` (no labels);
- an audit record with action `security.override`, target the file, and
  `file_kind`, `mode` and `exposed_to` as parameters.

`vtopctl validate` prints the same files as warnings and records nothing.
`vtopctl validate --print-effective` shows the setting. Unknown keys in
`security` are refused, so a misspelt override fails at load.

Prefer fixing the mount. Use the override for a setup you cannot change yet,
and alert on the counter so it does not stay that way unnoticed.

`vtopctl meta`, `node` and `tier` read no engine config, so they apply the
default rule to the audit file with no override.

## Mounting secrets from an external manager

Each recipe puts the secret in a file and the file's path in
`config.yaml`. The secret itself never appears in the config.

### Kubernetes Secret

Mount the whole Secret as a volume with `defaultMode: 0400`, and run the pod
with an `fsGroup` the engine's user belongs to. The kubelet then makes the
files `0440`, owned by root and group `fsGroup`, which is how a non-root
engine reads them, and which the engine accepts as a secret mount.

```yaml
spec:
  securityContext:
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
  containers:
    - name: engine
      volumeMounts:
        - name: manifest-keys
          mountPath: /etc/vtop/keys
          readOnly: true
        - name: state-store
          mountPath: /etc/vtop/state-store
          readOnly: true
  volumes:
    - name: manifest-keys
      secret:
        secretName: vtop-manifest-keys   # keys q3 and q4
        defaultMode: 0400
    - name: state-store
      secret:
        secretName: vtop-state-store     # key url
        defaultMode: 0400
```

```yaml
engine:
  state_store: { file: /etc/vtop/state-store/url }
manifest:
  write_version: "0.4"
  mac:
    active: q4-2026
    keys:
      - { id: q3-2026, file: /etc/vtop/keys/q3, legacy: true }
      - { id: q4-2026, file: /etc/vtop/keys/q4 }
```

- Do not use `subPath`. A `subPath` mount is a single file outside the
  kubelet's layout, so a `0440` file there is refused, and it is never
  updated when the Secret changes. For the same reason keep each
  `items[].path` a plain file name, without a directory part.
- The Secret volume's default mode is `0644`. Left unset, every key is
  readable by every user in the container and is refused.

**The ledger and the audit file on a writable volume.** With `fsGroup` set,
the kubelet gives every file on a writable volume (a PersistentVolumeClaim,
an `emptyDir`) group read and write, and with the default
`fsGroupChangePolicy: Always` it does so at every mount. A ledger the engine
created `0600` is then `0660` at the next start, and a group-writable ledger
is refused with no override. Set `fsGroupChangePolicy: OnRootMismatch` (the
Helm chart's default for its own pods), which leaves the files alone once the
volume's root has the right owner and mode; if a ledger was already widened,
`chmod 0600` it and its `-wal` and `-shm` once.

```yaml
spec:
  securityContext:
    fsGroup: 10001
    fsGroupChangePolicy: OnRootMismatch
```

### External Secrets Operator

The operator syncs a secret from your manager into an ordinary Kubernetes
Secret. Mount that Secret as above.

### Secrets Store CSI driver

The driver mounts the secret through a CSI volume. Set the volume
`readOnly: true` and give the files an owner-only mode: where your provider
supports a per-object file permission, use `0400`, or `0440` with an `fsGroup`
the engine shares. A mode such as `0644` is refused. Where the provider
cannot set the mode, sync the secret to a Kubernetes Secret
(`secretObjects`) and mount that as above.

### Vault Agent

Render the secret with a template and an owner-only mode, and run the agent
as the engine's user so that the file it writes is the engine's:

```hcl
template {
  destination = "/vault/secrets/manifest-q4"
  contents    = "{{ with secret \"secret/data/vtop/manifest\" }}{{ .Data.data.q4 }}{{ end }}"
  perms       = "0400"
}
```

```yaml
manifest:
  write_version: "0.4"
  mac:
    active: q4-2026
    keys:
      - { id: q4-2026, file: /vault/secrets/manifest-q4 }
```

With the Kubernetes injector, the annotation
`vault.hashicorp.com/agent-run-as-same-user: "true"` runs the agent as the
engine's user. A `0440` file written by another user and shared through a
group is refused unless its directory is closed, since the agent's output
directory is not a kubelet Secret mount.

### Docker and Compose secrets

Docker mounts secrets under `/run/secrets/` at mode `0444` unless told
otherwise, which is refused. Give each secret an owner-only mode and the
engine's uid:

```yaml
services:
  engine:
    user: "10001:10001"
    secrets:
      - source: vtop_state_store
        target: vtop-state-store
        uid: "10001"
        gid: "10001"
        mode: 0400
secrets:
  vtop_state_store:
    file: ./secrets/state-store.url
```

```yaml
engine:
  state_store: { file: /run/secrets/vtop-state-store }
```

Where a secret comes from a `file:` and your Compose version mounts that file
as it is, without applying `uid`, `gid` and `mode`, the host file's own mode
and owner are what the engine sees: `chmod 0400` it and `chown` it to the uid
the engine runs as.

### A plain host

Keep secrets in a directory only the engine's user can enter, or give each
file mode `0400` and the engine's user as owner:

```bash
install -d -m 0700 -o vtop -g vtop /etc/vtop/secrets
install -m 0400 -o vtop -g vtop q4.hex /etc/vtop/secrets/manifest-q4
```

## Upgrading to the release with this check

The check refuses at startup what earlier releases started: a world-readable
key file, a state-store secret file other users can read, or a `0644` SQLite
ledger in a directory others can enter. Before you upgrade:

1. Run the new `vtopctl validate` against each engine's config, on its host or
   in its pod, with `VTOP_AUDIT_LOG` set as the engine has it.
2. Fix each file it names: `chmod 0400` for key and secret files, `chmod 0600`
   for the ledger, its `-wal` and `-shm` files, and the audit file, or
   `chmod 0700` on the directory holding them. On Kubernetes, set the Secret
   volume's `defaultMode: 0400` and the pod's `fsGroup`.
3. Only if a file cannot be fixed yet, set
   `security.allow_loose_secret_permissions: true`. An engine of an earlier
   release ignores the `security` section.

For `vtop-node` (#732): before upgrading a node, make each TLS private key its
config names owner-only (`chmod 0400`), or `chmod 0700` the directory holding
it. A key generated with `openssl` under the usual umask is `0644`, which the
new release refuses. There is no override. Fix every node's keys before the
rolling upgrade reaches it, or that node will not start.

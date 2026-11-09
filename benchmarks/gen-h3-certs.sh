#!/usr/bin/env bash
# Mint the lab TLS material the benchmark HTTP/3 proxy serves (#484): one CA
# and one leaf, ECDSA P-256, SANs covering every name the proxy is reached by.
#
# WHY THERE IS A CERTIFICATE HERE AT ALL. HTTP/3 has no plaintext form, so the
# lab's h3 target cannot be an http:// listener the way toxiproxy is — the wire
# is TLS or it does not exist. And the engine's `verify_tls: false` does NOT
# relax certificate verification: vtop-upload/src/s3_native.rs says in as many
# words that the flag only permits plaintext endpoints and that "a self-signed
# or private-CA endpoint needs its CA in the system trust store". So the CA
# minted here is not decoration; it is the thing the client must trust, and
# lib/engine.py points SSL_CERT_FILE at it for exactly the scenarios that
# declare the proxy hop.
#
# WHY THREE NAMES. The same proxy is reached as `localhost:9443` from a host
# run, as `127.0.0.1:9443` by anything that resolves loopback numerically, and
# as `h3-proxy:9443` from inside the compose network (mc, a containerized
# sender). A certificate valid for only one of them turns a topology change
# into a handshake failure that reads like a transport result.
#
# Style follows scripts/live-chaos/gen-certs.sh: idempotent, quiet openssl,
# no CSR left behind, one machine-readable line at the end. Generated material
# is git-ignored (benchmarks/.gitignore) and never committed — it is a 30-day
# self-signed lab CA whose only purpose is letting a QUIC handshake complete
# against a store nobody else can reach.
#
# Usage: gen-h3-certs.sh     (writes benchmarks/tls; takes no arguments)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ONE place for the material, and no argument to move it (review). The script
# used to take an output directory, and minted there perfectly well — for
# nobody: the proxy's mounts, verify-h3.sh and the runner's CA path all read
# benchmarks/tls, so a custom directory produced a "ready" line for
# certificates no part of the lab could use. Refused rather than ignored, so a
# caller who passed one learns where the material actually goes.
if [[ $# -gt 0 ]]; then
  echo "gen-h3-certs: takes no arguments (got: $*). The material always goes to $SCRIPT_DIR/tls,
the one directory the h3 proxy's mounts, verify-h3.sh and the benchmark runner read." >&2
  exit 2
fi
CERT_DIR="$SCRIPT_DIR/tls"

# ONE notion of what compose will read (review), shared with verify-h3.sh: an
# exported value outranks .env, .env outranks the compose file's own default.
# Sourced before the cd below, while a relative path still means what it says.
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib/dotenv.sh
source "$SCRIPT_DIR/lib/dotenv.sh" || {
  echo "gen-h3-certs: cannot read $SCRIPT_DIR/lib/dotenv.sh — it is how this script works out
the identity compose will actually start the proxy with. Run the script from a full checkout." >&2
  exit 1
}

# The leaf's basename is the proxy's compose service name, so a directory
# listing says which service the material belongs to.
LEAF="h3-proxy"
DAYS=30
# The compose file's own fallback, from
# `user: "${VTOP_BENCH_UID:-1000}:${VTOP_BENCH_GID:-1000}"`. A pair that is set
# NOWHERE resolves to this, and so does a pair exported blank — so it is what
# the checks below have to compare against in either case. Pinned against drift
# by tests/test_compose_h3.py, which asserts that expression verbatim.
COMPOSE_DEFAULT_ID=1000
# Reminted with two days to spare rather than on the day it dies: a lab that
# only discovers an expired leaf mid-handshake reports it as a transport
# failure, which is the one reading this whole issue exists to prevent.
RENEW_WITHIN_SECONDS=$((2 * 24 * 3600))

# A leaf that is absent, unreadable, or about to expire takes the CA with it:
# reminting the leaf under an expired CA produces material that fails exactly
# as loudly and half an hour later.
#
# Judged by path, BEFORE the directory is created or entered: whether this run
# will mint decides whether it may run at all (below), and a refusal that had
# already made the directory would leave behind exactly the root-owned residue
# it refuses to work with.
needs_mint=0
reason="present and valid"
if [[ ! -f "$CERT_DIR/ca.pem" || ! -f "$CERT_DIR/$LEAF.pem" || ! -f "$CERT_DIR/$LEAF-key.pem" ]]; then
  needs_mint=1
  reason="material missing"
elif ! openssl x509 -in "$CERT_DIR/ca.pem" -checkend "$RENEW_WITHIN_SECONDS" -noout >/dev/null 2>&1; then
  needs_mint=1
  reason="CA expired or expiring within two days"
elif ! openssl x509 -in "$CERT_DIR/$LEAF.pem" -checkend "$RENEW_WITHIN_SECONDS" -noout >/dev/null 2>&1; then
  needs_mint=1
  reason="leaf expired or expiring within two days"
fi

# ROOT DOES NOT MINT (review). Material this run mints belongs to whoever runs
# it, and root is the one owner the proxy cannot run as — the owner check
# further down says why. That check used to be the only one, and it ran AFTER
# minting: a clean invocation as root created a root-owned directory and
# root-owned keys, then refused. The recovery it suggested did not recover.
# `sudo -u <user>` re-ran over that material, found it valid, reused it and hit
# the same refusal; renewing it later meant removing files from a directory the
# user could not write. So the identity is decided up front, from the effective
# uid, and a root run that would mint refuses having touched nothing — not the
# directory, not an expiring set it would have deleted to remint, not the .env.
#
# Only a run that MINTS is judged here. Reused material is judged by the key's
# owner below, so a root operator re-running over material an unprivileged user
# minted remains a supported no-op.
if [[ $needs_mint -eq 1 && "$(id -u)" -eq 0 ]]; then
  cat >&2 <<EOF
gen-h3-certs: refusing to mint as root (uid 0): $reason in $CERT_DIR.
Anything minted now would belong to root, and root is the one identity the h3 proxy cannot run
as: its container drops every capability, and a root nginx master chowns its temp paths before
it listens — a chown that needs CAP_CHOWN, which cap_drop: [ALL] has just removed.

Nothing was created, removed or written. Run this as the unprivileged user that will run the lab:
  sudo -u <user> $SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")
EOF
  exit 1
fi

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

if [[ $needs_mint -eq 1 ]]; then
  rm -f ca.pem ca-key.pem ca.srl "$LEAF.pem" "$LEAF-key.pem" "$LEAF.csr" leaf.ext
  cat > leaf.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, DNS:h3-proxy, DNS:vtop-bench-h3-proxy, IP:127.0.0.1, IP:0:0:0:0:0:0:0:1
EOF
  openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem 2>/dev/null
  openssl req -x509 -new -key ca-key.pem -sha256 -days "$DAYS" \
    -subj "/CN=vtop-bench-h3-ca" -out ca.pem 2>/dev/null
  openssl ecparam -name prime256v1 -genkey -noout -out "$LEAF-key.pem" 2>/dev/null
  openssl req -new -key "$LEAF-key.pem" -subj "/CN=localhost" -out "$LEAF.csr" 2>/dev/null
  openssl x509 -req -in "$LEAF.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -days "$DAYS" -sha256 -extfile leaf.ext -out "$LEAF.pem" 2>/dev/null
  rm -f "$LEAF.csr" leaf.ext ca.srl
  # The private keys are readable by their owner only: a lab key with a 0644
  # mode is a habit that travels somewhere it should not. The proxy container
  # therefore has to RUN as that owner, which is what the .env below arranges.
  chmod 600 ca-key.pem "$LEAF-key.pem"
  chmod 644 ca.pem "$LEAF.pem"
fi

# THE CONTAINER MUST BE THE KEY'S OWNER (review). The key is 0600 and owned by
# whoever minted it; the proxy service defaults to uid 1000, so on any host
# where that is not the owner, nginx starts and dies on "permission denied"
# reading its own key — and the documented setup would simply not work.
# Compose auto-loads a .env from the directory a -f file lives in (see the
# compose header), so writing the ids there makes the default correct without
# asking the operator to remember two exports.
#
# The ids come from the KEY, not from `id -u`. Reusing material somebody else
# minted is a supported outcome of the block above — this script reads only the
# public halves, which are 0644 — and the identity the proxy needs in that case
# is the owner's, not that of whoever happened to re-run this.
# GNU spells the owner `stat -c`, BSD/macOS `stat -f`; the lab is Linux but the
# material gets minted wherever the operator is.
#
# Captured into a variable first and only then split. `read a b <<< "$(...)"`
# takes its exit status from `read`, which succeeds on an empty here-string —
# so a stat that failed would leave both ids empty, write `VTOP_BENCH_UID=` to
# the .env and hand the proxy an identity of nothing, with `set -e` none the
# wiser. The assignment below does carry the failure, and the shape check after
# it is what refuses a value that is not an id at all.
key_owner="$(stat -c '%u %g' "$LEAF-key.pem" 2>/dev/null || stat -f '%u %g' "$LEAF-key.pem")"
read -r key_uid key_gid <<< "$key_owner"
if [[ ! "$key_uid" =~ ^[0-9]+$ || ! "$key_gid" =~ ^[0-9]+$ ]]; then
  echo "gen-h3-certs: could not read the owner of $PWD/$LEAF-key.pem — stat said '$key_owner'.
The proxy container runs as that identity to be able to read a 0600 key, and a guess would
produce a container that cannot." >&2
  exit 1
fi

# AND ROOT IS NOT AN IDENTITY THIS PROXY CAN RUN AS (review). Minting inside a
# development or CI container is commonly done as uid 0, and the material then
# belongs to root — which passes the shape check above and would be filed as
# `VTOP_BENCH_UID=0`, an identity that is not merely unusual here but known
# broken: nginx's master, when it is root, chowns its temp paths to the `nginx`
# user before it listens, that chown needs CAP_CHOWN, and the service drops
# every capability (tests/test_compose_h3.py pins both halves). So a root
# proxy dies at startup, having never read the key, and the failure names a
# cache directory rather than this file.
#
# This run cannot have minted root-owned material — the refusal above stops a
# root mint before it starts — so material reaching here owned by root was left
# by an earlier run or copied in, and is being REUSED. The recovery therefore
# hands over the whole directory, not only the two keys: a later renewal removes
# and remints every file in it, which needs the directory itself writable.
#
# Refused rather than written, on the rule the whole block below follows: an id
# that cannot work is not a preference. The GID is deliberately not judged the
# same way — a process with gid 0 and a non-zero uid is not root and starts
# fine — and the KEY's owner is what is judged, not `id -u`, so a root operator
# re-running over material an unprivileged user minted is still a supported
# no-op.
if [[ "$key_uid" -eq 0 ]]; then
  cat >&2 <<EOF
gen-h3-certs: $PWD/$LEAF-key.pem is owned by root (uid 0), and root is the one identity the
h3 proxy cannot run as. Its container drops every capability, and a root nginx master chowns
its temp paths before it listens — a chown that needs CAP_CHOWN, which cap_drop: [ALL] has
just removed. It would die at startup complaining about /var/cache/nginx, having never
reached this certificate.

Nothing was written to $SCRIPT_DIR/.env: VTOP_BENCH_UID=0 is an identity that cannot work,
and filing it would move the failure to a place that does not name it. Hand the material to
the unprivileged user that will run the lab — the whole directory, so a later renewal can
replace the files in it — and re-run:
  chown -R <uid>:<gid> $PWD
— or remove it and mint afresh as that user:
  rm -rf $PWD && sudo -u <user> $SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")
EOF
  exit 1
fi

# SCRIPT_DIR, not $0: the script has already cd'd into the cert directory by
# here, so a relative path would land the file beside the keys instead of
# beside the compose file that reads it.
env_file="$SCRIPT_DIR/.env"

# An id that is already there is LEFT there: an operator who set these
# deliberately has a reason, and silently overwriting their value would be the
# more surprising failure of the two.
#
# But an id that CANNOT work is not a preference (review) — it is a leftover
# from another user or another checkout, and preserving it makes the proxy run
# as a stranger to the key this invocation just minted. So the two are checked
# against the material rather than assumed to describe it, and a disagreement
# is refused with both halves named. Refused, not rewritten: which of the two
# is wrong is the operator's call, and this script only knows one of them.
#
# CHECKED AGAINST WHAT COMPOSE WILL RESOLVE, not against the file alone
# (review). An exported VTOP_BENCH_UID outranks $env_file, so validating the
# file and reporting success described an identity the stack would not use —
# the exact silent-disagreement failure this check exists to prevent, one layer
# up. A value exported blank is the same story in reverse: it masks the filed
# line and leaves compose on its ${VTOP_BENCH_UID:-1000} default, so it is
# compared as that default rather than waved through as "nothing set". Only a
# pair set NOWHERE is left uncompared — the loop below files the owner's ids,
# which is what makes it right.
#
# What is judged is the resolution IN THIS SHELL, the one that will run
# `compose up`. A wrong line in $env_file hidden behind a correct export is
# therefore not reported: it is invisible to compose as well, right up until
# somebody starts the profile from a terminal without the export — and running
# this script from that terminal is what names the line.
stale=""
for pair in "VTOP_BENCH_UID=$key_uid" "VTOP_BENCH_GID=$key_gid"; do
  key="${pair%%=*}"
  owner="${pair#*=}"
  origin="$(compose_value_origin "$env_file" "$key")"
  [[ "$origin" == "unset" ]] && continue
  raw="$(compose_raw_value "$env_file" "$key")"
  have="${raw:-$COMPOSE_DEFAULT_ID}"
  [[ "$have" == "$owner" ]] && continue
  case "$origin" in
    shell) where="exported in this shell, which OUTRANKS $env_file" ;;
    *)     where="filed in $env_file" ;;
  esac
  if [[ -z "$raw" ]]; then
    where="$where as an empty value, so compose falls back to its \${$key:-$COMPOSE_DEFAULT_ID} default"
  fi
  stale+="  $key=$have   ($where; the key's owner says $owner)"$'\n'
done
if [[ -n "$stale" ]]; then
  cat >&2 <<EOF
gen-h3-certs: the identity compose will start the h3 proxy with is not this material's:
$stale
The h3 proxy container runs as exactly that pair, and $PWD/$LEAF-key.pem is mode 0600 —
readable by its owner and by nobody else. A uid that is not the owner's makes nginx start
and then die on "permission denied" reading its own key, which reads as broken material
rather than as a stale value; a gid that is not the owner's means the pair was written for
different material, and the uid is the next thing to go.

This script never overwrites an id an operator set on purpose. Correct the value, or remove
it and re-run to have the owner's written. A value reported as exported cannot be corrected
in $env_file — compose reads the shell first — so unset it in the shell that will run
\`docker compose ... --profile h3 up\`, or export the owner's.
EOF
  exit 1
fi

for pair in "VTOP_BENCH_UID=$key_uid" "VTOP_BENCH_GID=$key_gid"; do
  key="${pair%%=*}"
  if [[ -f "$env_file" ]] && grep -q "^${key}=" "$env_file"; then
    continue
  fi
  # A FINAL NEWLINE FIRST, if the file lacks one (review). Appending to a file
  # whose last line is unterminated concatenates onto that line — the previous
  # value silently absorbs `VTOP_BENCH_UID=...`, compose then falls back to
  # 1000, and on a host that is not uid 1000 the proxy cannot read the key it
  # was just handed. An editor that trims trailing newlines is enough to cause
  # it, which is why it is worth a line of shell rather than an assumption.
  if [[ -s "$env_file" ]] && [[ -n "$(tail -c 1 "$env_file")" ]]; then
    echo "" >> "$env_file"
  fi
  echo "$pair" >> "$env_file"
  echo "h3_certs_env added=$pair file=$env_file reason=\"the proxy must run as the key's owner\""
done

# Both what happened and why, on one line: a re-run that silently reused
# material is indistinguishable from one that reminted it, and "why" is the
# half that explains a surprise remint the morning a lab CA aged out. `owner`
# rides along because the check above is otherwise invisible on the path where
# it passes, and "the ids agreed with the key" is exactly what a reader chasing
# a proxy that will not start needs to know was established.
action=$([[ $needs_mint -eq 1 ]] && echo minted || echo reused)
echo "h3_certs_ready dir=$PWD leaf=$LEAF.pem ca=ca.pem owner=$key_uid:$key_gid action=$action reason=\"$reason\""

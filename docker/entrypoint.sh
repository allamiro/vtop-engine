#!/usr/bin/env bash
set -euo pipefail

# Entrypoint for the VTOP Engine container.
#
# Credentials are expected to be supplied via environment variables (e.g.
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) or mounted secrets. This script
# never prints them.

CONFIG="${VTOP_CONFIG:-/app/examples/config.yaml}"

# Ensure the working / state directories exist (mounted as volumes in compose).
# These are usually pre-created by the volume mounts; tolerate a root-owned
# mount where the unprivileged `vtop` user cannot create them (don't abort).
for d in /data/input /data/spool /data/work /data/state; do
  mkdir -p "$d" 2>/dev/null || true
done
# The engine must be able to write work_dir and the state DB; fail clearly if not.
for d in /data/work /data/state; do
  if [ ! -w "$d" ]; then
    echo "vtop-engine: $d is not writable by uid $(id -u); fix the volume ownership" >&2
    exit 1
  fi
done

# Default action is "run"; any other vtopctl subcommand can be passed through.
ACTION="${1:-run}"
shift || true

echo "vtop-engine: starting action '${ACTION}' with config ${CONFIG}"
exec vtopctl "${ACTION}" --config "${CONFIG}" "$@"

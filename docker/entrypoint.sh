#!/usr/bin/env bash
set -euo pipefail

# Entrypoint for the VTOP Engine container.
#
# Credentials are expected to be supplied via environment variables (e.g.
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) or mounted secrets. This script
# never prints them.

CONFIG="${VTOP_CONFIG:-/app/examples/config.yaml}"

# Ensure the working / state directories exist (mounted as volumes in compose).
mkdir -p /data/input /data/spool /data/work /data/state

# Default action is "run"; any other vtopctl subcommand can be passed through.
ACTION="${1:-run}"
shift || true

echo "vtop-engine: starting action '${ACTION}' with config ${CONFIG}"
exec vtopctl "${ACTION}" --config "${CONFIG}" "$@"

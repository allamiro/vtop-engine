#!/bin/sh
# Entrypoint of the soak's engine container (#726). The clock_step fault
# chooses a wall-clock shim by writing its path to /soak/ctl/ld_preload before
# it starts the container; with no such file the engine runs on the real clock.
# The shim records the time it loaded at in /soak/ctl/skew-loaded, which is how
# the driver knows the step was applied.
set -eu
preload=/soak/ctl/ld_preload
rm -f /soak/ctl/skew-loaded 2>/dev/null || true
if [ -s "$preload" ]; then
  LD_PRELOAD="$(cat "$preload")"
  export LD_PRELOAD
  export VTOP_SKEW_LOADED_MARKER=/soak/ctl/skew-loaded
  echo "soak-entrypoint: loading clock shim $LD_PRELOAD" >&2
fi
exec /usr/local/bin/entrypoint.sh "$@"

#!/bin/sh
# Entrypoint of the soak's engine container (#726). The clock_step fault
# chooses a wall-clock shim by writing its path to /soak/ctl/ld_preload before
# it starts the container; with no such file the engine runs on the real clock.
# The shim records the time it loaded at in /soak/ctl/skew-loaded, which is how
# the driver knows the step was applied.
#
# The shim is given to vtopctl alone (review of #881). Exported here it would
# also reach the shell of docker/entrypoint.sh, whose own load of the shim
# writes the marker, so an engine that cannot load it (a static build) would
# still look stepped. A vtopctl first on PATH execs the real one with the shim
# in its environment only; docker/entrypoint.sh runs unchanged.
set -eu
preload=/soak/ctl/ld_preload
rm -f /soak/ctl/skew-loaded 2>/dev/null || true
if [ -s "$preload" ]; then
  shim="$(cat "$preload")"
  engine="$(command -v vtopctl)"
  wrapped="$(mktemp -d)"
  cat > "$wrapped/vtopctl" <<WRAPPER
#!/bin/sh
exec env LD_PRELOAD="$shim" VTOP_SKEW_LOADED_MARKER=/soak/ctl/skew-loaded "$engine" "\$@"
WRAPPER
  chmod 755 "$wrapped/vtopctl"
  PATH="$wrapped:$PATH"
  export PATH
  echo "soak-entrypoint: vtopctl will load clock shim $shim" >&2
fi
exec /usr/local/bin/entrypoint.sh "$@"

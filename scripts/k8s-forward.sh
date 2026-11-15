# shellcheck shell=bash
#
# Port-forwards for scripts/k8s-smoke.sh, sourced by it and by the bats suite
# in scripts/tests/k8s-forward.bats.
#
# A forward is ready when its LOCAL port accepts a connection, and only then
# (#786). These helpers used to start `kubectl port-forward` in the background
# and sleep a fixed 4 s. On a slow runner the forward was not listening yet, the
# single `curl` that followed exited 7 ("couldn't connect"), and the helm job
# failed while the deployment under test was fine. So the wait is now on the
# condition itself: poll the port until it accepts, bounded by
# FORWARD_WAIT_SECONDS, and start the forward again if kubectl exited.
#
# The caller supplies:
#   WORK      a directory for the forwards' own output, kept for the failure
#             message instead of being thrown away to /dev/null;
#   FORWARDS  a space-separated list of pids, which the caller's cleanup kills.
#             The pid of the forward now serving the port is always the LAST
#             entry, so `${FORWARDS##* }` names it.

FORWARD_WAIT_SECONDS="${FORWARD_WAIT_SECONDS:-30}"

# True once something accepts a TCP connection on 127.0.0.1:<port>. Bounded,
# so a listener that accepts the SYN and then never completes cannot hang the
# probe.
forward_port_open() { # localport
  timeout 2 bash -c ": </dev/tcp/127.0.0.1/$1" 2>/dev/null
}

forward_start() { # namespace pod localport remoteport log
  kubectl -n "$1" port-forward "pod/$2" "$3:$4" >>"$5" 2>&1 &
  FORWARDS="${FORWARDS:-} $!"
  # Job control off for this shell, so tearing the forwards down at exit does
  # not print a "Terminated" line per child over the test's own output.
  disown 2>/dev/null || true
}

# Starts a forward and waits until its local port accepts. Returns 1, after
# printing what happened, what was done and what to check, when the port is not
# accepting within FORWARD_WAIT_SECONDS. Returns rather than exits, because two
# callers sit inside retry loops where a forward to a restarting pod failing is
# an expected outcome of one attempt, not of the run.
forward_open() { # namespace pod localport remoteport
  local log="$WORK/port-forward-$3.log"
  local deadline=$((SECONDS + FORWARD_WAIT_SECONDS)) starts=1 pid
  : >"$log"
  forward_start "$1" "$2" "$3" "$4" "$log"
  while :; do
    forward_port_open "$3" && return 0
    pid="${FORWARDS##* }"
    if ! kill -0 "$pid" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; then
      # kubectl exited (the pod is not there yet, or the API stream dropped):
      # a forward that is gone will never start listening, so start another.
      starts=$((starts + 1))
      forward_start "$1" "$2" "$3" "$4" "$log"
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      kill "${FORWARDS##* }" 2>/dev/null || true
      {
        printf '[k8s-smoke] the port-forward to pod/%s port %s in namespace %s was not accepting on localhost:%s after %ss (%s start(s)).\n' \
          "$2" "$4" "$1" "$3" "$FORWARD_WAIT_SECONDS" "$starts"
        printf '[k8s-smoke] the forward was stopped. Check that the pod is Running and that nothing else holds port %s. kubectl port-forward said:\n' "$3"
        sed 's/^/[k8s-smoke]   /' "$log"
      } >&2
      return 1
    fi
    sleep 0.5
  done
}

# A forward that OUTLIVES its pod (#515). `kubectl port-forward` binds to a
# pod, not a Service, and exits for good when that pod is deleted — which the
# per-attempt forwards in k8s-smoke.sh handle by re-forwarding on every
# attempt, on a fresh port. A forward named in a CONFIG FILE cannot do that:
# the port is baked into the file, so the only way to keep that endpoint
# meaning "this pod" across a restart is to re-attach on the same port whenever
# kubectl exits. The one-second pause is not a settle: it keeps a pod that does
# not exist yet from turning the loop into a busy spin of API calls.
#
# The subshell's TERM trap takes the current kubectl down with it, so the
# cleanup's `kill` on the recorded pid still leaves no port-forward behind.
# The supervisor restarts kubectl itself, so the readiness wait here only
# polls the port. A timeout is a WARNING, not a failure: the supervisor keeps
# re-attaching, and every use of these ports sits in a bounded retry loop that
# fails with its own message if the pod never answers. The fixed sleep this
# replaces never failed here either.
forward_supervised_open() { # namespace pod localport remoteport
  local log="$WORK/port-forward-$3.log"
  local deadline=$((SECONDS + FORWARD_WAIT_SECONDS))
  : >"$log"
  (
    child=""
    trap 'kill "$child" 2>/dev/null; exit 0' TERM
    while :; do
      kubectl -n "$1" port-forward "pod/$2" "$3:$4" >>"$log" 2>&1 &
      child=$!
      wait "$child" || true
      sleep 1
    done
  ) &
  FORWARDS="${FORWARDS:-} $!"
  disown 2>/dev/null || true
  until forward_port_open "$3"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      {
        printf '[k8s-smoke] the supervised port-forward to pod/%s port %s in namespace %s was not accepting on localhost:%s after %ss.\n' \
          "$2" "$4" "$1" "$3" "$FORWARD_WAIT_SECONDS"
        printf '[k8s-smoke] continuing: the supervisor keeps restarting it, and the retries that use this port fail if it never answers. If they do, check that the pod is Running and that nothing else holds port %s. kubectl port-forward said:\n' "$3"
        sed 's/^/[k8s-smoke]   /' "$log"
      } >&2
      return 0
    fi
    sleep 0.5
  done
}

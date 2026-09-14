#!/usr/bin/env bash
# Prove the lab's h3 proxy is really serving HTTP/3, and not quietly serving
# TCP under the name (#484).
#
# WHY THIS IS A SCRIPT AND NOT A NOTE. A silent downgrade is invisible in every
# number the benchmark produces: the scenarios still pass, the throughput still
# looks like throughput, and every later transport comparison becomes a
# comparison of TCP with itself. So the claim has to be re-checkable on demand,
# by anyone, after any change to the image, nginx-h3.conf or the compose file.
#
# WHAT IT CHECKS, and why each part is here.
#
#   1. THE CLIENT WITNESS. h3_probe.py offers only the `h3` ALPN and reports
#      the protocol the handshake actually agreed on. There is nothing for it
#      to fall back to: no QUIC listener means no connection at all.
#   2. THE SERVER WITNESS. The proxy's own access log records the protocol it
#      served. A client cannot make that field say HTTP/3.0 by believing hard
#      enough, so this is the witness that a downgrade cannot survive.
#   3. THE CONTROL THAT THE LOG DISCRIMINATES. The same URL is fetched over
#      TCP by ordinary curl, and the log line for THAT request must NOT say
#      HTTP/3.0. Without this, "the log said HTTP/3.0" could just mean the log
#      always says HTTP/3.0.
#   4. THE CONTROL THAT THE PROBE CAN FAIL. The same probe is aimed at a port
#      with no QUIC listener (MinIO's own, which answers on TCP), and it must
#      fail. Without this, a probe that rubber-stamped everything would pass
#      this script forever.
#   5. THE AUTHORITY, on both wires. An S3 client signs the authority —
#      host AND port — into its SigV4 signature, and the store recomputes that
#      signature over whatever Host the proxy forwards. nginx leaves
#      `$http_host` empty on HTTP/3, so the proxy reconstructs one; a
#      reconstruction that names the wrong port makes the store answer
#      SignatureDoesNotMatch, which reads as a credential bug and not as a
#      proxy one. The access log records what was forwarded, and it must equal
#      the authority this script dialled. The same number has to appear in
#      `Alt-Svc`, or a TCP client is told to retry HTTP/3 where nothing is
#      listening. Both were wrong under VTOP_H3_PORT until #484's review.
#
# Requires the material and the profile:
#   benchmarks/gen-h3-certs.sh
#   docker compose -f benchmarks/docker-compose.benchmark.yml --profile h3 up -d
#   benchmarks/verify-h3.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$BENCH_DIR/docker-compose.benchmark.yml"
CA_FILE="$BENCH_DIR/tls/ca.pem"
PROBE_FILE="$BENCH_DIR/h3_probe.py"

# ONE notion of what compose read (review), shared with gen-h3-certs.sh.
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib/dotenv.sh
source "$BENCH_DIR/lib/dotenv.sh" || {
  echo "verify-h3: cannot read $BENCH_DIR/lib/dotenv.sh — it is how this check works out which
port compose published the proxy on. Run the script from a full checkout." >&2
  exit 1
}

# The published loopback listener, not an in-network shortcut: the compose UDP
# publish is part of what is being verified, and reaching the container over the
# compose bridge instead would skip it.
H3_HOST="${VTOP_H3_HOST:-localhost}"
# THE PORT COMPOSE PUBLISHED ON, resolved the way compose resolved it (review).
# `${VTOP_H3_PORT:-9443}` reads the shell and nothing else, and compose
# auto-loads benchmarks/.env — so an operator who filed the override there,
# which is the durable way to state it, moved the proxy while this check went
# on probing 9443. It then reported the whole lab broken: no QUIC listener, no
# Alt-Svc, no log line. Every assertion below is made against this number —
# the probe, the curl control, the advertised port and the forwarded
# authority — so getting it from the same place compose did is what makes them
# assertions about the proxy rather than about the port.
#
# VTOP_H3_HOST above is NOT resolved this way, and the difference is the point:
# compose never interpolates it, so a value filed in .env would reach nothing
# but this script and the two would be describing different things.
H3_PORT="$(compose_value "$BENCH_DIR/.env" VTOP_H3_PORT 9443)"
# The no-QUIC control target: MinIO's own published port, which answers on TCP
# and has no UDP listener at all.
CONTROL_PORT="${VTOP_H3_CONTROL_PORT:-9000}"
# Anything the origin answers 200 to and the proxy forwards unauthenticated.
# A health path rather than a bucket operation on purpose: this script is
# checking the WIRE, and a signature failure would be noise in that answer.
PROBE_PATH="/minio/health/live"

# PINNED, both of them (#507): an official base image at an explicit tag, plus
# an explicit aioquic version. A floating tag here would let the one check that
# guards against a silent downgrade change underneath the claim it supports.
PROBE_IMAGE="${VTOP_H3_PROBE_IMAGE:-python:3.13-slim}"
AIOQUIC_VERSION="${VTOP_H3_AIOQUIC_VERSION:-1.3.0}"

# How long to wait for the proxy's access log to carry a request that has already
# been answered. The log is written after the response, and `docker compose
# logs` reads it through the daemon, so a small lag is normal and a missing
# line after this long is a real absence.
LOG_WAIT_SECONDS="${VTOP_H3_LOG_WAIT_SECONDS:-15}"

die() { echo "verify-h3: $*" >&2; exit 1; }

compose() { docker compose -f "$COMPOSE_FILE" --profile h3 "$@"; }

# --- preconditions, each naming the exact thing to run ------------------------

[[ -f "$CA_FILE" ]] || die "the lab CA is missing at $CA_FILE — run $BENCH_DIR/gen-h3-certs.sh first"
[[ -f "$PROBE_FILE" ]] || die "the probe is missing at $PROBE_FILE"
command -v docker >/dev/null 2>&1 || die "docker is not on PATH; this check runs the proxy and the probe in containers"
command -v curl >/dev/null 2>&1 || die "curl is not on PATH; it is the TCP control this check compares against"

running="$(compose ps --status running --services 2>/dev/null || true)"
grep -qx "h3-proxy" <<< "$running" || die "the h3-proxy service is not running (running: ${running//$'\n'/, }). Start it:
  docker compose -f $COMPOSE_FILE --profile h3 up -d"

# Two tokens, and NEITHER is a prefix of the other: the log is searched by
# substring, and a token that contained the other's would match both requests
# and let the TCP line answer for the HTTP/3 one.
run_token="$(od -An -tx1 -N4 /dev/urandom | tr -d ' \n')"
h3_token="${run_token}h3"
tcp_token="${run_token}tcp"

# --- 1. the client witness ----------------------------------------------------

run_probe() { # run_probe <url> <expect-status> <timeout>
  # --network host because the proxy is published on LOOPBACK by design, which
  # nothing on the docker bridge can reach — and the published listener is
  # precisely what this check is about. The probe container is otherwise held
  # to the lab's baseline; it cannot be read-only because pip writes the wheel
  # into it, and it is --rm, so it exists for the length of one request.
  #
  # The probe arrives on STDIN rather than through a bind mount, and the reason
  # is the one the lab's mc service already ran into: the container is root
  # with CAP_DAC_OVERRIDE dropped, so a source file sitting at 0600 under a
  # developer's umask is simply unreadable to it, and the check then fails for
  # a reason that has nothing to do with HTTP/3. `cat`-ing it in runs as the
  # file's owner. The CA still has to be a real path (aioquic loads it by
  # filename), which is why gen-h3-certs.sh mints it world-readable.
  docker run --rm -i --network host \
    --cap-drop ALL --security-opt no-new-privileges:true \
    -v "$CA_FILE:/probe/ca.pem:ro" \
    "$PROBE_IMAGE" \
    sh -c "pip install --no-cache-dir --quiet --disable-pip-version-check \
                --root-user-action=ignore 'aioquic==$AIOQUIC_VERSION' >/dev/null && \
           exec python - --url '$1' --ca /probe/ca.pem \
                --expect-status '$2' --timeout '$3'" < "$PROBE_FILE"
}

h3_url="https://$H3_HOST:$H3_PORT$PROBE_PATH?probe=$h3_token"
echo "verify-h3: probing $h3_url over QUIC (aioquic $AIOQUIC_VERSION in $PROBE_IMAGE)"
probe_out=""
probe_status=0
# stderr is FOLDED IN, never discarded: the reason a handshake failed is the
# only useful thing this check can report when it fails.
probe_out="$(run_probe "$h3_url" 200 15 2>&1)" || probe_status=$?
echo "$probe_out"
if (( probe_status != 0 )); then
  die "the HTTP/3 probe failed (exit $probe_status); its own output is above. Either the proxy
is not serving HTTP/3, its UDP port is not published/reachable, or the probe could not run at
all. Check the listener:
  docker compose -f $COMPOSE_FILE --profile h3 ps
  docker compose -f $COMPOSE_FILE --profile h3 logs h3-proxy"
fi
grep -q "alpn=h3" <<< "$probe_out" || die "the probe returned success without reporting alpn=h3; it said: $probe_out"

# --- 3. the TCP control, which also gives the log something to discriminate ---

tcp_url="https://$H3_HOST:$H3_PORT$PROBE_PATH?probe=$tcp_token"
echo "verify-h3: fetching $tcp_url over TCP with curl, as the control"
curl_out=""
curl_status=0
# `-D -` because the response HEADERS are evidence too: Alt-Svc is the only way
# a client that arrived over TCP ever learns the HTTP/3 listener exists, and it
# names a port. The -w summary is appended after them, so it is the last line.
curl_out="$(curl -sS --cacert "$CA_FILE" -o /dev/null -D - \
  -w 'http_code=%{http_code} http_version=%{http_version}' "$tcp_url" 2>&1)" || curl_status=$?
curl_summary="$(tail -n 1 <<< "$curl_out")"
echo "verify-h3: curl says $curl_summary"
if (( curl_status != 0 )); then
  die "the TCP control could not reach the proxy. Without it this check cannot show
that the access log distinguishes HTTP/3 from anything else. curl said:
$curl_out"
fi
grep -q "http_code=200" <<< "$curl_summary" || die "the TCP control did not get a 200: $curl_summary"
# `if`, not `grep ... && die`: under `set -e` a trailing `&&` list whose left
# side fails takes the whole script down, and the left side failing is the
# NORMAL outcome here.
if grep -q "http_version=3" <<< "$curl_summary"; then
  die "curl reports HTTP/3 for the control request, so it is not a control: $curl_summary"
fi

# THE ADVERTISEMENT (part of 5). A TCP client discovers HTTP/3 here only
# through Alt-Svc, and the port it names is the one nginx was told it is
# PUBLISHED on — not the one it bound. Under VTOP_H3_PORT those differ, and a
# proxy advertising its container port sends every upgrading client to a number
# where nothing is listening; the client then reports HTTP/3 as unavailable,
# which is indistinguishable from the lab not having a counterparty at all.
alt_svc="$(grep -i '^alt-svc:' <<< "$curl_out" | tr -d '\r' | head -n 1)"
[[ -n "$alt_svc" ]] || die "the proxy answered the TCP control without an Alt-Svc header, so no client
arriving over TCP can discover the HTTP/3 listener at all. Response headers were:
$curl_out"
grep -q "h3=\":$H3_PORT\"" <<< "$alt_svc" || die "the proxy advertises '$alt_svc', but it is published on port $H3_PORT.
Every client that upgrades on that advertisement is sent where nothing is listening. The
advertised port follows VTOP_H3_PORT through VTOP_H3_ADVERTISED_PORT in the compose file."

# --- 2. the server witness ----------------------------------------------------

field_for() { # field_for <token> <field> -> the logged value, or empty
  compose logs --no-log-prefix h3-proxy 2>/dev/null \
    | grep -F "probe=$1" \
    | grep -o "\"$2\":\"[^\"]*\"" \
    | head -n 1 \
    | cut -d'"' -f4
}

proto_for() { # proto_for <token> -> the proto the proxy logged, or empty
  field_for "$1" proto
}

await_proto() { # await_proto <token> -> the proto, waiting for the line to land
  local token="$1" waited=0 found=""
  while (( waited < LOG_WAIT_SECONDS )); do
    found="$(proto_for "$token")"
    [[ -n "$found" ]] && { printf '%s' "$found"; return 0; }
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

h3_proto="$(await_proto "$h3_token")" || die "the proxy logged no request carrying probe=$h3_token within ${LOG_WAIT_SECONDS}s.
The probe reported success, so either the access log is not configured (nginx-h3.conf)
or the request reached a different server. Last log lines:
$(compose logs --tail 20 --no-log-prefix h3-proxy 2>&1)"
tcp_proto="$(await_proto "$tcp_token")" || die "the proxy logged no request carrying probe=$tcp_token within ${LOG_WAIT_SECONDS}s;
the control cannot be evaluated. Last log lines:
$(compose logs --tail 20 --no-log-prefix h3-proxy 2>&1)"

echo "verify-h3: the proxy logged proto=$h3_proto for the QUIC request and proto=$tcp_proto for the TCP one"
[[ "$h3_proto" == "HTTP/3.0" ]] || die "the proxy served the QUIC request as $h3_proto, not HTTP/3.0 — the proxy downgraded it"
[[ "$tcp_proto" != "HTTP/3.0" ]] || die "the proxy logged the plain-TCP control as HTTP/3.0 too, so the log field proves nothing;
this check cannot distinguish a downgrade and must not be trusted until that is explained"

# --- 5. the authority that reaches the store ----------------------------------
#
# The log's `host` field is the Host the proxy forwards upstream, and an S3
# client signs exactly that string — port included — into its SigV4 signature.
# Both requests above dialled "$H3_HOST:$H3_PORT", so both must arrive at the
# store as that; anything else and MinIO recomputes the signature over a
# different authority and answers SignatureDoesNotMatch, which reads as a
# credential problem rather than as the proxy problem it is.
#
# The HTTP/3 line is the one that has ever been wrong: nginx leaves $http_host
# empty on HTTP/3, so that Host is reconstructed rather than forwarded, and the
# reconstruction named the container's port until #484's review. The TCP line
# is checked too — it is what proves this assertion can tell the two apart
# rather than passing because every line happens to say the same thing.
expected_authority="$H3_HOST:$H3_PORT"
h3_host="$(field_for "$h3_token" host)"
tcp_host="$(field_for "$tcp_token" host)"
for pair in "HTTP/3:$h3_host" "TCP:$tcp_host"; do
  wire="${pair%%:*}"
  seen="${pair#*:}"
  [[ "$seen" == "$expected_authority" ]] || die "the proxy forwarded Host: '$seen' for the $wire request, but the client dialled
$expected_authority and an S3 client signs that authority — port and all — into its SigV4
signature. The store recomputes it over what arrives and answers SignatureDoesNotMatch,
which reads as a credential failure. Check VTOP_H3_ADVERTISED_PORT in the compose file and
the \$authority_port map in nginx-h3.conf.template."
done
echo "verify-h3: the proxy forwarded Host: $expected_authority on both wires, which is what a client signs"

# --- 4. the control that the probe can fail -----------------------------------

control_url="https://$H3_HOST:$CONTROL_PORT$PROBE_PATH?probe=$run_token-noquic"
echo "verify-h3: aiming the same probe at $control_url, which has no QUIC listener; it must fail"
control_out=""
control_status=0
control_out="$(run_probe "$control_url" 200 8 2>&1)" || control_status=$?
echo "$control_out"
# The control must fail for THE RIGHT REASON (review). Accepting any non-zero
# exit would let a transient registry failure, a pip error or a container that
# never started stand in as proof that the probe can detect a missing QUIC
# listener — and the script would then print PASS on the strength of an
# unrelated breakage, which is the precise failure a negative control exists to
# rule out. h3_probe reserves exit 3 for "no HTTP/3 exchange"; nothing else is
# the control succeeding.
readonly PROBE_NO_EXCHANGE=3
if (( control_status == 0 )); then
  die "the probe REPORTED SUCCESS against a port with no QUIC listener, so it is not testing anything.
Either something unexpected is serving HTTP/3 on port $CONTROL_PORT, or the probe is broken."
fi
if (( control_status != PROBE_NO_EXCHANGE )); then
  die "the no-QUIC control exited $control_status, not $PROBE_NO_EXCHANGE (no HTTP/3 exchange).
It failed for some OTHER reason — a registry or pip failure, a container that never started —
so it proves nothing about the probe's ability to detect a missing QUIC listener, and this
run must not be read as a pass. Its output was:
$control_out"
fi
if ! grep -q "no HTTP/3 exchange" <<<"$control_out"; then
  die "the no-QUIC control exited $PROBE_NO_EXCHANGE but did not report a missing HTTP/3 exchange.
The exit code and the diagnosis disagree, so one of them is wrong and neither can be trusted:
$control_out"
fi

cat <<EOF
verify-h3: PASS
  http/3 negotiated : client ALPN h3, the proxy logged $h3_proto  (probe=$h3_token)
  not vacuous       : the same log field read $tcp_proto for the curl control (probe=$tcp_token)
  probe can fail    : it exited $control_status (no HTTP/3 exchange) against port $CONTROL_PORT, which serves no QUIC
  signed authority  : Host: $h3_host forwarded on HTTP/3 and $tcp_host on TCP, which is what an S3 client signs
  advertised        : $alt_svc, the port a TCP client is told to retry HTTP/3 on
  target            : https://$H3_HOST:$H3_PORT -> minio:9000
EOF

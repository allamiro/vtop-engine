#!/usr/bin/env bash
# Randomized telemetry event generator for the VTOP lab.
#
# Emits randomized events of a chosen format to stdout so you can pipe them into
# Kafka, a file, or a syslog spool and exercise VTOP's per-batch format
# detection with realistic, varied data.
#
# Usage:
#   seed-events.sh <cef|leef|json|jsonl|syslog|logfmt|apache|text|mixed> [count]
#
# Examples:
#   seed-events.sh cef 100 | kafka-console-producer.sh --topic cef_events ...
#   seed-events.sh json 200 > data/input/app.json.log
#   seed-events.sh mixed 500 > data/input/mixed.log   # random format per line
set -euo pipefail

FORMAT="${1:-cef}"
COUNT="${2:-50}"

VENDORS=(VTOP Acme Globex Initech Umbrella)
PRODUCTS=(Engine Gateway Sensor Proxy Firewall)
EVENTS=(login logout file_access firewall_deny privilege_escalation malware_detected port_scan dns_query config_change)
USERS=(alice bob carol dave eve mallory root svc-account)
ACTIONS=(allow deny block quarantine login sudo read write delete)
OUTCOMES=(success failure unknown)
SEVERITIES=(1 2 3 4 5 6 7 8 9 10)
PROTOS=(TCP UDP ICMP HTTP HTTPS DNS)
HOSTS=(host01 host02 fw01 dc01 web03 db02)

pick() { local n=$#; eval "echo \${$(( (RANDOM % n) + 1 ))}"; }
rand_ip() { echo "$(( RANDOM % 223 + 1 )).$(( RANDOM % 256 )).$(( RANDOM % 256 )).$(( RANDOM % 256 ))"; }
rand_hex() { head -c 20 /dev/urandom 2>/dev/null | od -An -tx1 | tr -d ' \n' || echo "$RANDOM$RANDOM$RANDOM"; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

emit_cef() {
  local sig=$(( RANDOM % 900 + 100 ))
  printf 'CEF:0|%s|%s|1.%d|%d|%s|%s|src=%s dst=%s spt=%d dpt=%d suser=%s act=%s proto=%s outcome=%s fileHash=%s rt=%s\n' \
    "$(pick "${VENDORS[@]}")" "$(pick "${PRODUCTS[@]}")" "$(( RANDOM % 9 ))" "$sig" \
    "$(pick "${EVENTS[@]}")" "$(pick "${SEVERITIES[@]}")" \
    "$(rand_ip)" "$(rand_ip)" "$(( RANDOM % 65535 ))" "$(( RANDOM % 1024 ))" \
    "$(pick "${USERS[@]}")" "$(pick "${ACTIONS[@]}")" "$(pick "${PROTOS[@]}")" \
    "$(pick "${OUTCOMES[@]}")" "$(rand_hex)" "$(now)"
}

emit_json() {
  printf '{"ts":"%s","event":"%s","user":"%s","src":"%s","dst":"%s","port":%d,"action":"%s","severity":%s,"outcome":"%s","bytes":%d}\n' \
    "$(now)" "$(pick "${EVENTS[@]}")" "$(pick "${USERS[@]}")" \
    "$(rand_ip)" "$(rand_ip)" "$(( RANDOM % 65535 ))" "$(pick "${ACTIONS[@]}")" \
    "$(pick "${SEVERITIES[@]}")" "$(pick "${OUTCOMES[@]}")" "$(( RANDOM % 1000000 ))"
}

emit_syslog() {
  local pri=$(( RANDOM % 191 + 1 ))
  printf '<%d>1 %s %s %s %d - - event=%s user=%s src=%s action=%s outcome=%s\n' \
    "$pri" "$(now)" "$(pick "${HOSTS[@]}")" "$(pick "${PRODUCTS[@]}")" "$(( RANDOM % 9999 ))" \
    "$(pick "${EVENTS[@]}")" "$(pick "${USERS[@]}")" "$(rand_ip)" \
    "$(pick "${ACTIONS[@]}")" "$(pick "${OUTCOMES[@]}")"
}

emit_leef() {
  # LEEF 1.0, often syslog-framed (tab-delimited extensions).
  local pri=$(( RANDOM % 191 + 1 ))
  printf '<%d>%s %s LEEF:1.0|%s|%s|1.0|%s|src=%s\tdst=%s\tusrName=%s\tsev=%s\tcat=%s\toutcome=%s\n' \
    "$pri" "$(now)" "$(pick "${HOSTS[@]}")" "$(pick "${VENDORS[@]}")" "$(pick "${PRODUCTS[@]}")" \
    "$(pick "${EVENTS[@]}")" "$(rand_ip)" "$(rand_ip)" "$(pick "${USERS[@]}")" \
    "$(pick "${SEVERITIES[@]}")" "$(pick "${EVENTS[@]}")" "$(pick "${OUTCOMES[@]}")"
}

emit_logfmt() {
  # key=value (logfmt) — detected as text/raw, useful for varied structure.
  printf 'ts=%s level=%s event=%s user=%s src=%s action=%s outcome=%s duration_ms=%d\n' \
    "$(now)" "$(pick info warn error debug)" "$(pick "${EVENTS[@]}")" "$(pick "${USERS[@]}")" \
    "$(rand_ip)" "$(pick "${ACTIONS[@]}")" "$(pick "${OUTCOMES[@]}")" "$(( RANDOM % 5000 ))"
}

emit_apache() {
  # Apache/NGINX common log format.
  local methods=(GET POST PUT DELETE HEAD) codes=(200 201 301 304 400 401 403 404 500 502)
  printf '%s - %s [%s] "%s /path/%d HTTP/1.1" %s %d "-" "Mozilla/5.0"\n' \
    "$(rand_ip)" "$(pick "${USERS[@]}")" "$(now)" \
    "${methods[$((RANDOM % ${#methods[@]}))]}" "$(( RANDOM % 9999 ))" \
    "${codes[$((RANDOM % ${#codes[@]}))]}" "$(( RANDOM % 100000 ))"
}

emit_text() {
  # Free-form text log line.
  printf '%s [%s] %s by %s from %s -> %s\n' \
    "$(now)" "$(pick INFO WARN ERROR)" "$(pick "${EVENTS[@]}")" \
    "$(pick "${USERS[@]}")" "$(rand_ip)" "$(pick "${OUTCOMES[@]}")"
}

for _ in $(seq 1 "$COUNT"); do
  case "$FORMAT" in
    cef) emit_cef ;;
    leef) emit_leef ;;
    json | jsonl) emit_json ;;
    syslog) emit_syslog ;;
    logfmt) emit_logfmt ;;
    apache) emit_apache ;;
    text) emit_text ;;
    mixed)
      case $(( RANDOM % 7 )) in
        0) emit_cef ;;
        1) emit_leef ;;
        2) emit_json ;;
        3) emit_syslog ;;
        4) emit_logfmt ;;
        5) emit_apache ;;
        6) emit_text ;;
      esac
      ;;
    *)
      echo "unknown format: $FORMAT (use cef|leef|json|jsonl|syslog|logfmt|apache|text|mixed)" >&2
      exit 2
      ;;
  esac
done

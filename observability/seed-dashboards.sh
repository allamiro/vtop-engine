#!/bin/sh
# Seed the VTOP dashboards into Grafana through the API.
#
# NOT file-provisioning: Grafana 13 makes file-provisioned dashboards read-only
# in the UI (Save is refused), and this lab is meant to be poked at. Seeded
# dashboards are ordinary, fully editable ones.
#
# Idempotent: re-running re-applies the repo version (discarding local edits),
# which is exactly how you reset the lab after experimenting.
set -eu

GRAFANA_URL="${GRAFANA_URL:-http://grafana:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
DASHBOARD_DIR="${DASHBOARD_DIR:-/dashboards}"
FOLDER="${GRAFANA_FOLDER:-VTOP}"

echo "[seed] waiting for Grafana at $GRAFANA_URL"
i=0
until curl -sf -o /dev/null "$GRAFANA_URL/api/health"; do
  i=$((i + 1))
  [ "$i" -gt 60 ] && { echo "[seed] Grafana did not become healthy"; exit 1; }
  sleep 2
done

# Create the folder (ignore "already exists").
curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" -H "Content-Type: application/json" \
  -X POST "$GRAFANA_URL/api/folders" \
  -d "{\"title\":\"$FOLDER\"}" >/dev/null 2>&1 || true
FOLDER_UID=$(curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" "$GRAFANA_URL/api/folders" \
  | sed -n 's/.*"uid":"\([^"]*\)","title":"'"$FOLDER"'".*/\1/p' | head -1)

count=0
for f in "$DASHBOARD_DIR"/*.json; do
  [ -e "$f" ] || continue
  # Wrap the dashboard, drop any id so Grafana assigns its own, and overwrite.
  payload=$(sed 's/^/  /' "$f" | awk -v folder="${FOLDER_UID:-}" '
    BEGIN { print "{\"overwrite\":true,\"folderUid\":\"" folder "\",\"dashboard\":" }
    { print }
    END { print "}" }')
  if echo "$payload" | curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
       -H "Content-Type: application/json" \
       -X POST "$GRAFANA_URL/api/dashboards/db" -d @- >/dev/null; then
    count=$((count + 1))
    echo "[seed] applied $(basename "$f")"
  else
    echo "[seed] FAILED $(basename "$f")"; exit 1
  fi
done
echo "[seed] seeded $count dashboards into folder '$FOLDER' (editable + saveable)"

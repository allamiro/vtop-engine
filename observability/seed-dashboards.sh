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

# Create the folder with a KNOWN uid so there is nothing to parse back out.
# (Scraping the uid out of /api/folders with sed is brittle - field order is not
# guaranteed - and an empty result silently drops every dashboard into the root
# "General" folder, which is exactly what happened before.)
FOLDER_UID="${GRAFANA_FOLDER_UID:-vtop}"
curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" -H "Content-Type: application/json" \
  -X POST "$GRAFANA_URL/api/folders" \
  -d "{\"uid\":\"$FOLDER_UID\",\"title\":\"$FOLDER\"}" >/dev/null 2>&1 || true

# Fail loudly if the folder is not there: seeding into the wrong place is worse
# than not seeding, because the dashboards look "missing" in the UI.
if ! curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
     -o /dev/null "$GRAFANA_URL/api/folders/$FOLDER_UID"; then
  echo "[seed] could not create or find folder uid=$FOLDER_UID"; exit 1
fi

count=0
for f in "$DASHBOARD_DIR"/*.json; do
  [ -e "$f" ] || continue
  # Wrap the dashboard, drop any id so Grafana assigns its own, and overwrite.
  payload=$(sed 's/^/  /' "$f" | awk -v folder="$FOLDER_UID" '
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

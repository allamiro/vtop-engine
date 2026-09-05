#!/bin/sh
# Seed the VTOP dashboards into Grafana through the API.
#
# NOT file-provisioning: Grafana 13 makes file-provisioned dashboards read-only
# in the UI (Save is refused), and this lab is meant to be poked at. Seeded
# dashboards are ordinary, fully editable ones.
#
# NON-DESTRUCTIVE by default. `docker compose up -d` STARTS an exited service,
# so this seeder re-runs on every bring-up of the lab. If it blindly overwrote,
# a dashboard you edited and saved would be silently reverted the next time you
# ran the documented start command - which would defeat the entire point of
# seeding editable dashboards. So an already-present dashboard is left alone.
#
# To deliberately reset the lab to the repo's dashboards, set FORCE_RESEED:
#   docker compose -f docker-compose.yml -f docker-compose.observability.yml \
#       run --rm -e FORCE_RESEED=true grafana-seed
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

# Both provisioning paths must agree on ONE folder (#454). Grafana's alert
# provisioner creates the folder named in provisioning/alerting/vtop-alerts.yaml
# by TITLE, with a uid of its own choosing, and it runs at boot - before this
# seeder has seen Grafana healthy - so on a fresh volume the folder is already
# there under a uid that is not ours. (The rule group has no folderUid to pin
# it: Grafana 13.2 ignores the key. Tested.) Creating "vtop" then would make a
# SECOND folder with the same title - Grafana 13 allows it - with the rule in
# one VTOP and the dashboards in another; an older Grafana refuses the duplicate
# title and this seeder would abort. So: adopt the folder that carries our title
# if one exists, and create it with the known uid only when none does. In the
# other order the provisioner finds ours by title and uses it, so both orders
# converge on one folder.
#
# One folder per line before matching, so the uid read back belongs to the same
# object as the title matched, whatever order Grafana emits fields in. An empty
# match falls through to creating the known uid, and either way the folder is
# read back by uid below before anything is seeded into it.
FOLDER_UID=$(curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" "$GRAFANA_URL/api/folders?limit=1000" 2>/dev/null \
  | tr '{' '\n' | grep -F "\"title\":\"$FOLDER\"" | head -n 1 \
  | sed -n 's/.*"uid":"\([^"]*\)".*/\1/p' || true)
if [ -n "$FOLDER_UID" ]; then
  echo "[seed] adopting existing folder '$FOLDER' (uid=$FOLDER_UID)"
else
  FOLDER_UID="${GRAFANA_FOLDER_UID:-vtop}"
  curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" -H "Content-Type: application/json" \
    -X POST "$GRAFANA_URL/api/folders" \
    -d "{\"uid\":\"$FOLDER_UID\",\"title\":\"$FOLDER\"}" >/dev/null 2>&1 || true
fi

# Fail loudly if the folder is not there: seeding into the wrong place is worse
# than not seeding, because the dashboards look "missing" in the UI.
if ! curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
     -o /dev/null "$GRAFANA_URL/api/folders/$FOLDER_UID"; then
  echo "[seed] could not create or find folder uid=$FOLDER_UID"; exit 1
fi

FORCE_RESEED="${FORCE_RESEED:-false}"

count=0
skipped=0
for f in "$DASHBOARD_DIR"/*.json; do
  [ -e "$f" ] || continue

  # Preserve local edits unless explicitly told not to. The uid is stable and
  # generated into each dashboard, so presence is a reliable existence check.
  uid=$(sed -n 's/.*"uid"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$f" | head -n 1)
  if [ "$FORCE_RESEED" != "true" ] && [ -n "$uid" ] && \
     curl -sf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
       -o /dev/null "$GRAFANA_URL/api/dashboards/uid/$uid"; then
    skipped=$((skipped + 1))
    echo "[seed] kept existing $(basename "$f") (uid=$uid; FORCE_RESEED=true to reset)"
    continue
  fi

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
echo "[seed] applied $count, kept $skipped existing, folder '$FOLDER' (editable + saveable)"

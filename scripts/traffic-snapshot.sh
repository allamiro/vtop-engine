#!/usr/bin/env bash
# Capture GitHub's repository traffic before it expires (#analytics).
#
# GitHub keeps traffic for FOURTEEN DAYS and then drops it. Every question
# worth asking about where a project's attention comes from — is that referrer
# growing, did the write-up move anything, which docs do people actually open —
# is a question about a period longer than fourteen days, so the answer has to
# be accumulated while it exists. That is the whole job here: fetch, merge, and
# keep.
#
# Usage:
#   scripts/traffic-snapshot.sh [output-dir]      # default: analytics/traffic
#
# Requires: gh (authenticated), jq.
#
# TOKEN, and why it cannot be the workflow's default one: the traffic endpoints
# require the repository's *Administration: read* permission, which a workflow
# GITHUB_TOKEN cannot be granted — the `permissions:` block has no such scope.
# A fine-grained PAT with that permission, stored as a secret, is the only way
# a scheduled run can read this. The wrapper workflow says so on failure rather
# than letting it surface as an opaque 403.
set -euo pipefail

OUT_DIR="${1:-analytics/traffic}"
REPO="${TRAFFIC_REPO:-allamiro/vtop-engine}"

log()  { printf '[traffic] %s\n' "$*"; }
fail() { printf '[traffic] FAIL: %s\n' "$*" >&2; exit 1; }

command -v gh >/dev/null || fail "gh is not installed"
command -v jq >/dev/null || fail "jq is not installed"

mkdir -p "$OUT_DIR"
DAILY="$OUT_DIR/daily.json"
SNAPSHOTS="$OUT_DIR/snapshots.json"
SUMMARY="$OUT_DIR/README.md"

api() { # <path> — a traffic endpoint, or a clear failure
  local path="$1" body
  if ! body="$(gh api "repos/$REPO/$path" 2>&1)"; then
    case "$body" in
      *"Must have push access"* | *403* | *404*)
        fail "the token cannot read $path. Traffic endpoints need the repository's \
Administration: read permission — a workflow GITHUB_TOKEN cannot be granted it, so a \
fine-grained PAT must be supplied (see the header of this script)."
        ;;
      *) fail "reading $path: $body" ;;
    esac
  fi
  printf '%s' "$body"
}

log "reading traffic for $REPO"
VIEWS="$(api traffic/views)"
CLONES="$(api traffic/clones)"
REFERRERS="$(api traffic/popular/referrers)"
PATHS="$(api traffic/popular/paths)"

# ---------------------------------------------------------------------------
# Daily series: MERGED BY DATE, never appended.
#
# The API returns the same day's totals on every run of that day, and those
# totals climb as the day goes on. Appending would count a day once per run and
# invent traffic; keying by date and letting the newest reading win keeps a
# re-run — or three — idempotent, and keeps the last reading of a day, which is
# the complete one.
# ---------------------------------------------------------------------------
[[ -f "$DAILY" ]] || echo '{"views":{},"clones":{}}' > "$DAILY"

# READ FROM THE FILE, never expanded onto the command line. `--argjson
# "$(cat …)"` puts the entire accumulated history into one argv entry, and argv
# is capped (~128 KiB per argument on the runners) — so this would work for
# months and then fail with "Argument list too long" exactly when the history
# it protects had become worth something. `--slurpfile` hands jq a path and
# wraps the parsed document in an array, hence the `[0]`.
#
# The failure is also made FATAL rather than left to `&&`: a `jq … > tmp && mv`
# that loses its jq silently skips the move, and the collector would go on
# logging success while recording nothing (review).
if ! jq -n \
  --slurpfile existing "$DAILY" \
  --argjson views "$VIEWS" \
  --argjson clones "$CLONES" '
  def series(items): reduce items[] as $d ({};
    .[$d.timestamp | split("T")[0]] = { count: $d.count, uniques: $d.uniques });
  ($existing[0] // {views:{},clones:{}}) as $prior
  | {
      views:  (($prior.views  // {}) + series($views.views)),
      clones: (($prior.clones // {}) + series($clones.clones))
    }
' > "$DAILY.tmp"; then
  rm -f "$DAILY.tmp"
  fail "merging the daily series failed; the accumulated history is left untouched"
fi
mv "$DAILY.tmp" "$DAILY"

# ---------------------------------------------------------------------------
# Referrers and paths are a ROLLING 14-DAY AGGREGATE with no per-day breakdown,
# so they cannot be merged the way the series can — there is no key to merge
# on. They are stored as dated snapshots instead: each run appends one, and the
# trend is read across snapshots rather than within one. Same-day re-runs
# replace that day's snapshot, for the same reason the series is keyed.
# ---------------------------------------------------------------------------
[[ -f "$SNAPSHOTS" ]] || echo '{}' > "$SNAPSHOTS"
TODAY="$(date -u +%Y-%m-%d)"

# Same treatment, and this is the file that actually grows: one referrer and
# path snapshot per day, forever.
if ! jq -n \
  --slurpfile existing "$SNAPSHOTS" \
  --arg today "$TODAY" \
  --argjson referrers "$REFERRERS" \
  --argjson paths "$PATHS" \
  --argjson views "$VIEWS" \
  --argjson clones "$CLONES" '
  ($existing[0] // {}) + { ($today): {
    window_views:  { count: $views.count,  uniques: $views.uniques  },
    window_clones: { count: $clones.count, uniques: $clones.uniques },
    referrers: $referrers,
    paths: $paths
  }}
' > "$SNAPSHOTS.tmp"; then
  rm -f "$SNAPSHOTS.tmp"
  fail "merging the snapshot history failed; the accumulated history is left untouched"
fi
mv "$SNAPSHOTS.tmp" "$SNAPSHOTS"

# ---------------------------------------------------------------------------
# A rendered summary, because a JSON file nobody opens is not a report. Uniques
# lead: a view count is inflated by anything that polls, while uniques is the
# number that answers "how many people".
# ---------------------------------------------------------------------------
# One definition, used by every table below. Pipes are escaped, backticks
# become apostrophes so a code span cannot be broken out of, and any newline
# collapses to a space so one value cannot become two rows.
MD_ESCAPE='def md: tostring | gsub("\\|"; "\\|") | gsub("`"; "\u0027") | gsub("[\r\n]+"; " ");'

{
  echo "# Traffic"
  echo
  echo "Accumulated from GitHub's traffic API, which keeps only the last 14 days."
  echo "Generated by \`scripts/traffic-snapshot.sh\`; last run ${TODAY} (UTC)."
  echo
  echo "## Where it comes from (current 14-day window)"
  echo
  echo "| Referrer | Views | Uniques |"
  echo "|---|--:|--:|"
  # ESCAPED, because these strings come from the internet. A referrer or path
  # containing a pipe ends the table cell early, a backtick breaks the code
  # span, and a newline ends the row — so a report about where traffic comes
  # from could be malformed by the traffic it reports on (review).
  jq -r "$MD_ESCAPE"'
    .[] | "| \(.referrer|md) | \(.count) | \(.uniques) |"' <<< "$REFERRERS"
  echo
  echo "## What people open (current 14-day window)"
  echo
  echo "| Path | Views | Uniques |"
  echo "|---|--:|--:|"
  # Bounded INSIDE jq, not with `head`: piping into head lets head close the
  # pipe early and SIGPIPE the writer, which under `pipefail` fails a report
  # that was in fact produced — the same trap that took scenario 08 down.
  jq -r "$MD_ESCAPE"'
    .[:15][] | "| `\(.path|md)` | \(.count) | \(.uniques) |"' <<< "$PATHS"
  echo
  echo "## Daily history (most recent 60 days)"
  echo
  echo "The part GitHub does not keep. This table renders the latest 60 days;"
  echo "\`daily.json\` beside it holds every day the collector has ever seen."
  echo
  echo "| Date | Views | Unique visitors | Clones | Unique cloners |"
  echo "|---|--:|--:|--:|--:|"
  # The root is bound FIRST. After `.[] as $d` the input is the date array, so
  # `.views[$d]` would look the series up on a string and error out — which is
  # exactly how this failed the first time it ran.
  jq -r '
    . as $root
    | [ ($root.views | keys), ($root.clones | keys) ] | flatten | unique | reverse
    | limit(60; .[]) as $d
    | "| \($d) | \($root.views[$d].count // 0) | \($root.views[$d].uniques // 0) | \($root.clones[$d].count // 0) | \($root.clones[$d].uniques // 0) |"
  ' "$DAILY"
} > "$SUMMARY"

DAYS="$(jq -r '[(.views|keys),(.clones|keys)]|flatten|unique|length' "$DAILY")"
SNAPS="$(jq -r 'keys|length' "$SNAPSHOTS")"
log "history now covers $DAYS day(s) across $SNAPS snapshot(s) -> $OUT_DIR"

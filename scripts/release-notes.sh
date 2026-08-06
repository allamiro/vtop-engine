#!/usr/bin/env bash
#
# Generate the changelog section of a release body.
#
# Emits markdown on stdout: one entry per merged pull request in the tag range,
# grouped by the component prefix the commit subject already carries, plus the
# issues those pull requests closed.
#
# WHY THE API AND NOT THE COMMIT TEXT. Squash-merge subjects look like
# "broker: subject (#240) (#258)", where the trailing number is the pull
# request and an earlier one may be the issue — but only sometimes: a change
# with no issue has one number, and a change closing two issues has three.
# Parsing that positionally guesses, and a changelog that silently mislabels an
# issue as a pull request is worse than none. The API knows which is which.
#
# Usage: release-notes.sh <tag> [previous-tag]
#
# With no previous tag, the nearest preceding tag is used; if there is none —
# the first release — the range starts at the root commit.

set -euo pipefail

fail() {
    printf 'release-notes: %s\n' "$*" >&2
    exit 1
}

tag=${1:-}
[ -n "$tag" ] || fail "usage: release-notes.sh <tag> [previous-tag]"

repo=${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}

previous=${2:-}
if [ -z "$previous" ]; then
    # `git describe` on the tag's parent finds the tag before it. A first
    # release has no such tag, which is not an error.
    previous=$(git describe --tags --abbrev=0 "${tag}^" 2>/dev/null || true)
fi

if [ -n "$previous" ]; then
    range="${previous}..${tag}"
else
    range="$tag"
fi

# Every pull request that contributed a commit to the range. The
# commits/{sha}/pulls endpoint is authoritative about which PR a squashed
# commit came from; sort -u because a non-squashed merge contributes several.
pulls=$(
    git log --format=%H "$range" | while read -r sha; do
        gh api "repos/${repo}/commits/${sha}/pulls" \
            --jq '.[] | select(.merged_at != null) | .number' 2>/dev/null || true
    done | sort -un
)

if [ -z "$pulls" ]; then
    printf '_No merged pull requests in this range._\n'
    exit 0
fi

# Collect once: title, component prefix, and the issues each PR closes.
entries=$(mktemp)
issues=$(mktemp)
trap 'rm -f "$entries" "$issues"' EXIT

for number in $pulls; do
    json=$(gh pr view "$number" --repo "$repo" \
        --json title,closingIssuesReferences 2>/dev/null || true)
    [ -n "$json" ] || continue

    title=$(printf '%s' "$json" | jq -r '.title')
    # "broker,node: subject" -> "broker,node". A subject with no prefix groups
    # under "other" rather than being dropped.
    if printf '%s' "$title" | grep -q '^[a-z0-9,_-]\+:'; then
        component=${title%%:*}
        subject=${title#*: }
    else
        component="other"
        subject="$title"
    fi
    closes=$(printf '%s' "$json" | jq -r '.closingIssuesReferences[]?.number' | sort -un)
    link=""
    if [ -n "$closes" ]; then
        trailer_edited=0
        for issue in $closes; do
            link="${link}, [#${issue}](https://github.com/${repo}/issues/${issue})"
            printf '%s\n' "$issue" >>"$issues"
            # A subject ending in the issue it closes repeats what the "closes"
            # link is about to say. Drop only that one — see below for why the
            # others stay.
            before=$subject
            subject=$(printf '%s' "$subject" | sed "s/ *(#${issue})\$//")
            # Also drop it from a compound trailer like "(#240, closes #261)",
            # which this project's subjects use. Leaving the number bare there
            # made it the one unlinked reference in the whole changelog.
            subject=$(printf '%s' "$subject" |
                sed "s/, *closes *#${issue})/)/; s/(closes *#${issue}, */(/")
            [ "$subject" = "$before" ] || trailer_edited=1
        done
        link=" — closes ${link#, }"
        # Tidy ONLY a trailer this loop actually edited. Running it
        # unconditionally would rewrite legitimate titles — one ending in
        # `foo()`, or containing a trailing-comma expression — for a cleanup
        # that had nothing to do with them.
        if [ "$trailer_edited" = 1 ]; then
            subject=$(printf '%s' "$subject" | sed 's/ *()$//; s/(, /(/; s/, )/)/')
        fi
    fi
    # A reference the PR does NOT close is a different fact — "relates to",
    # "partially addresses" — and dropping it would lose it. Link every one
    # that survives, including inside a compound trailer: a bare "#240" beside
    # linked ones reads as an oversight.
    #
    # Anchored to a STANDALONE reference — start of line, a space, or an
    # opening paren before the `#`. Matching a bare `#digits` anywhere would
    # rewrite the fragment in a URL (`.../page#123`) and nest a link inside one
    # a title already carried. Two passes rather than a BRE alternation, which
    # is a GNU extension BSD sed does not accept.
    url="https://github.com/${repo}/issues"
    subject=$(printf '%s' "$subject" |
        sed "s|\([ (]\)#\([0-9][0-9]*\)|\1[#\2](${url}/\2)|g" |
        sed "s|^#\([0-9][0-9]*\)|[#\1](${url}/\1)|")

    printf '%s\t- %s ([#%s](https://github.com/%s/pull/%s))%s\n' \
        "$component" "$subject" "$number" "$repo" "$number" "$link" >>"$entries"
done

printf '### What changed\n\n'
cut -f1 "$entries" | sort -u | while read -r component; do
    printf '**%s**\n' "$component"
    grep "^${component}$(printf '\t')" "$entries" | cut -f2-
    printf '\n'
done

if [ -s "$issues" ]; then
    printf '### Issues closed\n\n'
    sort -un "$issues" | while read -r issue; do
        title=$(gh issue view "$issue" --repo "$repo" --json title -q .title 2>/dev/null || true)
        [ -n "$title" ] || continue
        printf -- '- [#%s](https://github.com/%s/issues/%s) — %s\n' \
            "$issue" "$repo" "$issue" "$title"
    done
    printf '\n'
fi

if [ -n "$previous" ]; then
    printf '**Full changelog:** [%s...%s](https://github.com/%s/compare/%s...%s)\n' \
        "$previous" "$tag" "$repo" "$previous" "$tag"
fi

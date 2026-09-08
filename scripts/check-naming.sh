#!/usr/bin/env bash
# The naming rule, enforced mechanically (#490).
#
# The product's own description of itself must not carry a commercial
# transport product's mark: not in a crate name, module name, feature flag,
# config key, CLI flag, metric label, transport variant, scenario name or
# document. Those are source-identifying uses, not references — and internal
# shorthand is in scope too, because internal shorthand leaks into READMEs.
# Prior-art analysis is the one legitimate home for those names, so exactly
# two documents are exempt, by explicit file path and never by directory: a
# new document under docs/ is scanned by default.
#
# The term list lives in scripts/naming-denylist.txt so adding a term is a
# one-line change and removing one is visible in review. That file and this
# script are the checker's own machinery: the list is skipped because it is
# where the terms are defined, and this script must simply never contain one.
#
# Usage: check-naming.sh [ROOT]   (default: the working directory)
set -euo pipefail

root="${1:-.}"
denylist="$root/scripts/naming-denylist.txt"

if [[ ! -f "$denylist" ]]; then
    echo "naming check: term list not found at $denylist" >&2
    exit 2
fi

# The scanned surface: every path the rule names, plus the top-level Rust
# source trees and deployment configuration the first cuts forgot (review,
# twice) — tests/ and fuzz/ hold named modules and scenarios exactly as
# crates/ does, and the compose files and .env.example are where config
# keys live, which the rule names explicitly. A gate with an unscanned
# surface is a gate with a documented way around it. Paths that do not
# exist under ROOT are skipped so the check runs unchanged in the test's
# miniature tree.
scanned=(crates helm examples benchmarks observability docker scripts
    tests fuzz README.md Cargo.toml docs
    docker-compose.yml docker-compose.observability.yml .env.example
    CONTRIBUTING.md COMMERCIAL.md LICENSE-EE
    .github .cargo deny.toml)

# The ONLY documents that may name commercial marks, because naming them is
# their job — two files, by exact path, never a directory — plus the
# checker's own term list, which is where the terms are defined.
is_exempt() {
    local file="$1" allowed
    for allowed in "docs/PRIOR_ART_SEARCH_PLAN.md" \
        "docs/INVENTION_DISCLOSURE_DRAFT.md" \
        "scripts/naming-denylist.txt"; do
        [[ "$file" == "$root/$allowed" ]] && return 0
    done
    return 1
}

# A content hit is exempt only when the exempt path is followed by grep's
# own :line: seam (review): parsing the filename out of "path:line:text"
# with a prefix cut wrongly exempted any file whose NAME extends an exempt
# path past a colon, e.g. "PRIOR_ART_SEARCH_PLAN.md:copy".
content_hit_is_exempt() {
    local line="$1" allowed
    for allowed in "docs/PRIOR_ART_SEARCH_PLAN.md" \
        "docs/INVENTION_DISCLOSURE_DRAFT.md" \
        "scripts/naming-denylist.txt"; do
        # Anchor on the exact exempt path followed by grep's :line: seam,
                # so a filename that merely BEGINS with an exempt path (e.g.
                # PRIOR_ART_SEARCH_PLAN.md:1:copy) is not mistaken for the exempt
                # file at line 1 (review): the char after the path must be a
                # colon, then digits, then a colon.
                local prefix="$root/$allowed"
                [[ "$line" == "$prefix":[0-9]*:* ]] && {
                    # Confirm the middle segment is ALL digits, not a colon-bearing
                    # name whose first colon happened to precede a digit.
                    local rest="${line#"$prefix":}"
                    local lineno="${rest%%:*}"
                    [[ "$lineno" =~ ^[0-9]+$ ]] && return 0
                }
    done
    return 1
}

# FIXED STRINGS via -F -f, never a built-up regex (review): a term
# containing regex syntax would silently change what the scan means, and a
# pattern error swallowed by a `|| true` would read as a clean tree. Terms
# match case-insensitively as SUBSTRINGS — an identifier like `mark_shim`
# has no word boundary at the underscore, and identifiers are the uses the
# rule most cares about — so the term list carries the distinctiveness
# burden.
patterns="$(mktemp)"
content_list=""
# One EXIT trap covers every temp file on every exit path (review): the
# earlier code removed content_list only on its two normal paths, leaking
# it whenever the scan exited early.
trap 'rm -f "$patterns" "$content_list"' EXIT
# Carriage returns are stripped as the patterns are built (review, with a
# repro): a denylist committed with CRLF endings would otherwise hand grep
# terms ending in \r, and every mark would stop matching — the checker
# failing OPEN over a line-ending convention.
grep -Ev '^[[:space:]]*(#|$)' "$denylist" | tr -d '\r' > "$patterns" || true

if [[ ! -s "$patterns" ]]; then
    echo "naming check: the term list is empty, which would pass everything" >&2
    exit 2
fi

# CONTENT targets are real files and directories only; a symlink operand
# — even an explicitly-listed root like README.md or docs — is NEVER
# handed to grep -r, which would follow it and scan an exempt or
# out-of-scope target's bytes under the operand's clean name (review).
# Symlink operands are name-checked instead, with the root links.
content_targets=()
symlink_operands=()
for path in "${scanned[@]}"; do
    full="$root/$path"
    if [[ -L "$full" ]]; then
        symlink_operands+=("$full")
    elif [[ -e "$full" ]]; then
        content_targets+=("$full")
    fi
done
targets=("${content_targets[@]}")
# EVERY root-level regular file rides along generically (review, twice):
# the explicit list can only name documents that exist today, and a new
# top-level file would otherwise be born outside the gate — name and
# contents both. Enumerated with find rather than a glob, because the
# glob missed dotfiles and a .something.yml is exactly where a config
# key hides — and symlinks ride along too (review): a linked root config
# is still a named surface. Directories stay explicit; a new directory
# is a reviewable event, a new root file should not need to be.
# Regular root files feed the CONTENT scan; root symlinks feed the NAME
# scan only (review): a link's target is reached through some real path
# that is either already scanned or deliberately not, and following the
# link would scan an exempt or out-of-tree target's bytes under the
# link's alias. The link's own name is still a source-identifying
# surface, so it is name-checked.
root_files_status=0
root_regular=$(find "$root" -maxdepth 1 -type f) || root_files_status=$?
root_links=$(find "$root" -maxdepth 1 -type l) || root_files_status=$?
if (( root_files_status != 0 )); then
    echo "naming check: the root-file enumeration itself failed" >&2
    exit 2
fi
while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    already=0
    for known in "${targets[@]}"; do
        [[ "$path" == "$known" ]] && already=1 && break
    done
    [[ "$already" -eq 0 ]] && targets+=("$path")
done <<< "$root_regular"

# FAIL CLOSED on scan errors (review): grep exits 0 on hits, 1 on none, and
# anything else is a broken scan that must not read as a clean one.
content_status=0
# The content scan runs over REAL FILES enumerated by find (-type f, no
# -L, so links are never followed anywhere in the walk), not over
# directory operands grep -r would traverse while following any symlink
# it meets (review). A link's own NAME is judged by the path scan below.
content_hits=""
if (( ${#targets[@]} > 0 )); then
    # NUL-SAFE end to end (review): filenames can hold spaces or glob
    # metacharacters, so paths never pass through word-splitting or a
    # newline string. find -print0 fills a bash array; grep reads that
    # array as explicit operands. Exempt files are excluded from the
    # array, so their hits never appear (a colon-bearing filename made
    # output-parsing the exemption impossible).
    # find writes to a temp file so its EXIT STATUS is checkable (review):
    # in `while ...; done < <(find ...)` the process substitution swallows
    # find's status, so a failed enumeration read as an empty (clean) scan.
    content_list="$(mktemp)"
    if ! find "${targets[@]}" -type f -print0 > "$content_list"; then
        rm -f "$content_list"
        echo "naming check: the content-file enumeration itself failed" >&2
        exit 2
    fi
    content_files=()
    while IFS= read -r -d '' cf; do
        is_exempt "$cf" || content_files+=("$cf")
    done < "$content_list"
    rm -f "$content_list"
    if (( ${#content_files[@]} > 0 )); then
        content_hits=$(grep -HinF -f "$patterns" "${content_files[@]}") || content_status=$?
    fi
fi
if (( content_status >= 2 )); then
    echo "naming check: the content scan itself failed" >&2
    exit 2
fi

# PATHS TOO (review): a crate, scenario or script NAMED after a mark is one
# of the rule's primary surfaces, and a file whose body never repeats its
# own name would otherwise pass. Directories included — a module is one.
#
# find's status is captured SEPARATELY from grep's (review): in a pipe only
# grep's status survives, so a traversal error — an unreadable directory —
# would read as "no hits" and pass a tree the scan never finished walking.
# And the names judged are REPOSITORY-RELATIVE (review): find prints paths
# under $root verbatim, so a checkout that happens to live under a
# directory named after a mark would flag every entry in it for its
# location rather than its name.
find_status=0
# Names of everything the scan reaches, symlinks included (review): a
# link named after a mark is a surface even when its target is out of
# scope. -P (find's default) never follows links, so a link is reported
# as itself, not walked into. Root links are appended only when present,
# so an empty set does not hand find a blank path argument.
find_targets=("${targets[@]}" "${symlink_operands[@]}")
while IFS= read -r link; do
    [[ -n "$link" ]] && find_targets+=("$link")
done <<< "$root_links"
all_paths=$(find "${find_targets[@]}" -print) || find_status=$?
if (( find_status != 0 )); then
    echo "naming check: the path enumeration itself failed" >&2
    exit 2
fi
relative_paths="${all_paths//"$root/"/}"
path_status=0
path_hits=$(grep -iF -f "$patterns" <<< "$relative_paths") || path_status=$?
if (( path_status >= 2 )); then
    echo "naming check: the path scan itself failed" >&2
    exit 2
fi

# Strip the exempt documents and the checker's own term list. Done after the
# scan rather than via grep excludes so the exemption logic is one readable
# predicate instead of flag soup.
filtered=""
# Exempt files were excluded from the grep input, so every content hit
# here is a real one — no per-line exemption parsing (review: a
# colon-bearing filename made output parsing ambiguous, so the exempt
# set is filtered from the INPUT instead).
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    filtered+="$line"$'\n'
done <<< "$content_hits"
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    is_exempt "$root/$line" || filtered+="(path) $line"$'\n'
done <<< "$path_hits"

if [[ -n "$filtered" ]]; then
    echo "naming check: a commercial transport mark appears where the product" >&2
    echo "describes itself (in a file, or as a file's own name). Cite it in" >&2
    echo "docs/PRIOR_ART_SEARCH_PLAN.md if it is prior art; otherwise rename." >&2
    echo "The rule and its reason live in CONTRIBUTING.md ('Naming and claims')." >&2
    printf '%s' "$filtered" >&2
    exit 1
fi

echo "naming check: clean"

#!/usr/bin/env bats
# Tests for scripts/check-naming.sh and scripts/check-egress-claims.sh (#490).
#
# The acceptance bar is that both checks are PROVEN CAPABLE OF FAILING, not
# merely present: a green check that cannot go red is a decoration. Every
# denylisted term used here is built by concatenation, because these tests
# live inside the scanned surface and a literal term would fail the very
# check they exercise — which is itself a nice demonstration that the scan
# has no test-file blind spot.

setup() {
    NAMING="${BATS_TEST_DIRNAME}/../check-naming.sh"
    CLAIMS="${BATS_TEST_DIRNAME}/../check-egress-claims.sh"
    ROOT="$(mktemp -d)"
    mkdir -p "$ROOT/scripts" "$ROOT/docs" "$ROOT/crates/demo/src"
    cp "${BATS_TEST_DIRNAME}/../naming-denylist.txt" "$ROOT/scripts/"
    # A term from the real list, assembled so it never appears literally.
    TERM="$(printf 'as%s' 'pera')"
}

teardown() {
    rm -rf "$ROOT"
}

# --- the naming check -------------------------------------------------------

@test "a clean tree passes the naming check" {
    echo "fn main() {}" > "$ROOT/crates/demo/src/lib.rs"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
}

@test "a term in a scanned file fails, naming the file" {
    printf 'pub mod %s_shim;\n' "$TERM" > "$ROOT/crates/demo/src/lib.rs"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    [[ "$output" == *"crates/demo/src/lib.rs"* ]]
}

@test "matching is case-insensitive: shorthand in a doc fails too" {
    upper="$(printf '%s' "$TERM" | tr '[:lower:]' '[:upper:]')"
    printf 'like %s but ours\n' "$upper" > "$ROOT/docs/NOTES.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a hyphenated CLI-flag spelling of a multi-word mark is caught" {
    # Terms built by concatenation so this scanned test file carries no mark.
    # kebab-case (a --flag spelling) must be listed like space/snake/concat.
    flag="--$(printf 'file%s' '-catalyst')-mode"
    printf 'run with %s here\n' "$flag" > "$ROOT/docs/CLI.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "the ordinary spaced spelling of a mark is caught" {
    spaced="$(printf 'Media%s' ' Shuttle')"
    printf 'the %s product\n' "$spaced" > "$ROOT/docs/CLI.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "the prior-art document is exempt by its exact path" {
    printf '| **%s** | prior art row |\n' "$TERM" > "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
}

@test "the exemption does not leak to sibling documents" {
    # Same content, different file under docs/: a NEW document is scanned by
    # default — the exemption names two files, never a directory.
    printf '| **%s** | not a prior art file |\n' "$TERM" > "$ROOT/docs/COMPARISON2.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "an empty term list is an error, not a pass" {
    grep '^#' "${BATS_TEST_DIRNAME}/../naming-denylist.txt" > "$ROOT/scripts/naming-denylist.txt"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 2 ]
}

@test "a file NAMED after a mark fails even when its body never repeats it" {
    # The path scan (review): a crate, scenario or script named after a
    # mark is one of the rule's primary surfaces, and a file whose body
    # never repeats its own name would otherwise pass.
    printf 'echo hello\n' > "$ROOT/scripts/${TERM}_shim.sh"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    [[ "$output" == *"(path)"* ]]
}

@test "a term made of regex syntax is matched as a fixed string, not a pattern" {
    # -F -f matching (review): a term containing regex metacharacters must
    # mean itself. A '.' treated as a wildcard would both miss the real
    # mark and flag unrelated prose.
    printf 'zz.qq\n' >> "$ROOT/scripts/naming-denylist.txt"
    printf 'this mentions zzXqq which a regex dot would match\n' > "$ROOT/docs/NOTES.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
    printf 'this mentions zz.qq literally\n' > "$ROOT/docs/NOTES.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a dot-prefixed root file is scanned like any other" {
    # The glob missed dotfiles (review), and a .something.yml is exactly
    # where a config key hides.
    upper="$(printf '%s' "$TERM" | tr '[:lower:]' '[:upper:]')"
    printf 'mode: %s\n' "$upper" > "$ROOT/.transfer.yml"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/.transfer.yml"
}

@test "a half-filled loss condition is refused by its own column" {
    write_doc "| c6i | 100ms | 0.1% |  | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Loss model"* ]]
}

@test "a filename with spaces or glob metacharacters is scanned safely" {
    # NUL-safe paths (review): a marked file whose NAME holds a space or a
    # glob char must still be scanned, and a clean one must not fail the
    # tree through word-splitting.
    printf 'has %s\n' "$TERM" > "$ROOT/docs/my [weird] name.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/docs/my [weird] name.md"
    printf 'clean\n' > "$ROOT/docs/a b c.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
    rm "$ROOT/docs/a b c.md"
}

@test "the top-level Rust source trees are scanned too" {
    # tests/ and fuzz/ hold named modules and scenarios exactly as crates/
    # does (review): a gate with an unscanned source tree is a gate with a
    # documented way around it.
    mkdir -p "$ROOT/tests"
    printf 'mod %s_scenario;\n' "$TERM" > "$ROOT/tests/t.rs"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a checkout under a directory named for a mark is judged by its own names" {
    # Repository-relative paths (review): find prints paths under ROOT
    # verbatim, so a tree that merely LIVES somewhere marked must not flag
    # every entry for its location.
    marked="$BATS_TEST_TMPDIR/${TERM}lab/root"
    mkdir -p "$marked/scripts" "$marked/docs"
    cp "${BATS_TEST_DIRNAME}/../naming-denylist.txt" "$marked/scripts/"
    echo clean > "$marked/docs/N.md"
    run bash "$NAMING" "$marked"
    [ "$status" -eq 0 ]
}

@test "a NEWLY ADDED root document is scanned without being named" {
    # The explicit list only names files that exist today (review): a new
    # top-level document must be born inside the gate, contents and name
    # alike.
    printf 'like %s but ours\n' "$TERM" > "$ROOT/NEW_GUIDE.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/NEW_GUIDE.md"
    printf 'clean body\n' > "$ROOT/${TERM}_guide.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    [[ "$output" == *"(path)"* ]]
}

@test "root-level product documents are scanned" {
    # CONTRIBUTING.md and COMMERCIAL.md are product-facing documents, and
    # the exemption list names exactly two prior-art files (review): a
    # mark in any other root document must fail.
    printf 'like %s but ours\n' "$TERM" > "$ROOT/COMMERCIAL.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a CRLF denylist still matches: the checker must not fail open on line endings" {
    sed 's/$/\r/' "${BATS_TEST_DIRNAME}/../naming-denylist.txt" > "$ROOT/scripts/naming-denylist.txt"
    printf 'like %s\n' "$TERM" > "$ROOT/docs/N.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/docs/N.md"
}

@test "a config key in a workflow file is scanned" {
    upper="$(printf '%s' "$TERM" | tr '[:lower:]' '[:upper:]')"
    mkdir -p "$ROOT/.github/workflows"
    printf 'env:\n  %s_MODE: on\n' "$upper" > "$ROOT/.github/workflows/x.yml"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a config key in a top-level compose file is scanned" {
    # The rule names config keys, and the compose files are where they live
    # (review, verified with a repro upstream).
    upper="$(printf '%s' "$TERM" | tr '[:lower:]' '[:upper:]')"
    printf 'services:\n  x:\n    environment:\n      %s_MODE: on\n' "$upper" \
        > "$ROOT/docker-compose.yml"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a path-enumeration error fails closed, never as a clean pass" {
    # DETERMINISTIC in every environment (review): the chmod-000 approach
    # cannot produce a traversal error under root — and the CI bats
    # container runs as root — so the failing find is injected through
    # PATH instead, which fails identically for any user.
    mkdir -p "$ROOT/stub"
    printf '#!/bin/sh\nexit 1\n' > "$ROOT/stub/find"
    chmod +x "$ROOT/stub/find"
    PATH="$ROOT/stub:$PATH" run bash "$NAMING" "$ROOT"
    [ "$status" -eq 2 ]
    [[ "$output" == *"enumeration"* ]]
}

@test "the repository itself is clean" {
    run bash "$NAMING" "${BATS_TEST_DIRNAME}/../.."
    [ "$status" -eq 0 ]
}

# --- the substantiation lint ------------------------------------------------

write_doc() {
    mkdir -p "$ROOT/docs"
    {
        echo "# Egress transport research"
        echo
        echo "## Measurements"
        echo
        echo "| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |"
        echo "|---|---|---|---|---|---|---|---|---|---|"
        if [ "$#" -gt 0 ]; then echo "$1"; fi
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
}

@test "a header-only table passes: no rows is an honest state" {
    write_doc
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a complete row passes" {
    write_doc "| c6i.xlarge | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a row missing one condition fails NAMING the column" {
    write_doc "| c6i.xlarge | 100ms | 0.1% | random (netem) | 1Gbit | | none | tcp | cwnd growth | 2026-09-07 |"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Buffer depth"* ]]
}

@test "a header missing a required column fails, so a rename cannot defang the lint" {
    {
        echo "## Measurements"
        echo "| Hardware | RTT | Loss rate | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |"
        echo "|---|---|---|---|---|---|---|---|---|"
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Loss model"* ]]
}

@test "a SHORT row cannot inherit the previous row's cells" {
    # The stale-array hole (review, two finders): awk arrays persist across
    # rows, so a row missing its trailing Date cell used to read the
    # previous row's date as its own and pass.
    write_doc "| c6i.xlarge | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |"
    echo "| c6i.xlarge | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth |" >> "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Date"* ]]
}

@test "a data row with a dash-only first cell is judged, not skipped as a separator" {
    # The separator guard is all-cells (review, with repro): '---' meaning
    # 'not measured' in the Hardware column is a data cell, and skipping the
    # row on that one cell's spelling waved a row missing Buffer depth
    # through the exact gate this lint is.
    write_doc "| --- | 100ms | 0.1% | random (netem) | 1Gbit | | none | tcp | cwnd growth | 2026-09-07 |"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Buffer depth"* ]]
}

@test "an escaped pipe is cell content, and cannot mask a missing column" {
    # Markdown spells a literal pipe inside a cell as \| (review): a raw
    # split turned it into an extra column, shifting every later value
    # right — so a row missing its trailing Date still filled all nine
    # checked positions.
    write_doc '| c6i \| x86 | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
    write_doc '| c6i \| x86 | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Date"* ]]
}

@test "a row with surplus cells is refused by count, not silently shifted" {
    write_doc '| c6i | x86 | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"11 cells but the header has 10"* ]]
}

@test "a row without outer pipes is judged, not invisible" {
    # Markdown permits rows without the leading pipe (review): keying the
    # row predicate on one waved the row — and its missing column — past
    # the gate entirely.
    write_doc
    echo 'c6i | 100ms | 0.1% | random (netem) | 1Gbit |  | none | tcp | cwnd growth | 2026-09-07' \
        >> "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Buffer depth"* ]]
}

@test "a cell ending in a literal backslash does not eat the delimiter" {
    # Backslash-run parity (review): a trailing literal backslash emits
    # \\ before the real delimiter, and treating that pipe as escaped
    # merged the columns and refused a valid row.
    write_doc '| path C:\\ | 100ms | 0.1% | random (netem) | 1Gbit | 250ms BDP | none | tcp | cwnd growth | 2026-09-07 |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a table without its delimiter row is no table, and fails" {
    # Without the delimiter markdown renders prose, not a table (review):
    # a header-only section with the delimiter deleted was reported clean.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"delimiter"* ]]
}

@test "a GFM short-hyphen delimiter of the right width is accepted" {
    # GFM requires ONE or more hyphens per delimiter cell, not three, and
    # honours colon alignment: a "-", "--" or ":-:" cell of the correct width
    # renders as a table and must pass (review).
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '| :- | - | -- | :-: | - | -: | - | - | - | - |'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a delimiter cell that is only a colon is still refused" {
    # A cell must carry at least one hyphen; ":" alone is not a delimiter cell.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '| : | - | - | - | - | - | - | - | - | - |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a delimiter of the wrong width is refused naming both counts" {
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"1 cells but the header has 10"* ]]
}

@test "a blank line between header and delimiter is a broken table, not a gap to step over" {
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo ''
        echo '|---|---|---|---|---|---|---|---|---|---|'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"immediately follow"* ]]
}

@test "a filename extending an exempt path past a colon is not exempt" {
    printf 'like %s\n' "$TERM" > "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md:copy"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md:copy"
}

@test "a mark-named root symlink is scanned" {
    printf 'clean\n' > "$ROOT/real.yml"
    ln -s real.yml "$ROOT/${TERM}_link.yml"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a symlink's content is never followed into the scan" {
    # A cleanly-named link to a directory with a marked descendant, or to
    # an exempt document, must not pull that content in under the link's
    # alias (review): only the link's own NAME is a surface.
    mkdir -p "$ROOT/outside"
    printf 'body has %s\n' "$TERM" > "$ROOT/outside/inner.md"
    ln -s ../outside "$ROOT/docs/clean_link"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
    # But a link NAMED after a mark is still flagged, by its name.
    printf 'ok\n' > "$ROOT/docs/real.md"
    ln -s real.md "$ROOT/docs/${TERM}_alias.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
}

@test "a row pasted after the table has ended is loose text, not a measurement" {
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo ''
        echo 'prose closes the table'
        echo ''
        echo '| stray | incomplete | row |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "an explicit-root symlink is not followed into the content scan" {
    # README.md itself a link to marked content: the operand must be
    # name-checked, never content-scanned (review).
    mkdir -p "$ROOT/realdocs"
    printf 'body has %s\n' "$TERM" > "$ROOT/realdocs/inner.md"
    ln -s realdocs/inner.md "$ROOT/README.md"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 0 ]
    rm "$ROOT/README.md"
}

@test "a colon-bearing non-exempt filename is not read as the exempt file" {
    printf '| %s | prior |\n' "$TERM" > "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md"
    printf 'has %s\n' "$TERM" > "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md:1:copy"
    run bash "$NAMING" "$ROOT"
    [ "$status" -eq 1 ]
    rm "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md:1:copy" "$ROOT/docs/PRIOR_ART_SEARCH_PLAN.md"
}

@test "a four-backtick fence holding a three-backtick line stays fenced" {
    {
        echo '# doc'
        echo '````'
        echo '```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '````'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a Measurements section inside an HTML comment is not the section" {
    {
        echo '# doc'
        echo '<!--'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '-->'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a Measurements heading inside a tilde fence is not the section" {
    {
        echo '# doc'
        echo '~~~'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '~~~'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a Measurements heading inside a code fence is not the section" {
    {
        echo '# doc'
        echo '```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '```'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "an inline HTML comment in a complete row does not skip the row" {
    write_doc '| c6i <!-- note --> | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
    write_doc '| c6i | 100ms | 0.1% | random | 1Gbit |  | none | tcp | cwnd <!-- x --> | 2026-09-08 |'
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Buffer depth"* ]]
}

@test "a Measurements section inside a raw HTML block is not the section" {
    {
        echo '# doc'
        echo '<pre>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '</pre>'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a four-space-indented marker inside a fence does not close it" {
    {
        echo '# doc'
        echo '````'
        echo '    ```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '````'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a Measurements section inside any raw HTML block is not the section" {
    for tag in div section iframe search DIV; do
        {
            echo '# doc'
            echo "<$tag>"
            echo '## Measurements'
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo ''
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a document without the measurements table fails" {
    echo "# empty" > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a blank line inside a type-1 HTML block does not reopen the section" {
    # A <pre> is a type-1 block: it closes on its END TAG, never on a blank
    # line. A contributor who planted a fake table inside a <pre> and split it
    # with a blank line must not have that blank line hand the section back to
    # the parser (review).
    {
        echo '# doc'
        echo '<pre>'
        echo '## Measurements'
        echo ''
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '</pre>'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a type-6 block still closes on a blank line, and a real table after it passes" {
    # <div> is type 6: a blank line closes it, so the genuine table that
    # follows is seen and the document passes. This is the counterpart to the
    # type-1 rule above — the two must not be conflated.
    {
        echo '# doc'
        echo '<div>'
        echo 'decorative preamble'
        echo '</div>'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a heading that only starts with Measurements is a different section" {
    # "## MeasurementsFake" must not be read as the Measurements section: a
    # renamed lookalike heading cannot smuggle a fake table past the exact
    # heading match (review).
    {
        echo '## MeasurementsFake'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a level-1 heading ends the Measurements section, just as a sibling does" {
    # The section must end at the next heading of level 1 OR 2. A table that
    # appears only after a following "# Other" heading is outside the section
    # and does not count (review).
    {
        echo '## Measurements'
        echo ''
        echo '# Other'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a type-1 block is not closed by a different type-1 element's end tag" {
    # <pre> must stay open until </pre>: a </script> is a foreign closer and
    # CommonMark keeps the content raw, so a fake table after it is not the
    # section (review).
    {
        echo '# doc'
        echo '<pre>'
        echo '</script>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        echo '</pre>'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a processing-instruction, declaration, or CDATA block hides a fake section" {
    # HTML block types 3/4/5 render literally, so a section planted inside one
    # is not the Measurements section (review).
    local open close
    for pair in '<?php|?>' '<!DOCTYPE|>' '<![CDATA[|]]>'; do
        open="${pair%|*}"
        close="${pair#*|}"
        {
            echo '# doc'
            echo "$open"
            echo '## Measurements'
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
            echo "$close"
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a single-line declaration closes, and a real table after it passes" {
    # <!DOCTYPE html> is a complete type-4 block on one line: it must not
    # swallow the genuine table that follows.
    {
        echo '# doc'
        echo '<!DOCTYPE html>'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a setext heading ends the Measurements section too" {
    # An empty section, then a level-1 (====) or level-2 (----) setext heading:
    # a table under that later heading is outside the section (review).
    for underline in '========' '--------'; do
        {
            echo '## Measurements'
            echo ''
            echo 'Appendix'
            echo "$underline"
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a standalone closing tag opens a close-on-blank-line block" {
    # A bare </pre> is not a type-1 opener; CommonMark treats it as a raw HTML
    # block that renders literally until a blank line, so a fake section right
    # after it is not the section (review).
    {
        echo '# doc'
        echo '</pre>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "an indented or tab-spaced ATX heading still ends the section" {
    # CommonMark allows up to three spaces of indent before an ATX heading and
    # a tab (not only a space) after the # run; the section terminator must
    # match both, or a following table is misattributed to Measurements
    # (review).
    for heading in '   # Appendix' "#	Appendix" '  ## Appendix'; do
        {
            echo '## Measurements'
            echo ''
            printf '%s\n' "$heading"
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "an indented setext underline still ends the section" {
    {
        echo '## Measurements'
        echo ''
        echo 'Appendix'
        echo '   ========'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a type-7 HTML block hides a fake section until a blank line" {
    # A standalone complete tag not in the type-1/6 lists (e.g. <x-widget>)
    # starts a CommonMark type-7 raw block that renders literally until a blank
    # line, so a fake heading and table right after it are not the section
    # (review).
    {
        echo '# doc'
        echo '<x-widget>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a type-7 tag with a > inside a quoted attribute still opens the block" {
    # A ">" inside a quoted attribute value is content, not the tag end, so
    # <x-widget data-x="a>b"> is still a lone tag and opens a type-7 raw block
    # — a fake section after it is not the section (review).
    for tag in '<x-widget data-x="a>b">' "<x-widget data-x='a>b'>"; do
        {
            echo '# doc'
            printf '%s\n' "$tag"
            echo '## Measurements'
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a type-7 block closes on a blank line, and a real table after it passes" {
    {
        echo '# doc'
        echo '<x-widget>'
        echo 'content'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "an unterminated raw-block opener inside a comment is not acted on" {
    # A multi-line comment carrying a stray <? / <![CDATA[ must not trip the
    # raw-block handlers mid-comment and eat the comment terminator; the real
    # table after the comment must still be seen (review).
    for stray in '<?' '<![CDATA[' '<!DOCTYPE'; do
        {
            echo '# doc'
            echo '<!--'
            printf '%s\n' "$stray"
            echo '-->'
            echo '## Measurements'
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 0 ]
    done
}

@test "an underline after a consumed block is a thematic break, not a setext heading" {
    # The section is STILL OPEN at the underline, and a CONSUMED block (a
    # multi-line comment) stands between the paragraph and the "----" (review):
    # para_prev must be cleared as those comment lines are skipped, so the
    # "----" is a thematic break over the raw comment block, not a setext
    # heading that closes Measurements and orphans the real table below.
    # Reverting the para_prev clearing makes this exact doc exit 1 — the
    # underline would close the section over the stale "Prose" antecedent — so
    # the test genuinely guards the fix.
    {
        echo '## Measurements'
        echo 'Prose'
        echo '<!--'
        echo 'a comment renders nothing'
        echo '-->'
        echo '----'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "an indented header and delimiter are code, not a measurements table" {
    # A header and delimiter indented four spaces (or a tab) render as an
    # indented code block, not a table, so they must not satisfy the gate
    # (review).
    for indent in '    ' "$(printf '\t')"; do
        {
            echo '## Measurements'
            printf '%s| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |\n' "$indent"
            printf '%s|---|---|---|---|---|---|---|---|---|---|\n' "$indent"
            printf '%s| c6i | 100ms | 0.1%% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |\n' "$indent"
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a comment-closing line is consumed whole, not reparsed as a heading" {
    # CommonMark keeps the entire line carrying "-->" inside the raw comment
    # block, so "-->## Measurements" yields no heading and a table under it is
    # not the section (review).
    {
        echo '# doc'
        echo '<!--'
        echo 'hidden'
        echo '-->## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "indented code between the header and delimiter breaks the table" {
    # A header row, then an indented-code line, then the delimiter: markdown
    # requires the delimiter to immediately follow the header, so the indented
    # code breaks the table and the section must close rather than accept the
    # later separator as a delimiter (review).
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '    indented code'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a tag followed by quoted prose is not a type-7 block" {
    # Only a lone tag opens a type-7 block; "<span> \"quote\"" has prose after
    # the tag, so the real Measurements section below it must still be seen and
    # a valid document must pass (review).
    {
        echo '<span> "a quoted phrase"'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a leading inline comment does not turn its suffix into a heading" {
    # A line beginning with "<!--" is a type-2 HTML block: the whole line is
    # raw, so "<!-- x -->## Measurements" is not a heading and a fake table
    # under it is not the section (review).
    {
        echo '# doc'
        echo '<!-- x -->## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a row that opens a multiline comment is still validated" {
    # The row prefix before an unclosed "<!--" must be checked, not swallowed
    # into comment state: an incomplete row there must fail, not pass unseen
    # (review).
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | <!-- the rest is commented out'
        echo 'still inside the comment'
        echo '-->'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"missing"* ]]
}

@test "an empty tag like </> does not open a type-7 block" {
    # "</>" has no tag name, so it is not a lone tag and must not open a raw
    # block that swallows the gap between a header and its delimiter (review):
    # the broken adjacency must fail rather than pass.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '</>'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a consumed line between the header and delimiter breaks the table" {
    # The delimiter must be the line immediately after the header. A consumed
    # line between them — a complete comment, a multi-line comment, or a fence —
    # advances past adjacency, so markdown renders no table and the section must
    # fail (review).
    # Complete single-line comment between header and delimiter.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '<!-- a comment -->'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    # Multi-line comment between header and delimiter.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '<!--'
        echo 'hidden'
        echo '-->'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    # Fenced block between header and delimiter.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '```'
        echo 'code'
        echo '```'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a second Measurements section is validated fresh, not folded into the first" {
    # Table state resets on each "## Measurements": a second section whose
    # header renames a required column must be caught, not read as data rows
    # under the first section's mapping (review).
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Notes | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a backtick line inside a raw HTML block does not open a fence" {
    # A "```" inside a <pre> is literal content, not a fence: opening one would
    # swallow the "</pre>" and a genuine table as fenced code, failing a valid
    # document (review).
    {
        echo '# doc'
        echo '<pre>'
        echo '```'
        echo 'literal code, not a fence'
        echo '</pre>'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a space-then-tab indented header and delimiter are code, not a table" {
    # Markdown expands tabs to four-column stops, so " <tab>" is four columns
    # of indentation — an indented code block, not table structure — even
    # though it is only one literal space (review).
    tab="$(printf '\t')"
    {
        echo '## Measurements'
        printf ' %s| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |\n' "$tab"
        printf ' %s|---|---|---|---|---|---|---|---|---|---|\n' "$tab"
        printf ' %s| c6i | 100ms | 0.1%% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |\n' "$tab"
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a fence nested in a list item hides a fake table" {
    # A fenced block can be the first content of a list item, its marker on the
    # opener line ("- ```"): the nested "## Measurements" heading and table are
    # code, not live structure, so the fake table must not be seen (review).
    for marker in '-' '*' '1.'; do
        pad='  '
        [ "$marker" = '1.' ] && pad='   '
        {
            printf '%s ```\n' "$marker"
            printf '%s## Measurements\n' "$pad"
            printf '%s| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |\n' "$pad"
            printf '%s|---|---|---|---|---|---|---|---|---|---|\n' "$pad"
            printf '%s| c6i | 100ms | 0.1%% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |\n' "$pad"
            printf '%s```\n' "$pad"
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a list-marker line inside a fence is content, not a closing fence" {
    # The list-marker strip runs only when detecting an OPENER: a "- \`\`\`"
    # line inside a bare fence is content, not a closer, so a fake table after
    # it stays inside the code fence and is not the section (review).
    {
        echo '```'
        echo '- ```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '```'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a backtick in a backtick-fence info string makes it not an opener" {
    # "\`\`\`bad\`info" is not a valid opener (a backtick fence info string
    # cannot contain a backtick), so the later bare "\`\`\`" is the opener and
    # the table between opener and closer is code, not the section (review).
    {
        printf '```bad`info\n'
        echo '```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '```'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a list fence with wide marker padding closes at its content indent" {
    # "-   \`\`\`" opens a fence whose content and closer align to column 4;
    # the closer must be recognized there so the fence closes and a REAL table
    # after it is seen and the document passes (review).
    tab_none=''
    {
        printf -- '-   ```\n'
        echo '    a code example'
        echo '    ```'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "valid ATX source forms of the Measurements heading are accepted" {
    # "## Measurements ##" (closing hashes), a tab after the hashes, and extra
    # spaces are all the heading "Measurements" (review).
    for heading in '## Measurements ##' "##	Measurements" '##   Measurements   '; do
        {
            printf '%s\n' "$heading"
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 0 ]
    done
    # Longer heading text is still a different section.
    {
        echo '## MeasurementsX'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "an incomplete first Measurements section is not rescued by a later one" {
    # A first section with a header and no delimiter, closed by a later heading,
    # renders no table and must fail even when a second valid section follows
    # (review).
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo ''
        echo '## Other'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a tab-padded list fence is closed at its visual content column, not earlier" {
    # "-<tab>\`\`\`" puts content at column 4; a 3-space closer is a dedent that
    # does NOT close it, so a fake table before the real column-4 closer stays
    # code and is not the section (review).
    {
        printf -- '-\t```\n'
        echo '    ## Measurements'
        echo '    | Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '    |---|---|---|---|---|---|---|---|---|---|'
        echo '   ```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "an unindented closer does not close a list-item fence" {
    # A list fence with base column 2 ("- \`\`\`") is not closed by a column-0
    # \`\`\` (that is a dedent ending the list); the fake table after it is still
    # code and not the section (review).
    {
        echo '- ```'
        echo '  ## Measurements'
        echo '  | Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '  |---|---|---|---|---|---|---|---|---|---|'
        echo '```'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a thematic break after a list item does not close the section" {
    # "---" after a list item is a thematic break, not a setext heading over the
    # list item, so the Measurements section stays open and the table passes
    # (review).
    {
        echo '## Measurements'
        echo '- setup note'
        echo '---'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "an incomplete first section closed by indented code is not rescued by a later one" {
    # A first section with a header and no delimiter, closed by an INDENTED-CODE
    # line (not a blank line, so the delimiter-gap rule never fires), renders no
    # table. The close must record that missing delimiter, or a second valid
    # section supplies the final header/delimiter and the broken first table
    # passes clean (review): the indented-code close path must finalize too.
    {
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '    indented code closes the section'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
    [[ "$output" == *"header but no delimiter"* ]]
}

@test "a lone tag after a paragraph does not open a type-7 block" {
    # A CommonMark type-7 HTML block cannot interrupt a paragraph: a lone
    # "<x-widget>" on the line right after paragraph text is inline content
    # continuing the paragraph, so a REAL "## Measurements" heading after it DOES
    # interrupt the paragraph and its table must be seen — the document passes
    # (review). (A tag after a blank line or a heading still opens the block.)
    {
        echo 'intro paragraph describing the section'
        echo '<x-widget>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "five spaces after a list marker is indented code, not a fence opener" {
    # CommonMark caps list-item padding at four columns: a marker followed by
    # 5+ spaces gives one space of padding and leaves the rest as CONTENT
    # indentation, so "-     \`\`\`" is an indented-code line, not a fence
    # opener. It must NOT open a fence that swallows the real Measurements
    # heading and table below it — the document passes (review).
    {
        printf -- '-     ```\n'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a top-level fence indented three spaces is closed by a column-0 fence" {
    # A fenced code block opener may be indented 0-3 spaces, and CommonMark lets
    # its closer sit at 0-3 spaces REGARDLESS of the opener indent — so a
    # column-0 "```" closes a three-space-indented opener. A non-list fence must
    # therefore anchor its closer at column 0, not at the opener indent, or a
    # valid document with a real table after the fence fails CI (review).
    {
        printf '   ```\n'
        echo '   a code example'
        printf '```\n'
        echo ''
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a thematic break before a lone tag does not suppress the raw block" {
    # A thematic break (---, ***, ___) is not paragraph text, so a lone tag on
    # the next line DOES open a type-7 raw block. The paragraph-state guard must
    # not treat the rule as an open paragraph, or a fake table hidden inside
    # <x-widget> after the rule would be accepted (review).
    for rule in '---' '***' '___'; do
        {
            printf '%s\n' "$rule"
            echo '<x-widget>'
            echo '## Measurements'
            echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
            echo '|---|---|---|---|---|---|---|---|---|---|'
            echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
        } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
        [ "$status" -eq 1 ]
    done
}

@test "a tab after spaces in list-marker padding counts absolute columns" {
    # "   -<tab> ```" has five columns of padding once the marker's absolute
    # column is counted (three-space indent + marker + a tab to the next stop +
    # a space), so the ``` is indented code, not a fence opener. It must NOT open
    # a fence that swallows the real table below it (review).
    {
        printf '   -\t ```\n'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "an inline comment does not synthesize a Measurements heading" {
    # An inline comment is inline content and cannot create block structure:
    # "#<!-- x --># Measurements" is paragraph text, not a "## Measurements"
    # heading. Stripping the comment must not concatenate the two "#" into a
    # heading marker, or a fake table under it would be accepted (review).
    {
        echo '#<!-- x --># Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "an invalid tag-like line is text, and a heading after it is seen" {
    # "<x =>" is not a syntactically complete tag (an attribute cannot start with
    # "="), so it is ordinary Markdown text, not a type-7 raw block. A real
    # "## Measurements" heading after it interrupts the paragraph and must be
    # seen — the valid document passes (review).
    {
        echo '<x =>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a list-nested fence ends when its item dedents" {
    # An unclosed fence inside a list item ends when a non-blank line dedents out
    # of the item. The parser must resume on that outside line, not consume the
    # real Measurements section that follows as fenced code (review).
    {
        printf -- '- ```\n'
        echo '    an indented code line'
        echo 'ordinary unindented line ends the list item'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a raw HTML block as the first content of a list item hides a fake section" {
    # A type-6 block can be the first content of a list item ("- <div>"): Markdown
    # removes the list container before recognizing the raw HTML block, so the
    # indented fake heading and table after it render inside <div> and are not the
    # section. The opener must be tested on the container-normalized line (review).
    {
        echo '- <div>'
        echo '  ## Measurements'
        echo '  | Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '  |---|---|---|---|---|---|---|---|---|---|'
        echo '  | c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a four-space-indented tag is code, not a raw HTML block" {
    # A four-space-indented line is indented code, so "    <div>" is NOT a raw
    # HTML block and the real Measurements table after it must be seen. The
    # container-normalized opener must still respect the four-column code rule
    # (review): stripping three spaces once let "    <div>" become " <div>",
    # which matched the opener and hid the table.
    {
        echo '    <div>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a blockquote line does not suppress a following raw block" {
    # A blockquote is a container construct, not paragraph text, so a lone tag on
    # the line after "> # quoted heading" opens a type-7 raw block. The paragraph
    # state must not treat the blockquote as an open paragraph, or a fake table
    # hidden inside <x-widget> after it would be accepted (review).
    {
        echo '> # quoted heading'
        echo '<x-widget>'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 1 ]
}

@test "a lowercase declaration opener is text, not a raw block" {
    # CommonMark's declaration block requires an UPPERCASE ASCII letter after
    # "<!", so "<!foo" is ordinary text, not a raw block. It must not swallow the
    # real Measurements heading and table to EOF — the valid document passes
    # (review).
    {
        echo '<!foo'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "a fence opened on a list continuation line ends with its item" {
    # A fence opened on a CONTINUATION line of a list item ("- item" then a
    # two-space-indented "```") has no marker of its own, but its base is the
    # item content column: a dedent out of the item ends it. The parser must
    # resume on the outside line, not eat the real table as fenced code (review).
    {
        echo '- item text'
        printf '  ```\n'
        echo '  code inside the item fence'
        echo 'back to column zero ends the item'
        echo '## Measurements'
        echo '| Hardware | RTT | Loss rate | Loss model | Bottleneck rate | Buffer depth | Competitor state | Transport | Bottleneck | Date |'
        echo '|---|---|---|---|---|---|---|---|---|---|'
        echo '| c6i | 100ms | 0.1% | random | 1Gbit | 250ms | none | tcp | cwnd | 2026-09-08 |'
    } > "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    run bash "$CLAIMS" "$ROOT/docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

@test "the repository's own document passes its lint" {
    run bash "$CLAIMS" "${BATS_TEST_DIRNAME}/../../docs/EGRESS_TRANSPORT_RESEARCH.md"
    [ "$status" -eq 0 ]
}

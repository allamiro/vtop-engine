#!/usr/bin/env bash
# The substantiation rule, enforced mechanically (#490): every measurement
# row in docs/EGRESS_TRANSPORT_RESEARCH.md carries its conditions, so a
# number can never outlive the context that made it true. A row missing a
# column is refused BY NAME, because "the lint failed" teaches nothing and
# "row 3 is missing 'Buffer depth'" fixes itself.
#
# The required columns are the conditions a transport comparison is
# meaningless without: what hardware, what RTT, what loss rate and what loss model (independently — half a
# loss condition is no condition, review),
# what bottleneck rate, what buffer depth, whether a competing flow was
# present, which transport, what actually limited throughput, and when.
#
# Usage: check-egress-claims.sh [DOC]   (default: docs/EGRESS_TRANSPORT_RESEARCH.md)
set -euo pipefail

doc="${1:-docs/EGRESS_TRANSPORT_RESEARCH.md}"

if [[ ! -f "$doc" ]]; then
    echo "claims lint: $doc does not exist" >&2
    exit 2
fi

awk -v doc="$doc" '
    function trim(s) { gsub(/^[ \t\r]+|[ \t\r]+$/, "", s); return s }
    # Leading indentation measured in COLUMNS, with tabs advancing to the next
    # four-column tab stop (review): markdown expands tabs before counting
    # indent, so " <tab>" is four columns — indented code — even though it is
    # only one literal space. A literal-space or bare-tab test cannot see that.
    function indent_cols(s,    i, c, col) {
        col = 0
        for (i = 1; i <= length(s); i++) {
            c = substr(s, i, 1)
            if (c == " ") col++
            else if (c == "\t") col += 4 - (col % 4)
            else break
        }
        return col
    }
    # Visual width of an arbitrary prefix string, tabs expanded to four-column
    # tab stops (review): used to turn a list-item marker prefix into the column
    # its content begins at, so a fence closer is matched at the right column.
    function visual_width(s,    i, c, col) {
        col = 0
        for (i = 1; i <= length(s); i++) {
            c = substr(s, i, 1)
            if (c == "\t") col += 4 - (col % 4)
            else col++
        }
        return col
    }
    # Is the whole line a single COMPLETE HTML tag (open or closing) followed
    # only by whitespace — a CommonMark type-7 block start? The tag boundary is
    # found by scanning for the first ">" that is NOT inside a quoted attribute
    # value, so a ">" in <x data="a>b"> does not end the tag, and quoted PROSE
    # after a tag (<span> "quote") is not erased into a lone tag either (review).
    function lone_tag(s,    n, i, closing, start) {
        sub(/^ {0,3}/, "", s)
        n = length(s)
        if (substr(s, 1, 1) != "<") return 0
        # A real tag NAME must follow "<" or "</" — "</>" and "<>" are not tags
        # (review). Closing tags carry no attributes, so nothing but whitespace
        # may sit between the name and its ">".
        closing = 0
        start = 2
        if (substr(s, 2, 1) == "/") { closing = 1; start = 3 }
        if (substr(s, start, 1) !~ /[a-zA-Z]/) return 0
        i = start
        while (i <= n && substr(s, i, 1) ~ /[a-zA-Z0-9-]/) i++
        if (closing) return (substr(s, i) ~ /^[ \t]*>[ \t]*$/)
        # An OPEN tag must be a SYNTACTICALLY COMPLETE tag, not merely a run of
        # anything up to the first unquoted ">" (review): CommonMark type-7 only
        # opens on a valid open tag, so "<x =>" is ordinary text and a heading
        # after it must be seen. The attribute grammar is validated below.
        return valid_open_tail(s, i, n)
    }
    # True when an unquoted attribute value may NOT contain c. The single quote
    # is compared via SQ so no literal apostrophe appears in this program.
    function bad_unquoted(c) {
        return (c == " " || c == "\t" || c == "\"" || c == SQ \
                || c == "=" || c == "<" || c == ">" || c == "`")
    }
    # Validate the tail of an OPEN tag beginning at position i (just past the tag
    # name): zero or more attributes each PRECEDED by whitespace, then optional
    # whitespace, an optional "/", and ">", followed only by whitespace. An
    # attribute is a name ([a-zA-Z_:][-a-zA-Z0-9_.:]*) with an optional
    # =value (unquoted, single- or double-quoted). Anything else is not a tag.
    function valid_open_tail(s, i, n,    c, saw_ws, j, qc) {
        while (i <= n) {
            saw_ws = 0
            while (i <= n && (substr(s, i, 1) == " " || substr(s, i, 1) == "\t")) { i++; saw_ws = 1 }
            c = substr(s, i, 1)
            if (c == ">") return (substr(s, i + 1) ~ /^[ \t]*$/)
            if (c == "/") {
                i++
                if (substr(s, i, 1) != ">") return 0
                return (substr(s, i + 1) ~ /^[ \t]*$/)
            }
            # An attribute must be introduced by whitespace and start with a
            # name character; "=" or any other byte here is not a valid tag.
            if (!saw_ws) return 0
            if (c !~ /[a-zA-Z_:]/) return 0
            i++
            while (i <= n && substr(s, i, 1) ~ /[-a-zA-Z0-9_.:]/) i++
            # Optional value: [ws]* "=" [ws]* value. Peek for the "=".
            j = i
            while (j <= n && (substr(s, j, 1) == " " || substr(s, j, 1) == "\t")) j++
            if (substr(s, j, 1) == "=") {
                i = j + 1
                while (i <= n && (substr(s, i, 1) == " " || substr(s, i, 1) == "\t")) i++
                c = substr(s, i, 1)
                if (c == "\"" || c == SQ) {
                    qc = c; i++
                    while (i <= n && substr(s, i, 1) != qc) i++
                    if (i > n) return 0
                    i++
                } else {
                    if (c == "" || bad_unquoted(c)) return 0
                    while (i <= n && !bad_unquoted(substr(s, i, 1))) i++
                }
            }
        }
        return 0
    }
    # Strip a leading list-item marker and its padding, but ONLY when the
    # padding is 1-4 columns (review): CommonMark caps list-item padding at four
    # columns, so a marker followed by five or more spaces gives ONE column of
    # padding with the rest as CONTENT indentation — "-     ```" is an
    # indented-code line, not a fence opener. Returns the content after the
    # marker when it is a same-line list opener, or the string unchanged
    # otherwise (no marker, no padding, or 5+ columns of it).
    function strip_list_marker(s, lead,    marker, after, pad, col, j, ch, adv) {
        if (s !~ /^([-*+]|[0-9]{1,9}[.)])[ \t]/) return s
        match(s, /^([-*+]|[0-9]{1,9}[.)])/)
        marker = substr(s, 1, RLENGTH)
        after = substr(s, RLENGTH + 1)
        # Padding is measured in COLUMNS from the marker end, tabs advancing to
        # the next four-column stop, so "-<tab>" is three columns of padding.
        # `lead` is the ABSOLUTE visual column the marker starts at — the width
        # of any leading indent already stripped from the line — so a tab in the
        # padding lands on the right tab stop even when the marker itself is
        # indented (review). Without it "   -\t ```" miscounts its padding and
        # opens a fence where CommonMark has indented code.
        col = lead + RLENGTH
        pad = 0
        j = 1
        while (j <= length(after)) {
            ch = substr(after, j, 1)
            if (ch == " ") { pad++; col++; j++ }
            else if (ch == "\t") { adv = 4 - (col % 4); pad += adv; col += adv; j++ }
            else break
        }
        if (pad >= 1 && pad <= 4) return substr(after, j)
        return s
    }
    # Is the line a THEMATIC BREAK — three or more matching -, * or _ characters,
    # each optionally separated by spaces or tabs, and nothing else (review)? A
    # thematic break is never paragraph text: it cannot be a setext antecedent,
    # and a lone type-7 tag on the line after it is NOT a paragraph continuation,
    # so it must not set the paragraph state that suppresses a raw-block opener.
    # "=" is deliberately excluded — "===" with nothing above it is a PARAGRAPH,
    # not a rule, so treating it as one would fail a valid document.
    function ruling_line(s,    t) {
        t = s
        sub(/^ {0,3}/, "", t)
        if (t ~ /[^-*_ \t]/) return 0
        gsub(/[ \t]/, "", t)
        return (t ~ /^-{3,}$/ || t ~ /^\*{3,}$/ || t ~ /^_{3,}$/)
    }
    # A section that had a header but never its delimiter renders NO table, so
    # closing or replacing it must record that failure rather than silently
    # discard the incomplete state (review): otherwise a first Measurements
    # section with a header and no delimiter, closed by a later heading, would
    # let a second valid table supply the final header/delimiter and pass clean.
    function finalize_section() {
        if (in_section && header_seen && !delimiter_seen) {
            printf "claims lint: a Measurements section has a header but no delimiter row, so it would not render as a table\n" > "/dev/stderr"
            failures++
        }
    }
    BEGIN {
        # CommonMark HTML block type 1 (pre/script/style/textarea) closes on
        # its END TAG; type 6 (every other block-level tag) closes on a
        # BLANK line. The two must be tracked apart or a blank line inside a
        # <pre> would wrongly end it.
        HTML1_TAGS = "pre|script|style|textarea"
        # The type-1 names appear in the type-6 set too (review): a type-1 block
        # only STARTS on an OPENING tag, so a standalone CLOSING tag like
        # </pre> is not a type-1 opener — it is a close-on-blank-line block, and
        # a fake section right after it must not be read as the section. The
        # type-1 opener rule runs first, so a real <pre> is still type 1; only a
        # bare closer falls through to here.
        HTML6_TAGS = "address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h1|h2|h3|h4|h5|h6|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|nav|noframes|ol|optgroup|option|p|param|pre|script|search|section|source|style|summary|table|tbody|td|tfoot|th|thead|textarea|title|tr|track|ul"
        in_fence = 0
        fence_char = ""
        fence_len = 0
        # The content column of the CURRENTLY-OPEN list item, carried across
        # lines (#490 review): a fence opened on a CONTINUATION line of a list
        # item ("- item" then "  ```") has no marker of its own, so its base
        # column is the item content column tracked here, not zero. Set when a
        # list marker is seen, cleared by a blank line or a dedent out of the
        # item. This is a single-level heuristic, not full container parsing.
        list_col = 0
        # Columns the fence base indent occupies (a list item content
        # indent); the closer may sit at fence_base + 0-3 spaces.
        fence_base = 0
        in_comment = 0
        # Set when a line opened a mid-line comment that did not close: the
        # comment takes effect on the NEXT line, so the opening line prefix
        # still validates.
        pending_comment = 0
        html_block_type = 0
        html1_tag = ""
        # A CommonMark HTML block of type 3 (<?...?>), 4 (<!DECL...>) or 5
        # (<![CDATA[...]]>) renders literally; while inside one, raw_end holds
        # the token whose line closes it.
        raw_end = ""
        # A single-quote character, built by code so it never appears literally
        # inside this single-quoted awk program: used to strip single-quoted
        # HTML attribute values when testing for a lone type-7 tag.
        SQ = sprintf("%c", 39)
        # Setext support keeps two variables: para_prev accumulates the CURRENT
        # line paragraph text (set only for a real text line), and prev_para is
        # the PREVIOUS one, promoted from it at the top of each line. The
        # promotion clears para_prev every line, so a line consumed early (a
        # fence, an HTML block, a heading, indented code) leaves prev_para empty
        # for the next line — an underline only counts over genuine paragraph
        # text, never over a skipped or code line.
        prev_para = ""
        para_prev = ""
        req = "Hardware;RTT;Loss rate;Loss model;Bottleneck rate;Buffer depth;Competitor state;Transport;Bottleneck;Date"
        required_count = split(req, required, ";")
        in_section = 0
        header_seen = 0
        delimiter_seen = 0
        failures = 0
        rows = 0
    }
    # CRLF is stripped from EVERY line first (review): a closing fence or a
    # delimiter that keeps its carriage return would never match, leaving a
    # fence open or a table unrecognized across the whole document.
    { sub(/\r$/, "") }
    # Promote the previous line paragraph text and clear the accumulator for
    # THIS line (review): only a line that renders as paragraph text sets
    # para_prev again (at the end), so any line consumed before then leaves the
    # setext antecedent empty — a "---"/"====" after indented code or a skipped
    # line is a thematic break, not a heading.
    { prev_para = para_prev; para_prev = "" }
    # A mid-line comment opened on the PREVIOUS line takes effect now (review):
    # deferring the transition to the top of the next line let that previous
    # line prefix — e.g. a table row before "<!--" — validate first.
    { if (pending_comment) { in_comment = 1; pending_comment = 0 } }
    # FENCE STATE IS DECIDED BEFORE ANYTHING ELSE (review): inside a fenced
    # block a "<!--" or a "## Measurements" is code, not structure, so the
    # fence handler runs first and consumes the line. A fence marker may be
    # indented 0-3 spaces; a 4+-space indent is itself code, so at most three
    # leading spaces are ignored before looking for the marker.
    {
        orig_line = $0
        fline = $0
        # A line indented FOUR OR MORE columns is indented code, whether the
        # indent is a tab (four columns) or four spaces (review): only then is
        # the leading indent stripped and a fence/HTML opener looked for. When it
        # IS indented code, fline keeps its indent so the "^ {0,3}<" openers below
        # cannot match it — a "    <div>" is code, not a raw-HTML block, and the
        # real table after it must still be seen. (Stripping only three spaces
        # here once let a four-space "    <div>" become " <div>", which matched.)
        indented_code = (indent_cols(orig_line) >= 4)
        if (!indented_code) sub(/^ {0,3}/, "", fline)
        if (in_fence) {
            cl_cols = indent_cols($0)
            # A LIST fence (base > 0) ends when a non-blank line DEDENTS below its
            # content column: the list item that contained it has ended, and an
            # open fenced block ends with its container (review). That outside
            # line is not code — fall through (no `next`) so it is parsed as
            # ordinary structure, which may be the real heading and table. A
            # NON-list fence (base 0) has no container to leave; it closes only on
            # its own fence line or at EOF.
            if (fence_base > 0 && cl_cols < fence_base && $0 !~ /^[ \t]*$/) {
                in_fence = 0
            } else {
                # The CLOSER is a run of the fence character and optional
                # whitespace, at the fence base COLUMN plus the 0-3 spaces
                # CommonMark allows — no more, no less (review). A MORE indented
                # run is content. Columns are tab-aware so tab padding lands on
                # its tab stop.
                cl_rest = $0
                sub(/^[ \t]*/, "", cl_rest)
                closed = 0
                if (cl_cols >= fence_base && cl_cols <= fence_base + 3 \
                        && cl_rest ~ ("^[" fence_char "]+[ \t]*$")) {
                    run = 0
                    while (substr(cl_rest, run + 1, 1) == fence_char) run++
                    if (run >= fence_len) closed = 1
                }
                if (closed) in_fence = 0
                next
            }
        }
        # Leave the active list item on a blank line or a dedent below its
        # content column (review): a new marker below resets it. Reached only
        # when not consumed by an open fence above.
        if ($0 ~ /^[ \t]*$/ || indent_cols(orig_line) < list_col)
            list_col = 0
        # A fenced block can be the first content of a LIST item, whose opener
        # carries the item marker on the same line (review): strip an optional
        # "- " / "* " / "+ " / "N. " / "N) " prefix ONLY when detecting an
        # OPENER (above, the closer path is already past), so "- ```" opens a
        # fence just as a bare "```" would while a "- ```" inside a fence stays
        # content. Only 1-4 columns of marker padding are stripped, though: a
        # marker followed by 5+ spaces leaves the content indented as CODE, not
        # a fence opener (strip_list_marker, review). The VISUAL column the
        # content starts at — the width of the leading indent plus the marker
        # and its padding, tab-aware — becomes the fence base so the closer
        # aligned to that column is recognized.
        list_fence = 0
        if (!indented_code) {
            fline_before = fline
            # The leading indent already stripped (line start minus fline) is
            # pure spaces here — a tab-led line is indented_code — so its length
            # is its visual column, the absolute base the marker sits at.
            fline = strip_list_marker(fline, length(orig_line) - length(fline))
            # A marker was actually consumed: this is a LIST fence, and only then
            # does its content column anchor the closer (below). Record that
            # content column as the active list column too, so a fence opened on
            # a later CONTINUATION line of the same item inherits it.
            if (fline != fline_before) {
                list_fence = 1
                list_col = visual_width(substr(orig_line, 1, \
                                length(orig_line) - length(fline)))
            }
        }
        # A fence only OPENS when no raw HTML or comment block is active
        # (review): a triple-backtick line inside a <pre> (or a comment, or a
        # PI/CDATA block) is literal content of that block, not a fence — and
        # opening one there would swallow the closing tag and a real table as
        # fenced code. Those states are handled by the rules just below.
        if (html_block_type == 0 && !in_comment && raw_end == "" \
                && !indented_code && (fline ~ /^`+/ || fline ~ /^~+/)) {
            fc = substr(fline, 1, 1); fl = 0
            while (substr(fline, fl + 1, 1) == fc) fl++
            if (fl >= 3) {
                # A BACKTICK fence info string cannot itself contain a backtick
                # (review): "```bad`info" is not an opener, so a later bare
                # "```" is the opener rather than a closer. Tilde fences place
                # no such restriction on their info string.
                rest = substr(fline, fl + 1)
                if (fc != "`" || index(rest, "`") == 0) {
                    fence_char = fc; fence_len = fl; in_fence = 1
                    # A NON-LIST fence has base column ZERO (review): CommonMark
                    # lets a fenced-code closer sit at 0-3 spaces of indent
                    # REGARDLESS of the opener own 0-3 indent, so a "   ```"
                    # opener is closed by a column-0 "```". Only a LIST fence
                    # anchors its closer to the content column the marker pushed
                    # it to — there a dedent below that column ends the list
                    # item and leaves the fence unterminated. So the base is the
                    # VISUAL width of the stripped leading-indent-plus-marker
                    # prefix ONLY when a marker was consumed; otherwise 0. This
                    # keeps a "-\t```" opener (content at column 4) closed by a
                    # column-4 closer, not a column-2 one.
                    if (list_fence)
                        fence_base = visual_width(substr(orig_line, 1, \
                                        length(orig_line) - length(fline)))
                    else if (list_col > 0 && indent_cols(orig_line) >= list_col)
                        # A fence opened on a CONTINUATION line of an open list
                        # item (no marker of its own): its base is the item
                        # content column, so a dedent out of the item closes it.
                        fence_base = list_col
                    else
                        fence_base = 0
                    next
                }
            }
        }
    }
    # Raw HTML blocks render their contents literally, so a fake section
    # inside one is not the section (review, twice): CommonMark HTML block
    # type 6 opens on an open OR close tag of a known block-level element —
    # case-insensitively, the full type-6 tag set, not four lowercase names
    # — and closes on a BLANK line. That blank-line close is the type-6 rule
    # and needs no matching end tag.
    # The HTML-block OPENERS test the list-container-normalized line `fline`
    # (leading indent and any list marker already stripped in the fence block),
    # not the raw source (review): a raw HTML block can be the first content of a
    # list item — "- <div>" then an indented fake table — and CommonMark removes
    # the list container before recognizing the block. Testing $0 missed that and
    # reported the fake table clean. (The block CLOSE checks below stay on $0:
    # once open, the whole line including its indent is raw content.)
    html_block_type == 0 && raw_end == "" && !in_comment && tolower(fline) ~ ("^ {0,3}<(" HTML1_TAGS ")([ \t/>]|$)") {
        # Record WHICH type-1 tag opened the block: it closes only on its own
        # end tag, not on any type-1 closer (review). <pre> followed by
        # </script> must stay open until </pre>.
        html1_tag = tolower(fline)
        sub(/^ {0,3}</, "", html1_tag)
        sub(/[^a-z].*$/, "", html1_tag)
        html_block_type = 1
        # A type-1 block can close on the same line it opened.
        if (tolower(fline) ~ ("</" html1_tag ">")) html_block_type = 0
        next
    }
    # Raw HTML block types 3/4/5 also render literally, so a fake section
    # inside one is not the section (review): type 3 is a processing
    # instruction <?...?>, type 4 a declaration <!UPPERCASE...> (the opener
    # requires an UPPERCASE ASCII letter after "<!" — "<!foo" is ordinary text,
    # not a raw block, so it must not swallow a following table, review), type 5 a CDATA
    # section <![CDATA[...]]>. Each ends on the line carrying its close token.
    # The raw-block openers are guarded with !in_comment (review): a multi-line
    # HTML comment can contain an unterminated "<?", "<!..." or "<![CDATA[",
    # and without the guard these handlers would run mid-comment and eat the
    # comment terminator, letting a later real table go unseen.
    html_block_type == 0 && raw_end == "" && !in_comment && fline ~ /^ {0,3}<\?/ {
        raw_end = "?>"
        if (fline ~ /\?>/) raw_end = ""
        next
    }
    html_block_type == 0 && raw_end == "" && !in_comment && fline ~ /^ {0,3}<!\[CDATA\[/ {
        raw_end = "]]>"
        if (index(fline, "]]>") > 0) raw_end = ""
        next
    }
    html_block_type == 0 && raw_end == "" && !in_comment && fline ~ /^ {0,3}<![A-Z]/ {
        raw_end = ">"
        if (index(fline, ">") > 1) raw_end = ""
        next
    }
    raw_end != "" {
        if (index($0, raw_end) > 0) raw_end = ""
        next
    }
    html_block_type == 0 && raw_end == "" && !in_comment && tolower(fline) ~ ("^ {0,3}</?(" HTML6_TAGS ")([ \t/>]|$)") {
        html_block_type = 6
        next
    }
    # CommonMark HTML block type 7: a line that is a single COMPLETE open or
    # closing tag (any name not already matched above) followed only by
    # whitespace opens a raw block that renders literally until a blank line
    # (review). Without it, "<x-widget>" then a fake "## Measurements" table on
    # the next line — no blank between — is raw HTML yet was being accepted.
    # lone_tag() finds the tag boundary respecting quoted attribute values, so
    # a ">" inside an attribute does not end the tag early and quoted prose
    # after a tag does not masquerade as a lone tag (review).
    # ALONE among the HTML block types, type 7 CANNOT interrupt a paragraph
    # (review): a lone tag on the line after paragraph text is inline content
    # continuing that paragraph, so a real "## Measurements" heading after it
    # DOES interrupt the paragraph and must be seen. prev_para != "" means the
    # previous line was paragraph text, so the tag does not open a raw block.
    html_block_type == 0 && raw_end == "" && !in_comment && prev_para == "" && lone_tag(fline) {
        html_block_type = 7
        next
    }
    # Type 1 closes only on ITS matching end tag; a blank line does NOT
    # close it (review). Type 6 closes on a blank line.
    html_block_type == 1 {
        if (tolower($0) ~ ("</" html1_tag ">")) html_block_type = 0
        next
    }
    html_block_type == 6 {
        if ($0 ~ /^[ \t]*$/) html_block_type = 0
        next
    }
    # Type 7 closes on a blank line, exactly like type 6.
    html_block_type == 7 {
        if ($0 ~ /^[ \t]*$/) html_block_type = 0
        next
    }
    # HTML comments render nothing. A line that BEGINS with "<!--" is a
    # CommonMark type-2 HTML block: the WHOLE line renders as raw HTML, so a
    # "## Measurements" after an inline-closed comment on that same line is NOT
    # a heading (review). Consume the whole line; stay in the comment only if it
    # does not close here.
    !in_comment && /^ {0,3}<!--/ {
        if ($0 ~ /-->/) next
        in_comment = 1
        next
    }
    # A COMPLETE inline comment elsewhere on the line is stripped so the row
    # around it still validates: a measurement row with a trailing
    # "<!-- note -->" is still a row, minus the comment. It is replaced with a
    # SPACE, not nothing (review): an inline comment is inline content and cannot
    # create block structure, so "#<!-- x --># Measurements" must not collapse to
    # the heading "## Measurements". A space keeps the two "#" tokens apart while
    # leaving pipe structure and cell boundaries intact.
    !in_comment { gsub(/<!--[^-]*(-[^-]+)*-->/, " ") }
    # A comment that OPENS mid-line without closing: strip from it to end of
    # line, keep the prefix for validation (a table row must not vanish because
    # it opened a comment, review), and enter the comment on the NEXT line via
    # pending_comment — set at the top of the next line so the in_comment guards
    # below do not swallow this line prefix.
    !in_comment && /<!--/ { sub(/<!--.*$/, ""); pending_comment = 1 }
    # The ENTIRE line that carries the closing "-->" is part of the raw
    # comment block (review): CommonMark does not reparse its suffix, so a
    # comment ending "-->## Measurements" does not yield a heading. Consume the
    # whole line rather than stripping up to the token and reparsing the rest.
    in_comment && /-->/ { in_comment = 0; next }
    in_comment { next }
    # A setext underline ends the section too (review): a paragraph line
    # followed by a line of "=" (level 1) or "-" (level 2) is a heading, and an
    # empty Measurements section followed by "Appendix\n====" must not let a
    # table under that appendix be read as the measurements table. The underline
    # only counts over a real text line (prev_para set); a bare "---" with no
    # text above it is a thematic break, not a heading. A valid pipe table never
    # produces a pure "=+"/"-+" line, so this cannot close a real table early.
    in_section && prev_para != "" && /^ {0,3}=+[ \t]*$/ { finalize_section(); in_section = 0; next }
    in_section && prev_para != "" && /^ {0,3}-+[ \t]*$/ { finalize_section(); in_section = 0; next }
    # EXACT heading TEXT, but every valid ATX SOURCE form of it (review):
    # "## Measurements" with a space or a tab after the hashes, an optional
    # closing "#" sequence ("## Measurements ##"), and trailing spaces are all
    # the heading "Measurements"; only longer text ("## MeasurementsX") is a
    # different section. Up to three leading spaces are allowed.
    # Entering a "## Measurements" section RESETS the table state (review): a
    # SECOND such section must be validated fresh — after finalizing the first,
    # so an incomplete first table is not discarded — or its header and
    # delimiter would be read as data rows under the column mapping of the first
    # table, and a second table renaming "Buffer depth" to "Notes" would pass.
    /^ {0,3}##[ \t]+Measurements([ \t]+#+)?[ \t]*$/ {
        finalize_section()
        in_section = 1
        header_seen = 0
        delimiter_seen = 0
        header_count = 0
        split("", header)
        next
    }
    # The section ends at the next heading of level 1 OR 2 (review): a later
    # "# Something" closes "## Measurements" just as a sibling "## " does. Match
    # CommonMark ATX indentation (0-3 spaces) and its whitespace rule — a # run
    # followed by a space, a tab, or end of line — or an indented heading would
    # slip past and leave a following table attributed to Measurements. Finalize
    # the closing section first so an incomplete table there is recorded.
    /^ {0,3}#{1,2}([ \t]|$)/ { finalize_section(); in_section = 0 }
    # An indented-code line (a tab, or 4+ leading spaces) is a code block, not
    # table structure (review): a header or delimiter indented four spaces
    # renders as code, so it must not satisfy the table gate. Once a HEADER has
    # been seen, indented code breaks the header-delimiter binding markdown
    # requires, so the table does not render — close the section so a later
    # separator is not accepted as its delimiter (review). Before any header,
    # indented code is just a code block the real table may follow, so only
    # skip it.
    in_section && indent_cols($0) >= 4 {
        if (header_seen) { finalize_section(); in_section = 0 }
        next
    }
    # NOTHING may stand between the header and its delimiter (review):
    # markdown binds them adjacently, so a blank or prose line there means
    # no rendered table — and the pipe-only predicate used to step straight
    # over the gap.
    in_section && header_seen && !delimiter_seen && !/\|/ {
        printf "claims lint: the delimiter row must immediately follow the header; markdown renders no table across the gap\n" > "/dev/stderr"
        failures++
        delimiter_seen = 1
        next
    }
    # THE TABLE ENDS AT THE FIRST GAP (review): a blank line or prose after
    # the delimiter terminates the markdown table, and a row pasted below
    # it renders as loose text, not a measurement — so once the table has
    # started, a non-pipe line closes it and later pipe-bearing lines are
    # no longer rows to check.
    in_section && delimiter_seen && !/\|/ { in_section = 0 }
    in_section && /\|/ {
        # ANY line carrying a pipe inside the section is a table row
        # (review): markdown permits rows without the outer pipes, and a
        # predicate keyed on a leading | waved a pipe-less row — and its
        # missing column — straight past the gate. Prose in this section
        # must simply not contain a pipe; a sentence that does will be
        # refused as a malformed row, which is the loud side to fail on.
        # ESCAPED PIPES ARE CELL CONTENT (review, twice): markdown spells a
        # literal pipe inside a cell as \|, and a raw split on | turned
        # "c6i \| x86" into two columns — shifting every later value right,
        # so a row missing its trailing Date could still fill all nine
        # checked positions. And the escape is judged by BACKSLASH-RUN
        # PARITY: a cell ending in a literal backslash emits \\ before the
        # real delimiter, so literal backslash pairs are parked FIRST, and
        # only a pipe behind the remaining odd backslash is an escaped one.
        # Both park on bytes no table cell carries and are restored per
        # cell after the split.
        row = $0
        # CRLF endings shed their carriage return HERE (review): left in
        # place, a trailing-pipe row grows a phantom \r cell — and an empty
        # trailing Date column then reads as present, carrying exactly the
        # unsubstantiated row this lint exists to refuse.
        sub(/\r$/, "", row)
        gsub(/\\\\/, "\035", row)
        gsub(/\\\|/, SUBSEP, row)
        n = split(row, cell, "|")
        # CLEARED per row (review): awk arrays persist across iterations,
        # and a short row — a missing trailing Date cell — would otherwise
        # inherit the previous rows values in exactly the positions it
        # failed to provide, reading as complete.
        split("", cells)
        # split leaves empty fields before the first and after the last pipe
        count = 0
        # Without outer pipes, split yields no empty first/last fields, so
        # the slice bounds follow the shape of the row itself rather than assuming
        # the outer-pipe form (review).
        first = (row ~ /^[ \t]*\|/) ? 2 : 1
        last = (row ~ /\|[ \t]*$/) ? n - 1 : n
        count = 0
        for (i = first; i <= last; i++) {
            count++
            gsub(SUBSEP, "|", cell[i])
            gsub(/\035/, "\\", cell[i])
            cells[count] = trim(cell[i])
        }
        if (!header_seen) {
            header_seen = 1
            # The input line the header sat on, so the delimiter can be
            # required on the very next line (review): a CONSUMED line between
            # them — a comment, a fence, an HTML block — advances NR without
            # tripping the non-pipe gap rule below, so adjacency is checked by
            # line number instead.
            header_nr = NR
            for (c = 1; c <= count; c++) header[c] = cells[c]
            # The header must carry every required column, or a rename would
            # quietly stop the per-row checks from meaning anything.
            for (r = 1; r <= required_count; r++) {
                found = 0
                for (c = 1; c <= count; c++)
                    if (header[c] == required[r]) { found = 1; break }
                if (!found) {
                    printf "claims lint: the measurements table header is missing the required column \"%s\"\n", required[r] > "/dev/stderr"
                    failures++
                }
            }
            header_count = count
            next
        }
        # THE DELIMITER ROW IS REQUIRED, EXACTLY ONCE, RIGHT AFTER THE
        # HEADER (review): without it markdown renders no table at all, so
        # a header-only section with the delimiter deleted was being
        # reported clean while the document showed prose. Each delimiter cell
        # is the GFM grammar — optional leading colon, ONE or more hyphens,
        # optional trailing colon (review): a "-", "--" or ":-:" cell is a
        # valid delimiter GitHub renders, so a three-hyphen minimum wrongly
        # rejected a correct table. Any later all-dash row is DATA and is
        # judged like any other row.
        if (header_seen && !delimiter_seen) {
            # The delimiter must be the line IMMEDIATELY after the header
            # (review): if a consumed line (a complete or multi-line comment, a
            # fence, an HTML block) sat between them, NR has advanced past
            # header_nr + 1 and markdown renders no table across the gap.
            if (NR != header_nr + 1) {
                printf "claims lint: the delimiter row must immediately follow the header; markdown renders no table across the gap\n" > "/dev/stderr"
                failures++
                delimiter_seen = 1
                in_section = 0
                next
            }
            all_sep = (count > 0)
            for (c = 1; c <= count; c++)
                if (cells[c] !~ /^:?-+:?$/) { all_sep = 0; break }
            if (!all_sep) {
                printf "claims lint: the row after the header must be the markdown delimiter (a colon-optional run of hyphens per cell); found a data row or a malformed delimiter\n" > "/dev/stderr"
                failures++
            } else if (count != header_count) {
                # Width matters (review): markdown only binds a delimiter of
                # the header width, so a one-cell or ten-cell separator
                # leaves no rendered table while reading as structure here.
                printf "claims lint: the delimiter has %d cells but the header has %d — markdown will not render this table\n", count, header_count > "/dev/stderr"
                failures++
            }
            delimiter_seen = 1
            next
        }
        rows++
        # A row with MORE cells than the header is refused by count: after
        # the escape handling above, a surplus cell means a malformed row —
        # an unescaped pipe in a value — and values silently shifted into
        # the wrong columns would be checked against the wrong names.
        if (count > header_count) {
            printf "claims lint: measurement row %d has %d cells but the header has %d — an unescaped | in a value?\n", rows, count, header_count > "/dev/stderr"
            failures++
            next
        }
        # A short row is judged against the FULL header: the cleared array
        # reads "" for every cell the row failed to provide, so a missing
        # trailing column is named like any other.
        for (c = 1; c <= header_count; c++) {
            for (r = 1; r <= required_count; r++) {
                if (header[c] == required[r] && cells[c] == "") {
                    printf "claims lint: measurement row %d is missing \"%s\"\n", rows, header[c] > "/dev/stderr"
                    failures++
                }
            }
        }
    }
    # Record THIS line as the next-line setext antecedent AND paragraph-open
    # signal, but only if it is genuine paragraph text: a blank line, a table
    # row (a pipe), a tab-led or 4+-space-indented line (indented code), a
    # LIST-ITEM line (review — a "---" after a list item is a thematic break,
    # not a setext heading over it), or an ATX HEADING is not paragraph text.
    # The ATX-heading exclusion matters twice (review): a heading leaves no open
    # paragraph, so a "---" after it is a thematic break rather than a setext
    # underline, AND a lone type-7 tag after it is NOT a paragraph continuation
    # and so DOES open a raw block. Leaving para_prev empty (the promotion
    # already cleared it) is what encodes "no open paragraph here."
    {
        if ($0 !~ /^[ \t]*$/ && $0 !~ /\|/ && indent_cols($0) < 4 \
                && $0 !~ /^ {0,3}([-*+]|[0-9]{1,9}[.)])[ \t]/ \
                && $0 !~ /^ {0,3}#{1,6}([ \t]|$)/ \
                && $0 !~ /^ {0,3}>/ \
                && !ruling_line($0))
            para_prev = $0
    }
    END {
        if (!header_seen) {
            printf "claims lint: %s has no measurements table under \"## Measurements\"\n", doc > "/dev/stderr"
            exit 1
        }
        if (!delimiter_seen) {
            printf "claims lint: the measurements table has no delimiter row, so it would not render as a table\n" > "/dev/stderr"
            exit 1
        }
        if (failures > 0) exit 1
        printf "claims lint: clean (%d measurement row(s))\n", rows
    }
' "$doc"

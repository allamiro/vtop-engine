"""MkDocs build hooks for the documentation site (mkdocs.yml `hooks:`).

The Markdown under docs/ is written to be read on GitHub first, and it stays
that way: these hooks adapt it at build time instead of forking it.

1. A link that leaves docs/ (`../README.md`, `../../crates/...`) or points at a
   page the site excludes resolves on GitHub but has no page on the site. It
   is rewritten to the file's GitHub URL, but only when the file exists in the
   repository: a link to a missing file is left alone, so `mkdocs build
   --strict` still fails on it.
2. GitHub alert blockquotes (`> [!NOTE]`) become Material admonitions.
3. `<!-- vtop:include PATH -->` inlines a repository file (CONTRIBUTING.md),
   with its relative links re-based, so the site and the repository share one
   source. `<!-- vtop:include PATH section="Heading" -->` inlines only the
   body of that `## Heading` section, its `###` headings raised to `##`, and
   turns each `<picture>` (a light and a dark image chosen by the reader's
   OS) into Material's `#only-light` / `#only-dark` image pair, which follows
   the site's own theme toggle instead.
4. Every page ends with a "Next:" link to the page after it in the nav, so the
   site reads as one path. It is added here rather than written into each
   file, so it follows the nav when the nav changes and GitHub readers of the
   deep documents never see it.
"""

from __future__ import annotations

import os
import posixpath
import re

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_FENCE = re.compile(r"^\s*(```|~~~)")
# Inline links and images: [text](target) / ![alt](target). The target stops
# at whitespace or ')' so a title ("...") after it is kept as written.
_INLINE = re.compile(r"(!?)\[((?:[^\[\]]|\[[^\]]*\])*)\]\(\s*<?([^)\s>]+)>?((?:\s+\"[^\"]*\")?)\s*\)")
_REFDEF = re.compile(r"^(\s{0,3}\[[^\]]+\]:\s*)(\S+)(.*)$")
_HTML_ATTR = re.compile(r"""\b(src|href)=(["'])([^"']+)\2""")
_ALERT = re.compile(r"^(\s*)>\s*\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*$")
_INCLUDE = re.compile(r'^<!--\s*vtop:include\s+(\S+)(?:\s+section="([^"]+)")?\s*-->\s*$')
_PICTURE = re.compile(
    r'<picture>\s*<source media="\(prefers-color-scheme: dark\)" srcset="([^"]+)">\s*'
    r'<img alt="([^"]*)" src="([^"]+)">\s*</picture>'
)


def _section(body: str, heading: str, rel: str) -> str:
    """The body of `## heading` in `body`, up to the next `## ` or `---`."""
    lines = body.split("\n")
    try:
        start = lines.index(f"## {heading}") + 1
    except ValueError:
        raise ValueError(f"vtop:include: {rel} has no '## {heading}' section") from None
    end = start
    while end < len(lines) and not lines[end].startswith("## ") and lines[end].strip() != "---":
        end += 1
    out = "\n".join(lines[start:end]).strip("\n")
    out = re.sub(r"^### ", "## ", out, flags=re.M)
    return _PICTURE.sub(
        lambda m: f"![{m.group(2)}]({m.group(3)}#only-light)\n![{m.group(2)}]({m.group(1)}#only-dark)",
        out,
    )

_ALERT_KIND = {
    "NOTE": "note",
    "TIP": "tip",
    "IMPORTANT": "info",
    "WARNING": "warning",
    "CAUTION": "danger",
}


def _is_external(target: str) -> bool:
    return (
        target.startswith(("#", "/", "mailto:", "data:"))
        or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target) is not None
    )


def _repo_url(config) -> str:
    return (config.get("repo_url") or "").rstrip("/")


def _github_url(config, repo_path: str, image: bool) -> str:
    base = _repo_url(config)
    if image:
        return base.replace("https://github.com/", "https://raw.githubusercontent.com/") + "/main/" + repo_path
    kind = "tree" if os.path.isdir(os.path.join(REPO_ROOT, repo_path)) else "blob"
    return f"{base}/{kind}/main/{repo_path}"


def _rewrite_target(target: str, page_dir: str, config, files, image: bool) -> str:
    """Return the link target the site should use for `target`."""
    if _is_external(target):
        return target
    path, sep, anchor = target.partition("#")
    if not path:
        return target
    docs_rel = posixpath.normpath(posixpath.join(page_dir, path))
    repo_path = posixpath.normpath(posixpath.join("docs", docs_rel))
    if not docs_rel.startswith("../"):
        f = files.get_file_from_path(docs_rel)
        if f is not None and not f.inclusion.is_excluded():
            return target  # a page or asset the site serves: MkDocs resolves it
    if repo_path.startswith("../") or not os.path.exists(os.path.join(REPO_ROOT, repo_path)):
        return target  # missing everywhere: leave it for --strict to report
    return _github_url(config, repo_path, image) + (sep + anchor if sep else "")


def _rewrite_links(markdown: str, page_dir: str, config, files) -> str:
    out = []
    in_fence = False
    for line in markdown.split("\n"):
        if _FENCE.match(line):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue

        def inline(m):
            target = _rewrite_target(m.group(3), page_dir, config, files, image=bool(m.group(1)))
            return f"{m.group(1)}[{m.group(2)}]({target}{m.group(4)})"

        line = _INLINE.sub(inline, line)
        ref = _REFDEF.match(line)
        if ref:
            line = ref.group(1) + _rewrite_target(ref.group(2), page_dir, config, files, False) + ref.group(3)
        line = _HTML_ATTR.sub(
            lambda m: f"{m.group(1)}={m.group(2)}"
            f"{_rewrite_target(m.group(3), page_dir, config, files, m.group(1) == 'src')}{m.group(2)}",
            line,
        )
        out.append(line)
    return "\n".join(out)


def _convert_alerts(markdown: str) -> str:
    lines = markdown.split("\n")
    out = []
    i = 0
    in_fence = False
    while i < len(lines):
        line = lines[i]
        if _FENCE.match(line):
            in_fence = not in_fence
        m = None if in_fence else _ALERT.match(line)
        if not m:
            out.append(line)
            i += 1
            continue
        indent = m.group(1)
        out.append(f"{indent}!!! {_ALERT_KIND[m.group(2)]}")
        out.append("")
        i += 1
        while i < len(lines) and lines[i].lstrip().startswith(">"):
            body = re.sub(r"^\s*>\s?", "", lines[i])
            out.append(f"{indent}    {body}" if body else "")
            i += 1
        out.append("")
    return "\n".join(out)


def _include(markdown: str, page_dir: str) -> str:
    out = []
    for line in markdown.split("\n"):
        m = _INCLUDE.match(line)
        if not m:
            out.append(line)
            continue
        rel = m.group(1)
        with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as fh:
            body = fh.read()
        if m.group(2):
            body = _section(body, m.group(2), rel)
        src_dir = posixpath.dirname(rel)
        # The included file's links are relative to ITS directory; re-base
        # them onto the including page so the pass above resolves them.
        page_repo_dir = posixpath.join("docs", page_dir)

        def rebase(target: str) -> str:
            if _is_external(target):
                return target
            path, sep, anchor = target.partition("#")
            if not path:
                return target
            repo_path = posixpath.normpath(posixpath.join(src_dir, path))
            return posixpath.relpath(repo_path, page_repo_dir or ".") + (sep + anchor if sep else "")

        body = _INLINE.sub(
            lambda m2: f"{m2.group(1)}[{m2.group(2)}]({rebase(m2.group(3))}{m2.group(4)})", body
        )
        out.append(body)
    return "\n".join(out)


def _title_of(nav_page) -> str:
    """The nav title, or the page's first H1 when the nav gives none (a
    section index page whose source has not been read yet)."""
    if nav_page.title:
        return nav_page.title
    with open(nav_page.file.abs_src_path, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("# "):
                return line[2:].strip()
    return nav_page.file.src_uri


def _next_link(page, page_dir: str) -> str:
    nxt = page.next_page
    if nxt is None or nxt.file is None:
        return ""
    target = posixpath.relpath(nxt.file.src_uri, page_dir or ".")
    return f"\n\n---\n\n**Next:** [{_title_of(nxt)}]({target})\n"


def on_page_markdown(markdown, page, config, files, **kwargs):
    page_dir = posixpath.dirname(page.file.src_uri)
    markdown = _include(markdown, page_dir)
    markdown = _convert_alerts(markdown)
    markdown = _rewrite_links(markdown, page_dir, config, files)
    return markdown.rstrip("\n") + "\n" + _next_link(page, page_dir)

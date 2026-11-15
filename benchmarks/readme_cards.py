"""The README's summary cards: four SVG pairs drawn from site_facts.py, never from typed numbers.

readme_charts.py calls `render()` after drawing its charts, so one command
redraws everything under docs/assets/readme/ and CI's drift check covers the
cards too:

- card-hero:       the commit rule, the prototype status and four measured tiles
- card-link-share: share of a lossy link carried, by uploads in flight (VTOP alone)
- card-throughput: produce throughput, Apache Kafka beside VTOP's Kafka gateway
- card-tested:     the Kafka, client and S3 versions the runs and tests used

Each card is drawn 600 units wide, with body text at 13 pt and up, for
`<img width="600">`: GitHub shrinks it on a phone, so nothing smaller than
the data and footer lines (11 pt) is drawn. Each card draws its own rounded
fill (colours.SURFACES card-light and card-dark), so its contrast does not
depend on the page behind it. Every colour is a colours.NEUTRALS grey or a
system's registered colour; text is never set in a system colour.

Text is laid out by measuring each string with the pinned font, so a longer
number wraps instead of overflowing; a value that cannot fit its tile even
alone on a line is refused. The image's description carries what its glyphs
say about provenance: the data files, and for a card drawn from a Kafka run,
that run's full footer (#600).
"""
from __future__ import annotations

import os
from typing import Any

import readme_charts
import site_facts

W = 600.0
PAD = 24.0
GAP = 16.0
RADIUS = 12.0
LEADING = 1.35
SANS = "DejaVu Sans"
MONO = "DejaVu Sans Mono"
BODY = 13.0
SMALL = 11.0


class Refused(site_facts.Refused):
    """A card cannot be drawn as specified; the message says why."""


# ---------------------------------------------------------------- measurement


def measure(text: str, size: float, weight: str = "normal", family: str = SANS) -> float:
    """Advance width of `text` in points at `size`, with the pinned font."""
    from matplotlib.font_manager import FontProperties
    from matplotlib.textpath import TextPath

    if not text:
        return 0.0
    path = TextPath((0, 0), text, size=size, prop=FontProperties(family=family, weight=weight))
    return float(path.get_extents().x1)


def wrap(text: str, size: float, width: float, weight: str = "normal", family: str = SANS) -> list[str]:
    """Greedy word wrap to `width` points; refuses a single word wider than the line."""
    lines: list[str] = []
    current = ""
    for word in text.split(" "):
        joined = f"{current} {word}" if current else word
        if measure(joined, size, weight, family) <= width:
            current = joined
            continue
        if not current:
            raise Refused(f"{word!r} is wider than {width:.0f} pt at {size} pt")
        lines.append(current)
        current = word
        if measure(current, size, weight, family) > width:
            raise Refused(f"{word!r} is wider than {width:.0f} pt at {size} pt")
    if current:
        lines.append(current)
    return lines


def block_height(lines: int, size: float, leading: float = LEADING) -> float:
    return lines * size * leading


# --------------------------------------------------------------------- canvas


class Canvas:
    """Draw operations in points, y downwards, replayed onto one matplotlib axes once the height is known."""

    def __init__(self, theme: dict[str, str]):
        self.theme = theme
        self.ops: list[Any] = []

    def text(self, x, top, lines, size, color, weight="normal", family=SANS, ha="left", leading=LEADING) -> float:
        """Lines of text whose first line box starts at `top`; returns the block's height."""
        lines = [lines] if isinstance(lines, str) else lines
        for index, line in enumerate(lines):
            baseline = top + size * leading * index + size * (leading - 1) / 2 + size * 0.8
            self.ops.append(("text", x, baseline, line, size, color, weight, family, ha))
        return block_height(len(lines), size, leading)

    def rect(self, x, y, w, h, fill="none", edge="none", radius=0.0, linewidth=1.0, dashed=False):
        self.ops.append(("rect", x, y, w, h, fill, edge, radius, linewidth, dashed))

    def bar(self, x, y, w, h, fill, end="right", radius=4.0):
        """A bar whose data end (`right` or `top`) is rounded and whose baseline end is square."""
        if w <= 0 or h <= 0:
            return
        r = min(radius, w / 2, h / 2)
        self.rect(x, y, w, h, fill=fill, radius=r)
        if end == "right":
            self.rect(x, y, min(r, w), h, fill=fill)
        else:
            self.rect(x, y + h - min(r, h), w, min(r, h), fill=fill)

    def line(self, x0, y0, x1, y1, color, linewidth=1.0, dashed=False):
        self.ops.append(("line", x0, y0, x1, y1, color, linewidth, dashed))

    def render(self, plt, height: float, description: str):
        from matplotlib.patches import FancyBboxPatch

        fig = plt.figure(figsize=(W / 72, height / 72), dpi=72)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_xlim(0, W)
        ax.set_ylim(height, 0)
        ax.axis("off")
        for order, op in enumerate(self.ops):
            kind = op[0]  # drawn in the order recorded: a later op sits over an earlier one
            if kind == "text":
                _k, x, y, s, size, color, weight, family, ha = op
                ax.text(x, y, s, fontsize=size, color=color, fontweight=weight, family=family, ha=ha,
                        va="baseline", zorder=order)
            elif kind == "rect":
                _k, x, y, w, h, fill, edge, radius, linewidth, dashed = op
                ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={radius}"
                                            if radius else "square,pad=0", facecolor=fill, edgecolor=edge,
                                            linewidth=linewidth if edge != "none" else 0,
                                            linestyle=(0, (4, 3)) if dashed else "-", zorder=order))
            elif kind == "line":
                _k, x0, y0, x1, y1, color, linewidth, dashed = op
                ax.plot([x0, x1], [y0, y1], color=color, linewidth=linewidth,
                        linestyle=(0, (3, 3)) if dashed else "-", solid_capstyle="butt", zorder=order)
        fig._vtop_footer = description  # readme_charts._save writes it into the SVG's description
        return fig


def _card(canvas: Canvas, height: float) -> None:
    """The card's own fill and outline, drawn first so everything sits on it."""
    canvas.ops.insert(0, ("rect", 0.5, 0.5, W - 1, height - 1, canvas.theme["card"], canvas.theme["border"],
                          RADIUS, 1.0, False))


def _title(canvas: Canvas, y: float, title: str, subtitle: str | None) -> float:
    y += canvas.text(PAD, y, wrap(title, 17, W - 2 * PAD, "bold"), 17, canvas.theme["ink"], "bold", leading=1.25)
    if subtitle:
        y += 4 + canvas.text(PAD, y + 4, wrap(subtitle, BODY, W - 2 * PAD), BODY, canvas.theme["ink2"])
    return y


def _source_line(canvas: Canvas, y: float, text: str) -> float:
    return canvas.text(PAD, y, wrap(text, SMALL, W - 2 * PAD), SMALL, canvas.theme["ink2"])


# ---------------------------------------------------------------------- cards


def hero(canvas: Canvas, facts: dict[str, Any]) -> tuple[float, str]:
    t = canvas.theme
    h = facts["hero"]
    inner = W - 2 * PAD
    y = PAD
    pill_w = measure(h["pill"], BODY, "bold") + 24
    canvas.rect(PAD, y, pill_w, 26, fill=t["tile"], edge=t["border"], radius=13)
    canvas.text(PAD + 12, y + 3.5, h["pill"], BODY, t["ink"], "bold")
    y += 26 + 16
    y += canvas.text(PAD, y, wrap(h["headline"], 24, inner, "bold"), 24, t["ink"], "bold", leading=1.2) + 12
    rule_lines = wrap(h["rule"], BODY, inner - 24, family=MONO)
    rule_h = block_height(len(rule_lines), BODY) + 16
    canvas.rect(PAD, y, inner, rule_h, fill=t["tile"], edge=t["border"], radius=6)
    canvas.text(PAD + 12, y + 8, rule_lines, BODY, t["ink"], family=MONO)
    y += rule_h + 24
    y += canvas.text(PAD, y, h["section"], 15, t["ink"], "bold") + 8

    col = (inner - GAP) / 2
    tiles = facts["tiles"]
    for row in range(0, len(tiles), 2):
        layouts = [_tile_layout(tile, col - 32) for tile in tiles[row:row + 2]]
        row_h = max(layout["height"] for layout in layouts) + 32
        for index, (tile, layout) in enumerate(zip(tiles[row:row + 2], layouts)):
            x = PAD + index * (col + GAP)
            if tile["tone"] == "positive":
                canvas.rect(x, y, col, row_h, fill=t["tile"], radius=8)
            else:
                canvas.rect(x, y, col, row_h, fill=t["card"], edge=t["ink2"], radius=8, linewidth=1.2)
            _tile(canvas, tile, layout, x + 16, y + 16, col - 32, y + row_h - 16)
        y += row_h + GAP
    y += canvas.text(PAD, y, wrap(h["caveat"], BODY, inner), BODY, t["ink2"])
    y += PAD
    paths = "; ".join(tile["source"]["path"] for tile in tiles)
    description = (f"Data: {paths}\n\n" + facts["footers"]["failover"] + "\n\n" + facts["footers"]["throughput"])
    return y, description


def _tile_layout(tile: dict[str, Any], width: float) -> dict[str, Any]:
    badge_w = measure(tile["badge"], SMALL, "bold") + 16 if tile["badge"] else 0.0
    label = wrap(tile["label"], BODY, width - (badge_w + 8 if badge_w else 0))
    value = _phrases(tile["value"], 26, width, "bold")
    detail = wrap(tile["detail"], BODY, width)
    condition = tile["condition"][0].upper() + tile["condition"][1:] + "."
    data = wrap(condition, SMALL, width) + wrap(f"Data: {tile['source']['text']}", SMALL, width)
    height = (block_height(len(label), BODY) + 6 + block_height(len(value), 26, 1.15) + 6
              + block_height(len(detail), BODY) + 12 + block_height(len(data), SMALL))
    return {"label": label, "value": value, "detail": detail, "data": data, "badge_w": badge_w, "height": height}


def _phrases(text: str, size: float, width: float, weight: str) -> list[str]:
    """A value on one line, or broken after its commas ("0 lost," / "0 duplicated"), never mid-phrase."""
    if measure(text, size, weight) <= width or ", " not in text:
        return wrap(text, size, width, weight)
    parts = text.split(", ")
    return [line for index, part in enumerate(parts)
            for line in wrap(part + ("," if index < len(parts) - 1 else ""), size, width, weight)]


def _tile(canvas: Canvas, tile, layout, x: float, y: float, width: float, bottom: float) -> None:
    t = canvas.theme
    if tile["badge"]:
        bx = x + width - layout["badge_w"]
        canvas.rect(bx, y, layout["badge_w"], 20, fill="none", edge=t["ink2"], radius=10)
        canvas.text(bx + 8, y + 2, tile["badge"], SMALL, t["ink"], "bold", leading=1.45)
    y += canvas.text(x, y, layout["label"], BODY, t["ink2"]) + 6
    y += canvas.text(x, y, layout["value"], 26, t["ink"], "bold", leading=1.15) + 6
    canvas.text(x, y, layout["detail"], BODY, t["ink2"])
    # The data line sits at the tile's foot, so the two tiles of a row end on one line.
    canvas.text(x, bottom - block_height(len(layout["data"]), SMALL), layout["data"], SMALL, t["ink2"])


def link_share(canvas: Canvas, facts: dict[str, Any]) -> tuple[float, str]:
    t = canvas.theme
    chart = facts["charts"]["link_share"]
    vtop = t["s2"]
    y = _title(canvas, PAD, chart["title"], chart["subtitle"]) + 20
    labels = [row["label"] for row in chart["rows"]] + [chart["default"]["label"]]
    label_w = max(measure(label, BODY) for label in labels) + 16
    value_w = max(measure(row["text"], BODY, "bold") for row in chart["rows"] + [chart["default"]]) + 10
    x0 = PAD + label_w
    span = W - PAD - x0 - value_w
    bar_h, row_h = 18.0, 30.0
    ref_x = x0 + span * chart["reference"]["fraction"]
    ref_label = chart["reference"]["label"]
    canvas.text(ref_x, y, ref_label, SMALL, t["ink2"], ha="right" if ref_x + measure(ref_label, SMALL) / 2 > W - PAD
                else "center")
    y += block_height(1, SMALL) + 4
    top = y
    rows_h = row_h * (len(chart["rows"]) + 1) + 12
    # The reference line goes down first, so bars and values sit over it.
    canvas.line(ref_x, top - 2, ref_x, top + rows_h, t["ink2"], dashed=True)

    def row(item, at):
        canvas.text(PAD, at + (row_h - block_height(1, BODY)) / 2, item["label"], BODY, t["ink"])
        width = span * item["fraction"]
        canvas.bar(x0, at + (row_h - bar_h) / 2, width, bar_h, vtop)
        # A knockout in the card's fill keeps the value clear of the reference line.
        text_w = measure(item["text"], BODY, "bold")
        canvas.rect(x0 + width + 3, at + (row_h - bar_h) / 2, text_w + 6, bar_h, fill=t["card"])
        canvas.text(x0 + width + 6, at + (row_h - block_height(1, BODY)) / 2, item["text"], BODY, t["ink"], "bold")

    for item in chart["rows"]:
        row(item, y)
        y += row_h
    y += 6
    canvas.line(PAD, y, W - PAD, y, t["grid"])
    y += 6
    row(chart["default"], y)
    y += row_h
    canvas.line(x0, top, x0, y, t["axis"])
    y += canvas.text(x0, y, wrap(chart["default"]["note"], SMALL, W - PAD - x0), SMALL, t["ink2"]) + 14
    y += _source_line(canvas, y, f"Data: {chart['source']['path']}")
    y += PAD
    return y, f"Data: {chart['source']['path']}"


def throughput(canvas: Canvas, facts: dict[str, Any]) -> tuple[float, str]:
    t = canvas.theme
    chart = facts["charts"]["throughput"]
    colour = {"kafka": t["s1"], "vtop": t["s2"]}
    y = _title(canvas, PAD, chart["title"], chart["subtitle"]) + 14
    x = PAD
    for system in ("kafka", "vtop"):
        canvas.rect(x, y + 3, 12, 12, fill=colour[system], radius=2)
        x += 18
        canvas.text(x, y, chart["series"][system], BODY, t["ink"])
        x += measure(chart["series"][system], BODY) + 20
    y += block_height(1, BODY) + 12
    plot_h = 190.0
    head = block_height(1, BODY) + 4  # room for the tallest bar's value
    base = y + head + plot_h
    groups = chart["groups"]
    slot = (W - 2 * PAD) / len(groups)
    bar_w, gap = 64.0, 2.0
    canvas.line(PAD, base, W - PAD, base, t["axis"])
    for index, group in enumerate(groups):
        centre = PAD + slot * (index + 0.5)
        for offset, system in ((-1, "kafka"), (1, "vtop")):
            cell = group[system]
            left = centre - bar_w - gap / 2 if offset < 0 else centre + gap / 2
            height = plot_h * cell["fraction"]
            canvas.bar(left, base - height, bar_w, height, colour[system], end="top")
            mid = left + bar_w / 2
            low, high = base - plot_h * cell["low_fraction"], base - plot_h * cell["high_fraction"]
            canvas.line(mid, low, mid, high, t["ink2"], linewidth=1.2)
            canvas.text(mid, min(high, base - height) - 4 - block_height(1, BODY), cell["text"], BODY, t["ink"],
                        "bold", ha="center")
        below = base + 8
        below += canvas.text(centre, below, group["label"], BODY, t["ink"], ha="center")
        canvas.text(centre, below, wrap(group["ratio_text"], BODY, slot - 16), BODY, t["ink2"], ha="center")
    y = base + 8 + block_height(1, BODY) + block_height(max(len(wrap(g["ratio_text"], BODY, slot - 16))
                                                         for g in groups), BODY) + 16
    text = f"Data: {chart['source']['path']}\n" + facts["footers"]["throughput"]
    for paragraph in text.split("\n"):
        y += _source_line(canvas, y, paragraph) + 2
    y += PAD - 2
    return y, text


def tested(canvas: Canvas, facts: dict[str, Any]) -> tuple[float, str]:
    t = canvas.theme
    data = facts["tested"]
    y = _title(canvas, PAD, "Tested against", "Versions as the benchmark runs, the Docker lab, CI and Cargo.lock "
               "record them.") + 18
    groups = (("Kafka brokers", data["kafka"]), ("Kafka clients", data["clients"]), ("S3 storage", data["s3"]))
    sources = []
    for name, items in groups:
        y += canvas.text(PAD, y, name, BODY, t["ink2"], "bold") + 8
        chips = [{"lines": [(f"{item['name']} {item['version']}", BODY, t["ink"], "bold"),
                            (item["ran"], SMALL, t["ink2"], "normal")], "dashed": False} for item in items]
        sources += [item.get("source", "") for item in items]
        if name == "S3 storage" and not data["s3_matrix"]["available"]:
            chips.append({"lines": [(data["s3_matrix"]["placeholder"], BODY, t["ink2"], "normal")], "dashed": True})
        y = _chips(canvas, chips, y) + 18
    y += PAD - 18
    return y, "Versions read from: " + ", ".join(sorted({site_facts.COMPOSE, site_facts.CI, site_facts.CARGO_LOCK,
                                                         readme_charts.THROUGHPUT_RUN + "/environment.json",
                                                         readme_charts.FAILOVER_RUN + "/environment.json",
                                                         *filter(None, sources)}))


def _chips(canvas: Canvas, chips, y: float) -> float:
    t = canvas.theme
    x, row_h = PAD, 0.0
    for chip in chips:
        width = max(measure(text, size, weight) for text, size, _c, weight in chip["lines"]) + 24
        if width > W - 2 * PAD:
            raise Refused(f"chip {chip['lines'][0][0]!r} is wider than the card")
        height = sum(block_height(1, size) for _t, size, _c, _w in chip["lines"]) + 16
        if x + width > W - PAD:
            x, y = PAD, y + row_h + 8
            row_h = 0.0
        canvas.rect(x, y, width, height, fill=t["card"] if chip["dashed"] else t["tile"],
                    edge=t["ink2"] if chip["dashed"] else t["border"], radius=8, dashed=chip["dashed"])
        top = y + 8
        for text, size, colour, weight in chip["lines"]:
            top += canvas.text(x + 12, top, text, size, colour, weight)
        x += width + 8
        row_h = max(row_h, height)
    return y + row_h


CARDS = (
    ("card-hero", hero),
    ("card-link-share", link_share),
    ("card-throughput", throughput),
    ("card-tested", tested),
)


def check_fonts() -> None:
    """Every face the cards draw with is matplotlib's own copy, as regenerate.check_font demands of DejaVu Sans."""
    import matplotlib
    from matplotlib import font_manager

    data = os.path.realpath(matplotlib.get_data_path())
    for family, weight in ((SANS, "bold"), (MONO, "normal")):
        path = font_manager.findfont(font_manager.FontProperties(family=family, weight=weight),
                                     fallback_to_default=False)
        if not os.path.realpath(path).startswith(data + os.sep):
            raise Refused(f"{family} {weight} resolves to {path}, not to matplotlib's bundled copy under {data}")


def render(repo: str = readme_charts.REPO) -> list[str]:
    check_fonts()
    facts = site_facts.site_facts(repo)
    out_dir = os.path.join(repo, readme_charts.OUT)
    os.makedirs(out_dir, exist_ok=True)
    written = []
    for name, draw in CARDS:
        for mode, theme in readme_charts.THEMES.items():
            plt = readme_charts._setup(theme)
            canvas = Canvas(theme)
            height, description = draw(canvas, facts)
            _card(canvas, height)
            fig = canvas.render(plt, height, description)
            written.append(readme_charts._save(fig, os.path.join(out_dir, f"{name}-{mode}.svg")))
            plt.close(fig)
    return written


def description_of(svg: bytes) -> str:
    """The text the SVG's description holds (what _save wrote), for tests and readers without a renderer."""
    import html
    import re

    match = re.search(rb"<dc:description>(.*?)</dc:description>", svg, re.S)
    return html.unescape(match.group(1).decode("utf-8")) if match else ""


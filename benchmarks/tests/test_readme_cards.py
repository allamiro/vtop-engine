"""readme_cards.py: the README's summary cards, and how README.md embeds them (#714).

The cards' numbers are site_facts.py's (tests/test_site_facts.py); these
tests hold the other links: the README embeds every card in both themes with
the alt text the data gives, the images carry their provenance, a colour
means one system, and a changed measurement redraws both SVGs. Rendering
needs matplotlib and runs in the benchmark-charts job, which installs the
pinned stack; the rest runs everywhere.
"""
import os
import re

import pytest

import readme_cards
import readme_charts
import site_facts
from kafka_compare import colours, footer

README = os.path.join(readme_charts.REPO, "README.md")
OUT = os.path.join(readme_charts.REPO, readme_charts.OUT)
PICTURE = re.compile(r'<picture>\s*<source media="\(prefers-color-scheme: dark\)" srcset="([^"]+)">\s*'
                     r'<img alt="([^"]*)" src="([^"]+)"( width="600")?>\s*</picture>')


def readme_text():
    with open(README, encoding="utf-8") as handle:
        return handle.read()


def svg(name):
    with open(os.path.join(OUT, f"{name}.svg"), "rb") as handle:
        return handle.read()


def pictures():
    return {re.search(r"readme/([a-z0-9-]+)-light\.svg", src).group(1): (dark, alt, src, width)
            for dark, alt, src, width in PICTURE.findall(readme_text())}


# ------------------------------------------------------- the README embeds them


def test_the_cards_are_the_ones_site_facts_writes_alt_text_for():
    assert tuple(name for name, _draw in readme_cards.CARDS) == site_facts.CARDS


@pytest.mark.parametrize("name", site_facts.CARDS)
def test_each_card_is_embedded_in_both_themes_with_the_alt_text_the_data_gives(name):
    found = pictures()
    assert name in found, f"README.md does not embed {name} as a light/dark <picture>"
    dark, alt, src, width = found[name]
    assert (dark, src) == (f"docs/assets/readme/{name}-dark.svg", f"docs/assets/readme/{name}-light.svg")
    assert width, f"{name} is embedded without width=600, so GitHub draws it at the SVG's own size"
    want = site_facts.site_facts()["alts"][name]
    assert alt == want, f"README.md's alt text for {name} is not what the data gives; replace it with:\n{want}"


def test_every_image_in_the_readme_has_alt_text():
    images = re.findall(r"<img\b[^>]*>", readme_text())
    assert images
    missing = [tag for tag in images if not re.search(r'\salt="[^"]+"', tag)]
    assert not missing, f"README.md images without alt text: {missing}"


def test_the_evidence_section_the_docs_inline_still_parses():
    # docs/benchmarks/index.md inlines "## Evidence" and rewrites its <picture>s with a strict pattern;
    # the cards sit outside it, so that section is unchanged in shape.
    evidence = readme_text().split("## Evidence\n", 1)[1].split("\n---\n", 1)[0]
    assert "card-" not in evidence
    assert len(PICTURE.findall(evidence)) == len(readme_charts.CHARTS)


# ------------------------------------------------------- the committed images


def test_every_card_is_committed_in_both_themes():
    for name in site_facts.CARDS:
        for mode in ("light", "dark"):
            assert os.path.isfile(os.path.join(OUT, f"{name}-{mode}.svg")), f"{name}-{mode}.svg is not committed"


def _colours(data):
    return {"#" + hex_.decode().lower() for hex_ in re.findall(rb"#([0-9a-fA-F]{6})\b", data)}


@pytest.mark.parametrize("name", [f"{card}-{mode}" for card in site_facts.CARDS for mode in ("light", "dark")])
def test_every_non_neutral_colour_on_a_card_is_a_system_colour(name):
    mode = name.rsplit("-", 1)[1]
    allowed = {colours.colour(system, mode) for system in colours.SYSTEM_SLOT}
    stray = sorted(c for c in _colours(svg(name)) if not colours.is_neutral(c) and c not in allowed)
    assert not stray, f"{name} draws colours outside the {mode} registry: {stray}"


VTOP_ONLY = ("egress-utilisation", "policed-uplink", "card-link-share")


@pytest.mark.parametrize("name", [f"{chart}-{mode}" for chart in VTOP_ONLY for mode in ("light", "dark")])
def test_a_chart_of_vtop_alone_draws_no_other_systems_colour(name):
    # One colour means one system in every image: Kafka's blue on a VTOP-only chart reads as Kafka.
    mode = name.rsplit("-", 1)[1]
    found = {c for c in _colours(svg(name)) if not colours.is_neutral(c)}
    assert found == {colours.colour("vtop", mode)}, f"{name} draws {sorted(found)}"


def test_the_hero_card_names_every_data_file_and_carries_both_runs_footers():
    facts = site_facts.site_facts()
    for mode in ("light", "dark"):
        text = readme_cards.description_of(svg(f"card-hero-{mode}"))
        for tile in facts["tiles"]:
            assert tile["source"]["path"] in text
        assert facts["footers"]["failover"] in text and facts["footers"]["throughput"] in text


def test_the_throughput_card_carries_its_runs_full_footer():
    env = readme_charts.throughput()["environment"]
    for mode in ("light", "dark"):
        text = readme_cards.description_of(svg(f"card-throughput-{mode}"))
        fields = footer.parse(text.split("\n", 1)[1])
        for label in footer.REQUIRED:
            assert fields.get(label), (mode, label)
        assert footer.footer_text(env) in text


# ------------------------------------------------------------------ rendering


def test_a_changed_measurement_redraws_both_svgs(data_tree):
    pytest.importorskip("matplotlib")
    from test_site_facts import a_lower_lossy_median, one_policed_object_failed

    same = data_tree(name="same")
    changed = data_tree({readme_charts.POLICED: one_policed_object_failed,
                         readme_charts.EGRESS: a_lower_lossy_median}, name="changed")
    readme_cards.render(same)
    readme_cards.render(changed)
    for name in ("card-hero", "card-link-share"):
        for mode in ("light", "dark"):
            rel = os.path.join(readme_charts.OUT, f"{name}-{mode}.svg")
            with open(os.path.join(same, rel), "rb") as one, open(os.path.join(changed, rel), "rb") as two:
                assert one.read() != two.read(), f"{name}-{mode} did not change with the data"
    # And an unchanged tree draws exactly what is committed (the drift check, in a test).
    for name in site_facts.CARDS:
        for mode in ("light", "dark"):
            with open(os.path.join(same, readme_charts.OUT, f"{name}-{mode}.svg"), "rb") as handle:
                assert handle.read() == svg(f"{name}-{mode}"), f"{name}-{mode} drifted from the committed SVG"


def test_the_hero_draws_the_prototype_pill_and_the_behind_badge():
    pytest.importorskip("matplotlib")
    facts = site_facts.site_facts()
    canvas = readme_cards.Canvas(readme_charts.THEMES["light"])
    readme_cards.hero(canvas, facts)
    drawn = [op[3] for op in canvas.ops if op[0] == "text"]
    assert facts["hero"]["pill"] in drawn and "Behind" in drawn
    assert any("Prototype" in text for text in drawn)


def test_no_text_is_drawn_under_eleven_points():
    # Embedded at width=600 and scaled down on a phone: body text is 13 pt, and only the data,
    # condition and footer lines go down to 11.
    pytest.importorskip("matplotlib")
    facts = site_facts.site_facts()
    for name, draw in readme_cards.CARDS:
        canvas = readme_cards.Canvas(readme_charts.THEMES["light"])
        draw(canvas, facts)
        small = [op[3] for op in canvas.ops if op[0] == "text" and op[4] < readme_cards.SMALL]
        assert not small, (name, small)


def test_every_drawn_line_fits_inside_the_card():
    pytest.importorskip("matplotlib")
    facts = site_facts.site_facts()
    for name, draw in readme_cards.CARDS:
        canvas = readme_cards.Canvas(readme_charts.THEMES["light"])
        draw(canvas, facts)
        for op in canvas.ops:
            if op[0] != "text":
                continue
            _k, x, _y, text, size, _c, weight, family, ha = op
            width = readme_cards.measure(text, size, weight, family)
            left = x - width / 2 if ha == "center" else x - width if ha == "right" else x
            assert readme_cards.PAD - 0.5 <= left and left + width <= readme_cards.W - readme_cards.PAD + 0.5, \
                (name, text)


def test_a_value_too_wide_for_any_line_is_refused():
    pytest.importorskip("matplotlib")
    with pytest.raises(readme_cards.Refused, match="wider than"):
        readme_cards.wrap("a " + "9" * 80, readme_cards.BODY, 200)

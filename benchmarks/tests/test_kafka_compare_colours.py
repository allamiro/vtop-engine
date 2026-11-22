"""One colour per system, in every chart, from one registry (#600).

The rule: one colour per system across every chart, colour-blind safe, the
same hue in light and dark mode, and Kafka and VTOP never told apart by red
against green. Before #600 it held by convention, with the hex pairs typed
into report.py and readme_charts.py separately; these tests hold it.
"""
import glob
import os
import re

import pytest

from kafka_compare import colours, failover_report, report

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS_SVGS = sorted(glob.glob(os.path.join(REPO, "docs", "benchmarks", "img", "*.svg")))
README_SVGS = sorted(glob.glob(os.path.join(REPO, "docs", "assets", "readme", "kafka-*.svg")))


# --------------------------------------------------------------- the registry


def test_the_registry_is_the_validated_pair():
    # Categorical slots 1 and 2 of the reference palette, light and dark steps.
    assert colours.colour("kafka", "light") == "#2a78d6"
    assert colours.colour("vtop", "light") == "#eb6834"
    assert colours.colour("kafka", "dark") == "#3987e5"
    assert colours.colour("vtop", "dark") == "#d95926"


@pytest.mark.parametrize("theme", sorted(colours.SURFACES))
def test_the_pair_passes_every_palette_check_on_its_surface(theme):
    """The validator's checks, in Python: lightness band, chroma floor, CVD separation,
    normal-vision distance, and 3:1 contrast against the surface the image is shown on."""
    mode = colours.surface_mode(theme)
    kafka, vtop = colours.colour("kafka", mode), colours.colour("vtop", mode)
    low, high = colours.LIGHTNESS_BAND[mode]
    for hex_ in (kafka, vtop):
        lightness, chroma, _hue = colours.oklch(hex_)
        assert low <= lightness <= high, (hex_, lightness)
        assert chroma >= colours.CHROMA_FLOOR, (hex_, chroma)
        assert colours.contrast(hex_, colours.SURFACES[theme]) >= 3.0, (hex_, theme)
    worst_cvd = min(colours.delta_e(kafka, vtop, kind) for kind in ("protan", "deutan", "tritan"))
    assert worst_cvd >= colours.CVD_TARGET
    assert colours.delta_e(kafka, vtop) >= colours.NORMAL_FLOOR


def test_the_python_checks_agree_with_the_reference_validator():
    # Figures printed by the dataviz validate_palette.js for the same pairs
    # (light on #fcfcfb, dark on #0d1117), to one decimal.
    assert round(colours.delta_e("#2a78d6", "#eb6834", "protan"), 1) == 24.7
    assert round(colours.delta_e("#2a78d6", "#eb6834"), 1) == 33.6
    assert round(colours.delta_e("#3987e5", "#d95926", "protan"), 1) == 26.8
    assert round(colours.delta_e("#3987e5", "#d95926"), 1) == 31.8


@pytest.mark.parametrize("mode", ("light", "dark"))
def test_no_system_colour_is_red_or_green(mode):
    for system in colours.SYSTEM_SLOT:
        hex_ = colours.colour(system, mode)
        assert not colours.is_red_or_green(hex_), f"{system} is drawn in {hex_}, a red or a green"


@pytest.mark.parametrize("hex_", ["#e34948", "#d03b3b", "#ff0000", "#ff4500", "#008300", "#0ca30c", "#27ae60",
                                  "#00a86b", "#1baf7a"])
def test_the_red_and_green_check_catches_a_red_or_a_green(hex_):
    # The mutation case: swap either system to one of these and the test above fails.
    assert colours.is_red_or_green(hex_)


def test_a_filtered_chart_does_not_repaint_the_survivors():
    both = colours.series(["kafka", "vtop"], "light")
    alone = colours.series(["vtop"], "light")
    assert alone["vtop"] == both["vtop"] == colours.colour("vtop", "light")


def test_a_system_beyond_the_validated_slots_is_refused():
    # Only the slots below pass every check in both modes; a third system needs its own
    # validated colour before it can be drawn (review of #600: slot 3's aqua is under 3:1 on the
    # light surfaces and close to blue for tritan vision in dark mode).
    registry = {**colours.SYSTEM_SLOT, "third": len(colours.SLOTS)}
    with pytest.raises(ValueError, match=f"at most {colours.MAX_SYSTEMS}"):
        colours.series(["kafka", "vtop", "third"], "light", registry)
    with pytest.raises(ValueError, match="slot"):
        colours.series(["third"], "light", registry)


@pytest.mark.parametrize("mode", ("light", "dark"))
def test_every_slot_pair_passes_every_check_in_both_modes(mode):
    # The docstring's claim, enforced: each slot clears the band, the floor and 3:1 on every surface
    # it is shown on, and every pair clears the CVD target (all three deficiencies) and the
    # normal-vision floor.
    surfaces = [hex_ for name, hex_ in colours.SURFACES.items() if colours.surface_mode(name) == mode]
    steps = [light if mode == "light" else dark for _name, light, dark in colours.SLOTS]
    low, high = colours.LIGHTNESS_BAND[mode]
    for hex_ in steps:
        lightness, chroma, _hue = colours.oklch(hex_)
        assert low <= lightness <= high and chroma >= colours.CHROMA_FLOOR, hex_
        for surface in surfaces:
            assert colours.contrast(hex_, surface) >= 3.0, (hex_, surface)
    for i, one in enumerate(steps):
        for two in steps[i + 1:]:
            for kind in ("protan", "deutan", "tritan"):
                assert colours.delta_e(one, two, kind) >= colours.CVD_TARGET, (one, two, kind)
            assert colours.delta_e(one, two) >= colours.NORMAL_FLOOR, (one, two)
    assert len(colours.SLOTS) == colours.MAX_SYSTEMS


@pytest.mark.parametrize("mode", ("light", "dark"))
def test_text_tokens_meet_aa_on_every_fill_they_sit_on(mode):
    # The README cards set text in ink or ink2 only, on the card's fill or a tile's; never in a
    # system colour, so 4.5:1 (WCAG AA body text) is the bar for every pair.
    neutrals = colours.NEUTRALS[mode]
    for text in colours.TEXT_TOKENS:
        for fill in colours.FILL_TOKENS:
            ratio = colours.contrast(neutrals[text], neutrals[fill])
            assert ratio >= 4.5, (mode, text, fill, round(ratio, 2))


@pytest.mark.parametrize("mode", ("light", "dark"))
def test_system_colours_are_marks_of_three_to_one_on_the_card_fills(mode):
    # A system colour is a mark beside a value printed in ink (the relief rule), so 3:1 on the fills it sits on.
    neutrals = colours.NEUTRALS[mode]
    for system in colours.SYSTEM_SLOT:
        for fill in colours.FILL_TOKENS:
            assert colours.contrast(colours.colour(system, mode), neutrals[fill]) >= 3.0, (mode, system, fill)


def test_every_neutral_is_a_grey_and_the_card_fills_are_registered_surfaces():
    for mode, neutrals in colours.NEUTRALS.items():
        for name, hex_ in neutrals.items():
            assert colours.is_neutral(hex_), (mode, name, hex_)
        assert neutrals["card"] == colours.SURFACES[f"card-{mode}"]


def test_the_chart_themes_are_the_registry_neutrals():
    import readme_charts

    for mode, theme in readme_charts.THEMES.items():
        assert {key: theme[key] for key in colours.NEUTRALS[mode]} == colours.NEUTRALS[mode]


def test_each_slot_keeps_its_hue_in_light_and_dark():
    for name, light, dark in colours.SLOTS:
        diff = abs(colours.oklch(light)[2] - colours.oklch(dark)[2])
        assert min(diff, 360 - diff) <= 3.0, (name, light, dark)


def test_a_system_without_a_slot_is_refused():
    with pytest.raises(ValueError, match="no colour slot"):
        colours.series(["kafka", "unknown"], "light")


def test_the_generators_draw_from_the_registry():
    for system in ("kafka", "vtop"):
        assert report.SYSTEM_STYLE[system]["color"] == colours.colour(system, "light")
        assert failover_report.STYLE[system]["color"] == colours.colour(system, "light")
    import readme_charts

    for mode, theme in readme_charts.THEMES.items():
        assert (theme["s1"], theme["s2"]) == (colours.colour("kafka", mode), colours.colour("vtop", mode))


# -------------------------------------------------------------- the images


def _fills(path):
    with open(path, encoding="utf-8") as handle:
        return {hex_.lower() for hex_ in re.findall(r"#[0-9a-fA-F]{6}\b", handle.read())}


def test_there_are_images_to_check():
    assert DOCS_SVGS and README_SVGS


@pytest.mark.parametrize("path", DOCS_SVGS + README_SVGS, ids=os.path.basename)
def test_every_non_neutral_colour_in_a_kafka_chart_is_a_system_colour(path):
    mode = "dark" if path.endswith("-dark.svg") else "light"
    allowed = {colours.colour(system, mode) for system in colours.SYSTEM_SLOT}
    stray = sorted(hex_ for hex_ in _fills(path) if not colours.is_neutral(hex_) and hex_ not in allowed)
    assert not stray, f"{os.path.basename(path)} draws colours outside the {mode} registry: {stray}"

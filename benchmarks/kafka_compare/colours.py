"""One colour per system in every Kafka-comparison chart (#600), and the checks that make it safe.

The systems take categorical slots of the reference palette in a FIXED order
(Kafka slot 1, blue; VTOP slot 2, orange), never by rank or by which series a
chart happens to draw, so a filtered chart never repaints a survivor. Each
slot has a light step and a dark step of the same hue: the docs pages show
the light step on their own light surface, and the README's light and dark
SVGs use one step each.

The colour science is the reference validator's (dataviz `validate_palette.js`),
written out so a test can run it: OKLab, Machado 2009 colour-vision-deficiency
simulation at full severity, distances in OKLab x100, WCAG contrast. Checks:
lightness band and chroma floor per mode, CVD separation >= 8 (the target),
normal-vision distance >= 15, and 3:1 against the surface the image is shown on.

Only the slots listed below are drawn, and the tests run every check on every
pair of them in both modes. The palette's third slot (aqua) is not among them:
its light step is under 3:1 on the light surfaces, and in dark mode it is close
to blue for tritan vision. A third system needs its own validated colour first.
"""
from __future__ import annotations

import math
from collections.abc import Iterable, Mapping

# (name, light step, dark step), in the palette's categorical order.
SLOTS = (
    ("blue", "#2a78d6", "#3987e5"),
    ("orange", "#eb6834", "#d95926"),
)
SYSTEM_SLOT: dict[str, int] = {"kafka": 0, "vtop": 1}
MAX_SYSTEMS = len(SLOTS)

# Where the images are shown: the docs pages draw their own light surface;
# the README's SVGs are transparent over GitHub's light or dark page.
SURFACES = {"docs": "#fcfcfb", "light": "#ffffff", "dark": "#0d1117"}

LIGHTNESS_BAND = {"light": (0.43, 0.77), "dark": (0.48, 0.67)}  # OKLCH L
CHROMA_FLOOR = 0.10  # OKLCH C
CVD_TARGET = 8.0  # OKLab x100
NORMAL_FLOOR = 15.0  # OKLab x100
NEUTRAL_CHROMA = 0.03  # below this an ink, grid or surface grey, not a series colour

# OKLCH hue bands (degrees) read as red and as green. The palette's red sits
# at 22-25, pure red at 29, CSS orangered at 35, its orange at 40; its greens at
# 142-152, jade at 159, aqua at 162.
RED_HUES = ((345.0, 360.0), (0.0, 37.0))
GREEN_HUES = ((110.0, 170.0),)


def colour(system: str, mode: str = "light", registry: Mapping[str, int] = SYSTEM_SLOT) -> str:
    """The system's colour in `mode` ("light" or "dark")."""
    if system not in registry:
        raise ValueError(f"system {system!r} has no colour slot; add it to colours.SYSTEM_SLOT")
    if not 0 <= registry[system] < len(SLOTS):
        raise ValueError(f"system {system!r} is registered to slot {registry[system] + 1}, but only "
                         f"{len(SLOTS)} slots are validated; validate a colour for it first")
    _name, light, dark = SLOTS[registry[system]]
    if mode not in ("light", "dark"):
        raise ValueError(f"mode is {mode!r}, not light or dark")
    return light if mode == "light" else dark


def series(systems: Iterable[str], mode: str = "light", registry: Mapping[str, int] = SYSTEM_SLOT) -> dict[str, str]:
    """{system: colour} for one chart; refuses more systems than the palette separates."""
    systems = list(systems)
    if len(systems) > MAX_SYSTEMS:
        raise ValueError(f"{len(systems)} systems in one chart; the palette separates at most {MAX_SYSTEMS}")
    return {system: colour(system, mode, registry) for system in systems}


# ------------------------------------------------------------- colour science


def _srgb(hex_: str) -> tuple[float, float, float]:
    text = hex_.strip().lstrip("#")
    if len(text) != 6:
        raise ValueError(f"{hex_!r} is not a #rrggbb colour")
    return tuple(int(text[i:i + 2], 16) / 255 for i in (0, 2, 4))  # type: ignore[return-value]


def _linear(hex_: str) -> tuple[float, float, float]:
    return tuple(c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in _srgb(hex_))  # type: ignore


def _oklab_from_linear(rgb) -> tuple[float, float, float]:
    r, g, b = rgb  # linear sRGB, each in 0..1, so the cube roots are real
    l_ = (0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b) ** (1 / 3)
    m_ = (0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b) ** (1 / 3)
    s_ = (0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b) ** (1 / 3)
    return (0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_,
            1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_,
            0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_)


def oklch(hex_: str) -> tuple[float, float, float]:
    """(lightness, chroma, hue in degrees)."""
    lightness, a, b = _oklab_from_linear(_linear(hex_))
    return lightness, math.hypot(a, b), math.degrees(math.atan2(b, a)) % 360


# Machado, Oliveira and Fernandes (2009), severity 1.0, on linear RGB.
MACHADO = {
    "protan": ((0.152286, 1.052583, -0.204868), (0.114503, 0.786281, 0.099216), (-0.003882, -0.048116, 1.051998)),
    "deutan": ((0.367322, 0.860646, -0.227968), (0.280085, 0.672501, 0.047413), (-0.011820, 0.042940, 0.968881)),
    "tritan": ((1.255528, -0.076749, -0.178779), (-0.078411, 0.930809, 0.147602), (0.004733, 0.691367, 0.303900)),
}


def _simulate(hex_: str, kind: str) -> tuple[float, float, float]:
    rgb = _linear(hex_)
    return tuple(min(1.0, max(0.0, sum(m * c for m, c in zip(row, rgb)))) for row in MACHADO[kind])  # type: ignore


def delta_e(one: str, two: str, kind: str | None = None) -> float:
    """OKLab distance x100, as seen with colour-vision deficiency `kind`, or with normal vision."""
    a = _oklab_from_linear(_simulate(one, kind) if kind else _linear(one))
    b = _oklab_from_linear(_simulate(two, kind) if kind else _linear(two))
    return 100 * math.dist(a, b)


def contrast(one: str, two: str) -> float:
    """WCAG 2 contrast ratio."""
    def luminance(hex_):
        r, g, b = _linear(hex_)
        return 0.2126 * r + 0.7152 * g + 0.0722 * b
    high, low = sorted((luminance(one), luminance(two)), reverse=True)
    return (high + 0.05) / (low + 0.05)


def is_neutral(hex_: str) -> bool:
    return oklch(hex_)[1] < NEUTRAL_CHROMA


def is_red_or_green(hex_: str) -> bool:
    _lightness, chroma, hue = oklch(hex_)
    if chroma < NEUTRAL_CHROMA:
        return False
    return any(low <= hue < high for low, high in RED_HUES + GREEN_HUES)

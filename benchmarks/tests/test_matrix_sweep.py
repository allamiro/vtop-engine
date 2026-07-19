"""The #90 sweep grid: a silent generator bug would surface as a
plausible-looking but incomplete matrix, so the grid itself is pinned."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from run_matrix import build_sweep, parse_compression, write_scenario  # noqa: E402


def test_parse_compression_with_and_without_level():
    assert parse_compression("gzip:6") == ("gzip", 6)
    assert parse_compression("zstd:9") == ("zstd", 9)
    assert parse_compression("none") == ("none", 0)


def test_sweep_is_the_full_cross_product_with_unique_names():
    cells = build_sweep(
        ["cef", "jsonl"], ["none", "gzip:6", "zstd:3"], ["small", "medium"], [10000], 50
    )
    assert len(cells) == 2 * 3 * 2 * 1
    names = [c["name"] for c in cells]
    assert len(set(names)) == len(names), "cell names must be unique (they name run dirs)"
    # Every dimension value must appear somewhere: an off-by-one in a loop
    # would drop a whole plane of the matrix.
    assert {c["format"] for c in cells} == {"cef", "jsonl"}
    assert {(c["compression"], c["compression_level"]) for c in cells} == {
        ("none", 0), ("gzip", 6), ("zstd", 3)
    }
    assert {c["file_size"] for c in cells} == {"small", "medium"}
    assert all(c["volume"] == 50 for c in cells)
    assert all(c["backend"] == "mock" for c in cells)


def test_written_scenario_round_trips_through_the_loader(tmp_path):
    lib_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"
    )
    sys.path.insert(0, os.path.dirname(lib_dir))
    from lib.scenario import load_scenario  # noqa: E402

    cell = build_sweep(["cef"], ["zstd:3"], ["small"], [10000], 5)[0]
    path = write_scenario(cell, str(tmp_path))
    loaded = load_scenario(path)
    assert loaded.get("format") == "cef"
    assert loaded.get("compression") == "zstd"
    assert int(loaded.get("compression_level")) == 3
    assert int(loaded.get("batch_max_records")) == 10000

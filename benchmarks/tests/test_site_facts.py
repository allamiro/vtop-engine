"""site_facts.py: the one source of every figure on the README's cards (#714).

The cards are worth something only if every number on them comes from a
committed measurement, and if each sentence on them stays true when a re-run
changes a number. These tests hold both, and need no matplotlib.
"""
import ast
import csv
import io
import json
import os
import re

import pytest

import readme_cards
import readme_charts
import site_facts

POLICED_ROW = "fix2-s24-r4,policed-uplink-default-width-20261111T061010Z-zo2cf5,16,0,16,"
LOSSY_DEFAULT = "wan100-loss1,default,,,confirm,5,9.0374,8.7473,9.1384,9.41,0.9666,"


def one_policed_object_failed(text):
    assert POLICED_ROW in text
    return text.replace(POLICED_ROW, POLICED_ROW.replace(",16,0,16,", ",15,1,16,"))


def a_lower_lossy_median(text):
    assert LOSSY_DEFAULT in text
    return text.replace(LOSSY_DEFAULT, LOSSY_DEFAULT.replace(",0.9666,", ",0.9555,"))


def tile(facts, tile_id):
    return next(t for t in facts["tiles"] if t["id"] == tile_id)


# ------------------------------------------------------------ the numbers flow


def test_the_vtop_failover_tile_is_vtops_own_tally():
    # "VTOP lost 0" must rest on VTOP's rows alone, not on a total that includes Kafka's.
    fo = readme_charts.failover()
    rows = [row for row in readme_charts.failover_schema.load_failover_run(
        os.path.join(readme_charts.REPO, readme_charts.FAILOVER_RUN)).rows if row["system"] == "vtop"]
    vtop = fo["per_system"]["vtop"]
    assert vtop["repetitions"] == len(rows)
    assert vtop["acked"] == sum(int(row["records_acked"]) for row in rows)
    assert fo["per_system"]["kafka"]["acked"] + vtop["acked"] == fo["acked"]
    assert tile(site_facts.site_facts(), "failover")["value"] == f"{vtop['lost']} lost, {vtop['duplicates']} duplicated"


def test_a_kafka_loss_does_not_reach_the_vtop_tile(data_tree):
    csv_path = f"{readme_charts.FAILOVER_RUN}/failover.csv"

    def kafka_lost_three(text):
        rows = list(csv.DictReader(io.StringIO(text)))
        next(row for row in rows if row["system"] == "kafka")["acked_lost"] = "3"
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        return out.getvalue()

    facts = site_facts.site_facts(data_tree({csv_path: kafka_lost_three}))
    assert tile(facts, "failover")["value"] == "0 lost, 0 duplicated"
    assert facts["results"][0]["figure"].startswith("3 lost")
    assert "was lost or duplicated by either system" not in facts["results"][0]["finding"]


def test_a_changed_count_changes_the_tile_the_alt_text_and_the_json(data_tree):
    before = site_facts.site_facts()
    after = site_facts.site_facts(data_tree({readme_charts.POLICED: one_policed_object_failed}))
    assert tile(before, "policed")["value"] == "96 of 96"
    assert tile(after, "policed")["value"] == "95 of 96"
    assert tile(after, "policed")["tone"] == "neutral"
    assert "95 of 96" in after["alts"]["card-hero"] and "95 of 96" not in before["alts"]["card-hero"]
    assert site_facts.to_json(after) != site_facts.to_json(before)
    assert "everything after" not in after["results"][2]["finding"]


def test_a_changed_median_changes_the_link_share_tile_and_chart(data_tree):
    after = site_facts.site_facts(data_tree({readme_charts.EGRESS: a_lower_lossy_median}))
    assert tile(after, "link-share")["value"] == "0.956"
    assert after["charts"]["link_share"]["default"]["text"] == "0.956"
    assert "0.956" in after["alts"]["card-hero"] and "0.956" in after["alts"]["card-link-share"]


def test_write_is_byte_identical_and_follows_the_data(tmp_path, data_tree):
    root = data_tree()
    first = open(site_facts.write("out/facts.json", root), encoding="utf-8").read()
    second = open(site_facts.write("out/facts.json", root), encoding="utf-8").read()
    assert first == second
    assert json.loads(first)["schema"] == site_facts.SCHEMA


def test_a_missing_measurement_is_refused(data_tree):
    with pytest.raises((site_facts.Refused, readme_charts.Refused)):
        site_facts.site_facts(data_tree({readme_charts.POLICED: None}))


# ------------------------------------------------------------------ honesty


def test_the_unflattering_number_and_the_status_cannot_be_dropped():
    facts = site_facts.site_facts()
    behind = [t for t in facts["tiles"] if t["tone"] == "neutral" and t["badge"] == "Behind"]
    assert behind, "no tile says where VTOP is behind"
    assert facts["prototype"] is True
    assert "Prototype" in facts["hero"]["pill"] and "Prototype" in facts["alts"]["card-hero"]
    assert facts["hero"]["caveat"].rstrip(".").lower() == readme_charts.footer.CAVEAT


def test_the_commit_rule_and_quick_start_are_the_readmes():
    with open(os.path.join(readme_charts.REPO, "README.md"), encoding="utf-8") as handle:
        readme = handle.read()
    facts = site_facts.site_facts()
    assert f"```text\n{facts['hero']['rule']}\n```" in readme
    quick = readme.split("## Quick start\n", 1)[1].split("\n## ", 1)[0]
    for lines in facts["quickstart"].values():
        assert "```bash\n" + "\n".join(lines) + "\n```" in quick
    # The quick start stays copyable text: no command is shown only as an image.
    assert "<picture>" not in quick and "<img" not in quick


def test_every_finding_is_built_from_the_numbers_it_states():
    facts = site_facts.site_facts()
    fo = readme_charts.failover()
    assert facts["results"][0]["figure"] == f"{fo['lost']} lost, {fo['duplicates']} duplicated, " \
                                            f"{fo['repetitions']} failovers"
    assert facts["results"][2]["finding"].endswith("committed nothing before #666 and everything after.")


# -------------------------------------------------------------- the S3 strip


def test_without_matrix_results_the_strip_says_when_more_servers_appear():
    s3 = site_facts.s3_servers()
    assert s3["lab"]["name"] == "MinIO" and s3["lab"]["version"].startswith("RELEASE.")
    assert s3["matrix"] == {"available": False, "entries": [],
                            "placeholder": "More S3 servers: when the matrix is published (#722)"}


def test_a_committed_matrix_result_joins_the_strip(data_tree):
    rel = f"{site_facts.S3_RESULTS}/garage.json"
    root = data_tree({rel: lambda _text: json.dumps({"server": "Garage", "version": "v2.1.0", "passed": 41})})
    facts = site_facts.site_facts(root)
    assert facts["tested"]["s3_matrix"]["available"] is True
    assert {"name": "Garage", "version": "v2.1.0", "ran": "S3 compatibility matrix", "source": rel} \
        in facts["tested"]["s3"]
    assert "Garage v2.1.0" in facts["alts"]["card-tested"]
    assert "when the matrix is published" not in facts["alts"]["card-tested"]


def test_a_matrix_result_that_names_no_server_is_refused(data_tree):
    root = data_tree({f"{site_facts.S3_RESULTS}/x.json": lambda _text: json.dumps({"version": "1"})})
    with pytest.raises(site_facts.Refused, match="name its server and version"):
        site_facts.site_facts(root)


def test_the_kafka_versions_are_the_runs_the_lab_s_and_ci_s():
    tested = site_facts.site_facts()["tested"]
    by_version = {item["version"]: item["ran"] for item in tested["kafka"]}
    run_tag = readme_charts.throughput()["environment"]["comparable"]["kafka_image"].split("@")[0].split(":")[1]
    assert "benchmarks" in by_version[run_tag]
    with open(os.path.join(readme_charts.REPO, "docker-compose.yml"), encoding="utf-8") as handle:
        lab = set(re.findall(r"image: apache/kafka:(\S+)", handle.read()))
    for tag in lab:
        assert "Docker lab" in by_version[tag]


# ---------------------------------------------------------------- alt text


def test_alt_text_holds_no_character_an_attribute_cannot_carry():
    for name, text in site_facts.site_facts()["alts"].items():
        assert text and not set(text) & set('"<>&'), name
    assert set(site_facts.site_facts()["alts"]) == set(site_facts.CARDS)


# ------------------------------------------------- no digits in the templates


# Strings in the generators that are not copy: matplotlib's box-style syntax.
NOT_COPY = {"round,pad=0,rounding_size=", "square,pad=0"}


def _copy_strings(path):
    """Every string literal in `path` that can reach a card or its alt text: plain strings and the
    literal parts of f-strings. Docstrings and f-string format specs (`.2f`) are not copy."""
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    skip = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                skip.add(id(first.value))
        if isinstance(node, ast.FormattedValue) and node.format_spec is not None:
            skip.update(id(part) for part in ast.walk(node.format_spec))
    return [(node.lineno, node.value) for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip]


@pytest.mark.parametrize("module", [site_facts, readme_cards], ids=lambda m: m.__name__)
def test_the_generators_type_no_number(module):
    # Every digit on a card comes from the data. What may hold a digit in a literal: an issue reference
    # (#666 names a change, not a measurement), a word that starts with a letter (a key such as ink2 or
    # wan100-loss1, a name such as S3), a file path, and a regex.
    typed = []
    for line, text in _copy_strings(module.__file__):
        if text in NOT_COPY or ("/" in text and " " not in text) or "\\d" in text:
            continue
        if re.search(r"\d", re.sub(r"\b[A-Za-z_][\w.-]*", "", re.sub(r"#\d+", "", text))):
            typed.append((line, text))
    assert not typed, f"{os.path.basename(module.__file__)} types numbers into its copy: {typed}"


def test_the_digit_check_catches_a_typed_number(tmp_path):
    # The mutation case: a typed figure, a typed condition and a typed version are each caught.
    path = tmp_path / "gen.py"
    path.write_text('X = f"{a} of {b}"\nY = "96 of 96"\nZ = f"lossy 100 ms {c:.2f}"\nV = "Prototype, 0.x"\n')
    found = [text for _line, text in _copy_strings(str(path))
             if re.search(r"\d", re.sub(r"\b[A-Za-z_][\w.-]*", "", re.sub(r"#\d+", "", text)))]
    assert sorted(found) == ["96 of 96", "Prototype, 0.x", "lossy 100 ms "]


def test_a_refusal_reaches_the_chart_command_as_a_refusal():
    # readme_charts.py's main() turns its own Refused into exit 2; the cards' refusals must be one.
    assert issubclass(site_facts.Refused, readme_charts.Refused)
    assert issubclass(readme_cards.Refused, readme_charts.Refused)


def test_a_fractional_link_rate_is_not_truncated(data_tree):
    root = data_tree({site_facts.EGRESS_ENV: lambda text: text.replace("tbf 10000 kbit/s", "tbf 1500 kbit/s")})
    assert site_facts.link_conditions(root)["rate_mbit"] == "1.5"
    assert site_facts.link_conditions()["rate_mbit"] == "10"

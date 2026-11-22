"""The footer every Kafka-comparison image carries, and the host disk a run records (#600).

#600: "Every image carries a footer: host CPU/RAM/disk, Kafka version, VTOP
commit, client and version, profile, repetitions, date." Before #600 there
were two copies of the footer, the README's charts carried none of it, and no
image named a disk. Glyphs are drawn as paths, so the footer is also written
into each image's metadata (SVG dc:description, PNG text chunk), where a test
and a screen reader can read it.
"""
import ast
import copy
import glob
import html
import json
import os
import re
import subprocess

import pytest
from test_kafka_compare_failover import environment as failover_environment
from test_kafka_compare_failover import rows_for as failover_rows
from test_kafka_compare_failover import write_run as write_failover_run
from test_kafka_compare_report import COMPARABLE, complete_rows, write_run
from test_kafka_compare_report import environment as throughput_environment

from kafka_compare import failover_report, footer, regenerate, report, run, schema
from kafka_compare import failover_schema as fschema

REPO = regenerate.REPO
DOCS_IMAGES = sorted(glob.glob(os.path.join(REPO, "docs", "benchmarks", "img", "*.svg")))
DOCS_PNGS = sorted(glob.glob(os.path.join(REPO, "docs", "benchmarks", "img", "*.png")))
README_IMAGES = sorted(glob.glob(os.path.join(REPO, "docs", "assets", "readme", "kafka-*.svg")))


def committed_environments():
    out = []
    for run_dir in regenerate.committed_runs():
        with open(os.path.join(run_dir, "environment.json")) as handle:
            out.append(json.load(handle))
    return out


# ------------------------------------------------------------------ the text


@pytest.mark.parametrize("env", committed_environments(), ids=lambda env: env["run_id"])
def test_the_footer_names_every_required_field_for_each_committed_run(env):
    fields = footer.parse(footer.footer_text(env))
    missing = [label for label in footer.REQUIRED if not fields.get(label)]
    assert not missing, f"the footer of run {env['run_id']} has no {missing}"
    assert fields["VTOP"].startswith(env["comparable"]["vtop_commit"][:12])
    assert fields["date"] == env["started_at"][:10]
    assert fields["repetitions"] == str(env["matrix"]["repetitions"])


def test_a_run_that_recorded_no_disk_says_so_and_names_the_volume():
    env = throughput_environment()
    assert "host_disk" not in env["comparable"]
    fields = footer.parse(footer.footer_text(env))
    assert fields["disk"] == "not recorded (the run predates the disk probe)"
    assert fields["Docker"].endswith("volumes /dev/vda1 ext4")


def test_a_recorded_disk_is_printed():
    env = throughput_environment()
    env["comparable"]["host_disk"] = "APPLE SSD AP4096Z, SSD, Apple Fabric, apfs"
    assert footer.parse(footer.footer_text(env))["disk"].startswith("APPLE SSD AP4096Z, SSD")


def test_a_failover_footer_names_both_clients_and_the_offered_rate():
    env = copy.deepcopy(failover_environment())
    env["comparable"] = {**COMPARABLE, **env["comparable"], "client_kafka": "jvm", "client_vtop": "native"}
    fields = footer.parse(footer.footer_text(env))
    assert fields["client"] == "jvm vs native"
    assert fields["offered"] == "100 records/s"


def test_the_footer_has_one_source():
    # report.py and failover_report.py each had their own copy; the README charts had none.
    assert not hasattr(report, "footer_text")
    assert not hasattr(failover_report, "footer_text")
    here = os.path.dirname(os.path.abspath(footer.__file__))
    for path in (os.path.join(here, "report.py"), os.path.join(here, "failover_report.py"),
                 os.path.join(os.path.dirname(here), "readme_charts.py")):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        defined = [node.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef)]
        assert "footer_text" not in defined, f"{path} defines its own footer_text"
        assert "footer.footer_text(" in source, f"{path} draws no footer from footer.py"


# ---------------------------------------------------------------- the images


def svg_footer(path):
    with open(path, encoding="utf-8") as handle:
        found = re.search(r"<dc:description>(.*?)</dc:description>", handle.read(), re.S)
    return html.unescape(found.group(1)) if found else ""


def test_there_are_images_to_check():
    assert DOCS_IMAGES and DOCS_PNGS and README_IMAGES


@pytest.mark.parametrize("path", DOCS_IMAGES + README_IMAGES, ids=os.path.basename)
def test_every_kafka_svg_carries_every_required_footer_field(path):
    fields = footer.parse(svg_footer(path))
    missing = [label for label in footer.REQUIRED if not fields.get(label)]
    assert not missing, f"{os.path.relpath(path, REPO)} carries no {missing} in its dc:description"


@pytest.mark.parametrize("path", DOCS_PNGS, ids=os.path.basename)
def test_every_kafka_png_carries_every_required_footer_field(path):
    image_mod = pytest.importorskip("PIL.Image")
    with image_mod.open(path) as image:
        text = image.info.get("Description", "")
    fields = footer.parse(text)
    missing = [label for label in footer.REQUIRED if not fields.get(label)]
    assert not missing, f"{os.path.relpath(path, REPO)} carries no {missing} in its Description text chunk"


@pytest.mark.parametrize("path", DOCS_IMAGES + README_IMAGES, ids=os.path.basename)
def test_the_footer_in_the_metadata_is_the_run_the_image_is_drawn_from(path):
    # The raw text, not a parse of it (a parse could drop the same thing on both sides), and the
    # run the chart is drawn from, not any committed run.
    name = os.path.basename(path)
    kind = "failover" if name.startswith("kafka-failover") else "throughput"
    env = next(env for env in committed_environments() if env.get("kind", "throughput") == kind)
    assert footer.footer_text(env) in svg_footer(path), f"{name} does not carry the footer of run {env['run_id']}"


def test_a_value_holding_the_separator_is_refused():
    env = throughput_environment()
    env["comparable"]["host_disk"] = "Foo | bar: baz"
    with pytest.raises(ValueError, match="separator"):
        footer.footer_text(env)


def test_a_label_that_appears_twice_is_refused():
    with pytest.raises(ValueError, match="twice"):
        footer.parse("host: a | disk: b\ndisk: c")


def test_the_footer_wraps_between_fields():
    text = footer.footer_text(committed_environments()[0])
    for width in (150, 190, 230):
        for line in footer.wrap(text, width).split("\n"):
            assert len(line) <= width + len(footer.SEPARATOR), (width, line)
            assert ": " in line or line.strip() == footer.CAVEAT, f"a line with no field: {line!r}"


# --------------------------------------------------- recording the host disk


def test_a_new_environment_records_the_host_disk_under_the_current_schema(monkeypatch):
    def fake_sh(args, **_kwargs):
        if args[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(args, 0, '{"NCPU": 8}', "")
        if args[:2] == ["docker", "version"]:
            return subprocess.CompletedProcess(args, 0, '{"Server": {}}', "")
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0, '[{"Id": "sha256:00"}]', "")
        if args[0] == "git":
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "Filesystem Type\n/dev/vda1 ext4 1 1 1 1% /probe\n", "")

    monkeypatch.setattr(run, "sh", fake_sh)
    env = run.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"}, "Test SSD, SSD, NVMe, ext4")
    assert env["schema_version"] == schema.SCHEMA_VERSION == 3
    assert env["kind"] == "throughput"
    assert env["comparable"]["host_disk"] == "Test SSD, SSD, NVMe, ext4"


DISKUTIL = {
    "/": {"MediaName": "", "SolidState": True, "BusProtocol": "Apple Fabric", "FilesystemType": "apfs",
          "APFSPhysicalStores": [{"APFSPhysicalStore": "disk0s2"}], "ParentWholeDisk": "disk3"},
    "disk0s2": {"MediaName": "", "ParentWholeDisk": "disk0"},
    "disk0": {"MediaName": "APPLE SSD AP4096Z", "SolidState": True, "BusProtocol": "Apple Fabric"},
}


def test_the_macos_disk_is_the_physical_store_under_the_volume():
    assert run.describe_macos_disk("/", DISKUTIL.__getitem__) == "APPLE SSD AP4096Z, SSD, Apple Fabric, apfs"


def test_a_macos_disk_diskutil_cannot_name_is_refused_with_the_reason():
    with pytest.raises(run.HostDiskUnknown, match="diskutil failed"):
        run.describe_macos_disk("/", lambda _device: (_ for _ in ()).throw(RuntimeError("diskutil failed")))
    blank = {"/": {"APFSPhysicalStores": [{"APFSPhysicalStore": "disk0s2"}]}, "disk0s2": {}}
    with pytest.raises(run.HostDiskUnknown, match="model"):
        run.describe_macos_disk("/", blank.__getitem__)


def lsblk_of(table):
    return lambda device: table[device]


def test_the_linux_disk_is_the_disk_under_the_mount_however_deep():
    # LVM on LUKS on a partition: the walk goes to the TYPE="disk" node, not one level up.
    table = {"/dev/mapper/vg-root": (
        'NAME="vg-root" TYPE="lvm" MODEL="" ROTA="0" TRAN=""\n'
        'NAME="luks" TYPE="crypt" MODEL="" ROTA="0" TRAN=""\n'
        'NAME="nvme0n1p3" TYPE="part" MODEL="" ROTA="0" TRAN="nvme"\n'
        'NAME="nvme0n1" TYPE="disk" MODEL="Samsung SSD 990 PRO 2TB" ROTA="0" TRAN="nvme"\n')}
    described = run.describe_linux_disk("/dev/mapper/vg-root ext4\n", lsblk_of(table))
    assert described == "Samsung SSD 990 PRO 2TB, non-rotational (kernel flag), nvme, ext4"


def test_a_btrfs_subvolume_source_is_read_as_its_device():
    table = {"/dev/nvme0n1p3": 'NAME="nvme0n1p3" TYPE="part" MODEL="" ROTA="0" TRAN="nvme"\n'
                               'NAME="nvme0n1" TYPE="disk" MODEL="WD Black SN850X" ROTA="0" TRAN="nvme"\n'}
    described = run.describe_linux_disk("/dev/nvme0n1p3[/root] btrfs\n", lsblk_of(table))
    assert described == "WD Black SN850X, non-rotational (kernel flag), nvme, btrfs"


def test_a_rotational_linux_disk_says_what_the_kernel_says():
    # virtio disks report rotational; the footer repeats the kernel's flag rather than guess SSD or HDD.
    table = {"/dev/vda1": 'NAME="vda1" TYPE="part" MODEL="" ROTA="1" TRAN=""\n'
                          'NAME="vda" TYPE="disk" MODEL="" ROTA="1" TRAN=""\n'}
    with pytest.raises(run.HostDiskUnknown, match="no model"):
        run.describe_linux_disk("/dev/vda1 xfs\n", lsblk_of(table))
    table["/dev/vda1"] = table["/dev/vda1"].replace('MODEL=""', 'MODEL="QEMU HARDDISK"')
    described = run.describe_linux_disk("/dev/vda1 xfs\n", lsblk_of(table))
    assert described == "QEMU HARDDISK, rotational (kernel flag), unknown bus, xfs"


def test_a_linux_disk_that_cannot_be_read_is_refused_with_the_reason():
    def broken(_device):
        raise RuntimeError("lsblk: not found")
    with pytest.raises(run.HostDiskUnknown, match="lsblk: not found"):
        run.describe_linux_disk("/dev/sda1 xfs\n", broken)
    with pytest.raises(run.HostDiskUnknown, match="findmnt"):
        run.describe_linux_disk("", broken)


@pytest.mark.parametrize("module_name", ["run", "failover"])
def test_a_run_whose_disk_cannot_be_read_is_refused_before_any_docker_work(monkeypatch, tmp_path, module_name):
    import importlib

    module = importlib.import_module(f"kafka_compare.{module_name}")
    calls = []

    def fake_sh(args, **_kwargs):
        calls.append(args)
        if args[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(args, 0, '{"DockerRootDir": "/var/lib/docker"}', "")
        if args[:2] == ["docker", "ps"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(f"ran {args} before refusing")

    def no_disk(_root):
        raise run.HostDiskUnknown("diskutil: no such disk")

    monkeypatch.setattr(run, "sh", fake_sh)
    monkeypatch.setattr(module, "sh", fake_sh)
    monkeypatch.setattr(run, "host_disk", no_disk)
    monkeypatch.setattr(module, "HERE", str(tmp_path))
    monkeypatch.setattr(module, "check_equal_limits", lambda: {"cpus": "2", "mem_limit": "2g"})
    with pytest.raises(SystemExit, match="diskutil: no such disk"):
        module.main(["--run-id", "probe-test"])
    assert not (tmp_path / "results").exists(), "a refused run left a run directory behind"
    assert not [args for args in calls if args[:2] in (["docker", "pull"], ["docker", "build"])]


# ------------------------------------------------- the schema refuses without it


def _throughput_run(tmp_path, mutate):
    env = throughput_environment()
    mutate(env)
    return write_run(str(tmp_path / "run"), env, complete_rows(env))


def _failover_run(tmp_path, mutate):
    env = failover_environment()
    mutate(env)
    rows, timeline = failover_rows(env)
    return write_failover_run(tmp_path / "fo", env, rows, timeline)


LOADERS = {"throughput": (_throughput_run, schema.load_run), "failover": (_failover_run, fschema.load_failover_run)}


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_schema_2_run_without_a_host_disk_is_refused(tmp_path, kind):
    build, load = LOADERS[kind]
    path = build(tmp_path, lambda env: env.update(schema_version=2))
    with pytest.raises(schema.ReportRefused, match="host_disk"):
        load(path)


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_schema_2_run_with_an_empty_host_disk_is_refused(tmp_path, kind):
    build, load = LOADERS[kind]

    def mutate(env):
        env["schema_version"] = 2
        env["comparable"]["host_disk"] = ""
    with pytest.raises(schema.ReportRefused, match="host_disk"):
        load(build(tmp_path, mutate))


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_schema_2_run_with_a_host_disk_loads(tmp_path, kind):
    build, load = LOADERS[kind]

    def mutate(env):
        env["schema_version"] = 2
        env["comparable"]["host_disk"] = "Test SSD, SSD, NVMe, ext4"
    load(build(tmp_path, mutate))


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_run_from_a_newer_harness_is_refused(tmp_path, kind):
    build, load = LOADERS[kind]
    with pytest.raises(schema.ReportRefused, match="schema_version 4"):
        load(build(tmp_path, lambda env: env.update(schema_version=4)))


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_run_from_before_schema_2_still_loads(tmp_path, kind):
    # The committed runs are schema 1 (no schema_version key) and raw data is never edited.
    build, load = LOADERS[kind]
    load(build(tmp_path, lambda env: None))


@pytest.mark.parametrize("kind", sorted(LOADERS))
@pytest.mark.parametrize("version", [2.0, "2", True, 0, -1, None])
def test_a_malformed_schema_version_is_called_malformed_not_newer(tmp_path, kind, version):
    build, load = LOADERS[kind]
    with pytest.raises(schema.ReportRefused, match="malformed"):
        load(build(tmp_path, lambda env: env.update(schema_version=version)))


@pytest.mark.parametrize("kind", sorted(LOADERS))
def test_a_newer_run_that_did_not_finish_is_called_newer(tmp_path, kind):
    # The version is read first: an unfinished run from a newer harness is a newer run.
    build, load = LOADERS[kind]
    with pytest.raises(schema.ReportRefused, match="newer harness"):
        load(build(tmp_path, lambda env: env.update(schema_version=4, run_state="running")))

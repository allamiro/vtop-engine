"""The soak's lab: the compose project, and every call the driver makes into it (#726).

The lab is `docker-compose.soak.yml`: a single-node KRaft Kafka, the lab's own
MinIO, a toxiproxy that stands between the engine and both of them, a
producer that generates the load, and the engine. The driver is a host
process that reaches into the containers with `docker exec`; it publishes one
port (toxiproxy's admin API, on loopback) and nothing else.

Everything that runs a command goes through one `runner`, so the unit tests
substitute a recorder and no test needs Docker. The parsers are plain
functions for the same reason.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

SOAK_DIR = Path(__file__).resolve().parent
REPO_ROOT = SOAK_DIR.parents[1]
COMPOSE_FILE = SOAK_DIR / "docker-compose.soak.yml"
SKEW_SCENARIO = REPO_ROOT / "scripts" / "live-chaos" / "scenarios" / "07-clock-skew.sh"

TOPIC = "soak-topic"
GROUP = "vtop-soak"
BUCKET = "soak-archive"
KAFKA_TOOLS = "/opt/kafka/bin"
# The ports toxiproxy listens on for the engine (docker-compose.soak.yml and
# toxiproxy.json), by the name of the proxy.
PROXY_PORTS = {"kafka": 19094, "minio": 19000}
CONTAINER_CONFIG = "/soak/config/config.yaml"
CONTAINER_CTL = "/soak/ctl"
SHIM_DIR_IN_IMAGE = "/opt/soak/shims"


class LabError(RuntimeError):
    """A lab call that failed, saying which and what it printed."""


@dataclass
class Result:
    code: int
    out: str
    err: str

    @property
    def ok(self) -> bool:
        return self.code == 0

    def text(self) -> str:
        return (self.out + ("\n" + self.err if self.err else "")).strip()


Runner = Callable[[list, Optional[float]], Result]


def subprocess_runner(argv: list, timeout: float | None = None) -> Result:
    """Run a command and capture it; a command that outlives its timeout is
    killed and reported as exit 124, like timeout(1)."""
    try:
        done = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        out = error.stdout.decode() if isinstance(error.stdout, bytes) else (error.stdout or "")
        return Result(124, out, f"timed out after {timeout}s: {' '.join(map(str, argv))}")
    except FileNotFoundError as error:
        return Result(127, "", str(error))
    return Result(done.returncode, done.stdout, done.stderr)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_probe(text: str) -> dict:
    """The engine probe's `key value` lines. Raises ValueError when the engine
    process is not the one the probe measured (PID 1 must be vtopctl, or the
    resident size is some wrapper's)."""
    fields: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            fields[parts[0]] = parts[1].strip()
    if fields.get("comm") != "vtopctl":
        raise ValueError(f"PID 1 in the engine container is '{fields.get('comm')}', not vtopctl")
    try:
        return {
            "rss_bytes": int(fields["rss_kb"]) * 1024,
            "open_fds": int(fields["fds"]),
            "work_dir_bytes": int(fields["work_kb"]) * 1024,
            "ledger_bytes": int(fields["ledger_bytes"]),
        }
    except (KeyError, ValueError) as error:
        raise ValueError(f"engine probe output unreadable ({error}): {text[:200]!r}") from None


DESCRIBE_ROW = re.compile(r"^(?P<group>\S+)\s+(?P<topic>\S+)\s+(?P<partition>\d+)\s+(?P<current>\S+)\s+(?P<end>\S+)\s+")
DESCRIBE_HEADER = re.compile(r"^GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\s+LOG-END-OFFSET")


def parse_describe(text: str, group: str, topic: str) -> dict[int, dict]:
    """The group's committed offset and the log end per partition, from
    `kafka-consumer-groups.sh --describe`. The tool exits 0 even when its query
    failed, so a body without the offsets table is an error, not an empty
    answer; a partition the group has no commit for reads as committed None."""
    rows: dict[int, dict] = {}
    header = False
    for line in text.splitlines():
        if DESCRIBE_HEADER.match(line):
            header = True
            continue
        match = DESCRIBE_ROW.match(line)
        if not (header and match and match["group"] == group and match["topic"] == topic):
            continue

        def number(token: str) -> int | None:
            return int(token) if token.isdigit() else None

        rows[int(match["partition"])] = {"committed": number(match["current"]), "end": number(match["end"])}
    if not header:
        shown = " | ".join(line for line in text.splitlines() if line.strip())[:300] or "(no output)"
        raise ValueError(f"no offsets table for group {group} in the describe output: {shown}")
    return rows


def lag_totals(partitions: dict[int, dict], expected_partitions: int) -> dict:
    """Topic end and group committed offset, summed. Raises ValueError unless
    every expected partition reported an end, since a sum over a missing
    partition would read as a smaller topic."""
    if sorted(partitions) != list(range(expected_partitions)) or any(p["end"] is None for p in partitions.values()):
        raise ValueError(f"expected {expected_partitions} partitions with an end offset, got {partitions}")
    committed = sum(p["committed"] or 0 for p in partitions.values())
    return {"end": sum(p["end"] for p in partitions.values()), "committed": committed}


def parse_json_report(text: str) -> dict:
    """The JSON object a `vtopctl --json` report prints. The report may follow
    log lines on the same stream, so the object is found by its first line
    that is a bare `{`, or by the whole text."""
    candidates = [text.strip()]
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "{":
            candidates.append("\n".join(lines[index:]))
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ValueError(f"no JSON report in the output: {text[-300:]!r}")


def scrub_summary(report: dict) -> dict:
    findings = report.get("findings") or []
    return {
        "report_mode": report.get("mode"),
        "batches_checked": report.get("checked"),
        "committed_rows": report.get("committed_rows"),
        "findings": len(findings),
        "passed": bool(report.get("passed")),
    }


GAP_LINE = re.compile(r"^\s+gap \[(\d+), (\d+)\):")


def audit_gaps(text: str) -> list[str]:
    """The stretches `audit-coverage` reports as archived by nothing, as the
    lines it printed them on. For a Kafka partition it prints them without
    failing, because a production topic can have gaps that compaction or
    transaction markers explain; the soak's producer is neither compacting nor
    transactional, so for it a gap is a hole and the exit status is not enough."""
    return [line.strip() for line in text.splitlines() if GAP_LINE.match(line)]


AUDIT_UNIT = re.compile(
    r"^\S+\s+kafka (?P<topic>.+)\[(?P<partition>\d+)\] group .+: \d+ committed range\(s\) "
    r"over \[(?P<start>\d+), (?P<end>\d+)\)"
)


def audit_ranges(text: str, topic: str) -> dict[int, tuple[int, int]]:
    """The span `audit-coverage` judged for each partition of the topic, from
    its `over [start, end)` field."""
    spans: dict[int, tuple[int, int]] = {}
    for line in text.splitlines():
        match = AUDIT_UNIT.match(line)
        if match and match["topic"] == topic:
            spans[int(match["partition"])] = (int(match["start"]), int(match["end"]))
    return spans


def coverage_problems(spans: dict[int, tuple[int, int]], partitions: int, ends: dict[int, int] | None) -> list[str]:
    """What the audit's spans leave unproven about the archive's boundaries.
    The audit judges gaps between committed ranges and leaves what lies below
    the first one unjudged, so a prefix the engine never archived reads as a
    clean audit. The soak's topic is new, its first record is offset 0, and the
    engine reads from the beginning: the archive must start at 0 on every
    partition, and, once the load has stopped and the engine drained, end where
    the partition does (`ends`)."""
    problems = []
    for partition in range(partitions):
        if partition not in spans:
            problems.append(f"partition {partition}: the audit reported no committed range")
            continue
        start, end = spans[partition]
        if start != 0:
            problems.append(
                f"partition {partition}: the archive starts at offset {start}, not 0; [0, {start}) is unjudged"
            )
        if ends is not None and end != ends.get(partition):
            problems.append(
                f"partition {partition}: the archive ends at offset {end}, the partition at {ends.get(partition)}"
            )
    return problems


def parse_pruned(text: str) -> int | None:
    """Rows a `prune-ledger` run deleted: from its JSON status or its sentence."""
    try:
        report = parse_json_report(text)
        if "deleted" in report:
            return int(report["deleted"])
    except ValueError:
        pass
    match = re.search(r"pruned (\d+) committed ledger row", text)
    return int(match[1]) if match else None


def extract_skew_shim_source(scenario: Path = SKEW_SCENARIO) -> str:
    """The C source of the clock-skew shim, as live-chaos scenario 07 embeds
    it. The soak builds the same shim rather than keeping a copy that could
    drift: this reads the heredoc out of the scenario, and a test pins that it
    still can."""
    text = scenario.read_text()
    match = re.search(r"cat > \"\$WORKDIR/skew\.c\" <<'EOF'\n(.*?)\nEOF\n", text, re.S)
    if not match or "clock_gettime" not in match[1]:
        raise LabError(f"cannot find the clock-skew shim's source in {scenario}")
    return match[1] + "\n"


def shim_name(seconds: int) -> str:
    """File name of the shim built for a step, matching Dockerfile.engine."""
    return f"skew{seconds:+d}.so"


# ---------------------------------------------------------------------------
# The lab
# ---------------------------------------------------------------------------


class Lab:
    def __init__(
        self,
        project: str,
        run_dir: Path,
        engine_image: str,
        *,
        toxiproxy_port: int,
        data_dir: Path | None = None,
        load: dict | None = None,
        subnet: str | None = None,
        runner: Runner = subprocess_runner,
        sleep: Callable[[float], None] = time.sleep,
        http: Callable[[str, str, bytes | None], tuple[int, str]] | None = None,
    ):
        self.project = project
        self.run_dir = run_dir
        self.engine_image = engine_image
        self.toxiproxy_port = toxiproxy_port
        self.data_dir = data_dir
        self.load = load or {}
        self.subnet = subnet
        self.run = runner
        self.sleep = sleep
        self.http = http or self._http

    # -- names ---------------------------------------------------------

    def container(self, service: str) -> str:
        return f"{self.project}-{service}"

    @property
    def compose_env(self) -> dict[str, str]:
        env = {
            "SOAK_PROJECT": self.project,
            "SOAK_RUN_DIR": str(self.run_dir),
            "SOAK_ENGINE_IMAGE": self.engine_image,
            "SOAK_TOXIPROXY_PORT": str(self.toxiproxy_port),
            "SOAK_TOPIC": TOPIC,
            **{k: str(v) for k, v in self.load.items()},
        }
        if self.data_dir is not None:
            env.update(
                SOAK_KAFKA_DATA=str(self.data_dir / "kafka"),
                SOAK_MINIO_DATA=str(self.data_dir / "minio"),
                SOAK_ENGINE_STATE=str(self.data_dir / "engine-state"),
                SOAK_ENGINE_WORK=str(self.data_dir / "engine-work"),
            )
        return env

    # -- docker --------------------------------------------------------

    def docker(self, *args: str, timeout: float | None = 120) -> Result:
        return self.run(["docker", *args], timeout)

    def compose(self, *args: str, timeout: float | None = 600) -> Result:
        # The environment travels on the command line (`env VAR=... docker`),
        # not through the driver's own environment, so a runner that records
        # argv records the whole call.
        env = [f"{k}={v}" for k, v in sorted(self.compose_env.items())]
        files = ["-f", str(COMPOSE_FILE)]
        if self.subnet:
            # A host with many Docker networks can run out of the daemon's
            # predefined address pools; an explicit subnet needs none.
            override = self.run_dir / "network.override.yml"
            override.write_text(f"networks:\n  soak:\n    ipam:\n      config:\n        - subnet: {self.subnet}\n")
            files += ["-f", str(override)]
        argv = ["env", *env, "docker", "compose", "-p", self.project, *files, *args]
        return self.run(argv, timeout)

    def require(self, result: Result, what: str) -> Result:
        if not result.ok:
            raise LabError(f"{what} failed (exit {result.code}): {result.text()[-600:]}")
        return result

    def up(self, *services: str) -> None:
        self.require(self.compose("up", "-d", "--wait", *services, timeout=900), f"compose up {' '.join(services)}")

    def create(self, *services: str) -> None:
        self.require(self.compose("up", "--no-start", *services, timeout=300), f"compose create {' '.join(services)}")

    def prepare_data_dirs(self) -> None:
        """Hand the engine's state and work directories to the user the image
        runs as. A new Docker volume is owned by root, and the engine, which
        never runs as root, would refuse to start on it."""
        self.require(
            self.compose(
                "run",
                "--rm",
                "--no-deps",
                "--user",
                "0",
                "--cap-add",
                "CHOWN",
                "--entrypoint",
                "chown",
                "engine",
                "-R",
                "10001:10001",
                "/data/state",
                "/data/work",
                timeout=180,
            ),
            "preparing the engine's data directories",
        )

    def down(self) -> Result:
        return self.compose("down", "--volumes", "--remove-orphans", "--timeout", "30", timeout=300)

    def state(self, service: str) -> dict:
        """`docker inspect` of a service's container: running, exit code,
        start time and health. {} when the container does not exist."""
        fmt = (
            "{{.State.Running}}|{{.State.ExitCode}}|{{.State.StartedAt}}|"
            "{{if .State.Health}}{{.State.Health.Status}}{{end}}|{{.State.OOMKilled}}"
        )
        result = self.docker("inspect", "--format", fmt, self.container(service), timeout=30)
        if not result.ok:
            return {}
        running, code, started, health, oom = (result.out.strip().split("|") + [""] * 5)[:5]
        return {
            "running": running == "true",
            "exit_code": int(code or 0),
            "started_at": started,
            "health": health,
            "oom_killed": oom == "true",
        }

    def running(self, service: str) -> bool:
        return bool(self.state(service).get("running"))

    def wait_state(self, service: str, predicate: Callable[[dict], bool], timeout: float, what: str) -> dict:
        deadline = time.monotonic() + timeout
        state: dict = {}
        while True:
            state = self.state(service)
            if predicate(state):
                return state
            if time.monotonic() >= deadline:
                raise LabError(f"{what}: {service} did not get there within {timeout:.0f}s; last state {state}")
            self.sleep(1)

    # -- engine --------------------------------------------------------

    def engine_exec(self, *argv: str, timeout: float | None = 60) -> Result:
        return self.docker("exec", self.container("engine"), *argv, timeout=timeout)

    def engine_probe(self) -> dict:
        script = (
            "echo comm $(cat /proc/1/comm); "
            "awk '/^VmRSS/ {print \"rss_kb\", $2}' /proc/1/status; "
            "echo fds $(ls /proc/1/fd | wc -l); "
            "echo work_kb $(du -sk /data/work | cut -f1); "
            "db=/data/state/state.db; "
            "echo ledger_bytes $(( $(stat -c %s $db 2>/dev/null || echo 0)"
            " + $(stat -c %s $db-wal 2>/dev/null || echo 0) ))"
        )
        result = self.engine_exec("sh", "-c", script, timeout=60)
        if not result.ok:
            raise LabError(f"engine probe exit {result.code}: {result.text()[-200:]}")
        return parse_probe(result.out)

    def scrape_metrics(self) -> str:
        result = self.engine_exec("curl", "-sf", "--max-time", "10", "http://127.0.0.1:9090/metrics", timeout=30)
        if not result.ok:
            raise LabError(f"engine /metrics exit {result.code}: {result.text()[-200:]}")
        return result.out

    def vtopctl(self, *args: str, timeout: float | None = 300, json_report: bool = False) -> Result:
        flags = ["--json"] if json_report else []
        return self.engine_exec("vtopctl", *flags, *args, "--config", CONTAINER_CONFIG, timeout=timeout)

    def set_preload(self, seconds: int | None) -> None:
        """Choose the clock shim the engine's next start loads (None for none).
        The entrypoint in the soak layer reads this file when the container
        starts."""
        path = self.run_dir / "ctl" / "ld_preload"
        if seconds is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(f"{SHIM_DIR_IN_IMAGE}/{shim_name(seconds)}\n")
            path.chmod(0o644)

    def skew_marker(self) -> int | None:
        """The wall-clock second the shim recorded when the engine started
        (already shifted by the skew), or None when no shim loaded. Read inside
        the engine's container: the shim creates the file with mode 0600 as
        the image's own user, whom the host's user is not."""
        result = self.engine_exec("cat", f"{CONTAINER_CTL}/skew-loaded", timeout=30)
        try:
            return int(result.out.strip()) if result.ok else None
        except ValueError:
            return None

    def clear_skew_marker(self) -> None:
        # The directory is world-writable, so the host may remove a file the
        # engine's user made; the entrypoint removes it at start as well.
        try:
            (self.run_dir / "ctl" / "skew-loaded").unlink(missing_ok=True)
        except PermissionError:
            pass

    def start_engine(self, timeout: float = 120) -> None:
        self.require(self.docker("start", self.container("engine"), timeout=60), "docker start engine")
        self.wait_ready(timeout)

    def wait_ready(self, timeout: float) -> None:
        """The engine is running and its /metrics answers."""
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            if not self.running("engine"):
                last = "the container is not running"
            else:
                try:
                    self.scrape_metrics()
                    return
                except LabError as error:
                    last = str(error)
            self.sleep(1)
        raise LabError(f"the engine did not serve /metrics within {timeout:.0f}s: {last}")

    def kill_engine(self) -> dict:
        self.require(self.docker("kill", "--signal=KILL", self.container("engine"), timeout=60), "docker kill engine")
        return self.wait_state("engine", lambda s: not s.get("running"), 30, "after SIGKILL")

    def stop_engine(self, grace_s: int = 60) -> dict:
        self.require(
            self.docker("stop", "--time", str(grace_s), self.container("engine"), timeout=grace_s + 60),
            "docker stop engine",
        )
        return self.wait_state("engine", lambda s: not s.get("running"), 30, "after SIGTERM")

    # -- kafka ---------------------------------------------------------

    def kafka_tool(self, tool: str, *args: str, timeout: float | None = 120) -> Result:
        return self.docker(
            "exec",
            self.container("kafka"),
            f"{KAFKA_TOOLS}/{tool}",
            "--bootstrap-server",
            "kafka:9092",
            *args,
            timeout=timeout,
        )

    def create_topic(self, partitions: int, retention_hours: int) -> None:
        self.require(
            self.kafka_tool(
                "kafka-topics.sh",
                "--create",
                "--if-not-exists",
                "--topic",
                TOPIC,
                "--partitions",
                str(partitions),
                "--replication-factor",
                "1",
                "--config",
                f"retention.ms={retention_hours * 3600 * 1000}",
                "--config",
                "segment.bytes=268435456",
            ),
            "create topic",
        )

    def lag(self, partitions: int) -> dict:
        """Topic end and group committed offset, summed over the partitions."""
        result = self.kafka_tool("kafka-consumer-groups.sh", "--describe", "--group", GROUP, timeout=90)
        if not result.ok:
            raise LabError(f"kafka-consumer-groups --describe exit {result.code}: {result.text()[-300:]}")
        try:
            return lag_totals(parse_describe(result.out + "\n" + result.err, GROUP, TOPIC), partitions)
        except ValueError as error:
            raise LabError(str(error)) from None

    def partition_offsets(self, partitions: int) -> dict[int, dict]:
        result = self.kafka_tool("kafka-consumer-groups.sh", "--describe", "--group", GROUP, timeout=90)
        if not result.ok:
            raise LabError(f"kafka-consumer-groups --describe exit {result.code}: {result.text()[-300:]}")
        try:
            rows = parse_describe(result.out + "\n" + result.err, GROUP, TOPIC)
        except ValueError as error:
            raise LabError(str(error)) from None
        lag_totals(rows, partitions)
        return rows

    def start_producer(self) -> None:
        self.require(self.compose("up", "-d", "producer", timeout=120), "start the producer")

    # -- toxiproxy -----------------------------------------------------

    def _http(self, method: str, path: str, body: bytes | None) -> tuple[int, str]:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.toxiproxy_port}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()
        except (urllib.error.URLError, OSError) as error:
            return 0, str(error)

    def set_proxy(self, name: str, enabled: bool) -> None:
        status, body = self.http("POST", f"/proxies/{name}", json.dumps({"enabled": enabled}).encode())
        if status != 200:
            raise LabError(f"toxiproxy refused to set proxy {name} enabled={enabled}: HTTP {status} {body[:200]}")

    def proxy_enabled(self, name: str) -> bool:
        status, body = self.http("GET", f"/proxies/{name}", None)
        if status != 200:
            raise LabError(f"toxiproxy proxy {name}: HTTP {status} {body[:200]}")
        return bool(json.loads(body).get("enabled"))

    def engine_can_reach(self, name: str) -> bool:
        """Whether the engine container can open a TCP connection through the
        named proxy: how a fault proves it landed, and that it lifted."""
        port = PROXY_PORTS[name]
        return self.engine_exec("nc", "-z", "-w", "3", "toxiproxy", str(port), timeout=20).ok

    # -- host ----------------------------------------------------------

    def host_info(self) -> dict:
        info = {}
        for key, argv in (
            (
                "docker_version",
                ["docker", "version", "--format", "{{.Server.Version}} {{.Server.Os}}/{{.Server.Arch}}"],
            ),
            (
                "docker_info",
                ["docker", "info", "--format", "{{.OperatingSystem}}; {{.NCPU}} CPUs; {{.MemTotal}} bytes memory"],
            ),
            ("uname", ["uname", "-srm"]),
        ):
            result = self.run(argv, 30)
            info[key] = result.out.strip() if result.ok else f"unavailable: {result.text()[:100]}"
        return info

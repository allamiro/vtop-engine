"""A scripted stand-in for soak_lab.Lab, for tests that drive the injectors or a
whole run without Docker (#726)."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import soak_lab  # noqa: E402

EXPOSITION = (
    "# TYPE vtop_commits_total counter\n"
    'vtop_commits_total{{tenant="soak",source_type="kafka",format="raw"}} {n}\n'
    "# TYPE vtop_verified_total counter\n"
    'vtop_verified_total{{tenant="soak",source_type="kafka",format="raw"}} {n}\n'
    "# TYPE vtop_verification_failures_total counter\n"
    'vtop_verification_failures_total{{tenant="soak",source_type="kafka",format="raw"}} 0\n'
    "# TYPE vtop_batch_duration_seconds histogram\n"
    'vtop_batch_duration_seconds_sum{{tenant="soak",source_type="kafka",format="raw"}} {sum}\n'
    'vtop_batch_duration_seconds_count{{tenant="soak",source_type="kafka",format="raw"}} {n}\n'
)


class FakeLab:
    """The calls the injectors make, scripted; records what they did."""

    def __init__(self):
        self.log = []
        self.engine_running = True
        self.kill_exit = 137
        self.proxies = {"kafka": True, "minio": True}
        self.reach_ignores_proxy = False
        self.cannot_reach_at_all = False
        self.skew_marker_value = None
        self.shim_loads = True
        self.kafka_started = "t0"
        self.now = None  # a fixed host time, or the wall clock when None
        self._init_run()

    def host_now(self):
        return self.now if self.now is not None else time.time()

    def container(self, service):
        return f"c-{service}"

    def running(self, service):
        if service == "producer":
            return not self.producer_stopped
        return self.engine_running if service == "engine" else True

    def state(self, service):
        if service == "kafka":
            return {"running": True, "health": "healthy", "started_at": self.kafka_started}
        return {"running": self.engine_running}

    def kill_engine(self):
        self.log.append("kill")
        self.engine_running = False
        return {"running": False, "exit_code": self.kill_exit}

    def stop_engine(self, grace_s=60):
        self.log.append("stop")
        self.engine_running = False
        return {"running": False, "exit_code": 0}

    def start_engine(self, timeout=120):
        self.log.append("start")
        self.engine_running = True
        if self.preload is not None and self.shim_loads:
            self.skew_marker_value = int(self.host_now()) + self.preload
        elif self.preload is None:
            self.skew_marker_value = None

    preload = None

    def set_preload(self, seconds):
        self.log.append(f"preload {seconds}")
        self.preload = seconds

    def skew_marker(self):
        return self.skew_marker_value

    def clear_skew_marker(self):
        self.skew_marker_value = None

    def docker(self, *args, timeout=None):
        self.log.append("docker " + " ".join(args))
        if args[0] == "restart":
            self.kafka_started = "t1"
        if args[0] == "stop" and args[-1].endswith("producer"):
            self.producer_stopped = True
        return soak_lab.Result(0, "", "")

    def require(self, result, what):
        return result

    def wait_state(self, service, predicate, timeout, what):
        state = self.state(service)
        assert predicate(state), f"{what}: {state}"
        return state

    def set_proxy(self, name, enabled):
        self.log.append(f"proxy {name} {enabled}")
        self.proxies[name] = enabled

    def proxy_enabled(self, name):
        return self.proxies[name]

    def engine_can_reach(self, name):
        if self.cannot_reach_at_all:
            return False
        return True if self.reach_ignores_proxy else self.proxies[name]

    # -- what a whole run needs ----------------------------------------

    engine_image = "fake:1"

    def _init_run(self):
        self.end = 0
        self.scrapes = 0
        self.producer_stopped = False
        self.audit_ok = True
        self.audit_gap = False  # a Kafka gap the audit prints and still exits 0 over
        self.partitions = 2
        self.audit_prefix_from = 0  # where the audit says the archive starts
        self.audit_end_short = 0  # how far short of the partition's end it says the archive stops
        self.scrub_ok = True
        self.calls = []

    def host_info(self):
        return {"docker_version": "fake"}

    def up(self, *services):
        self.log.append("up " + " ".join(services))

    def create_topic(self, partitions, retention_hours):
        self.log.append("create_topic")

    def prepare_data_dirs(self):
        self.log.append("prepare_data_dirs")

    def wait_ready(self, timeout):
        self.log.append("wait_ready")

    def start_producer(self):
        self.log.append("start_producer")

    def down(self):
        self.log.append("down")

    def engine_probe(self):
        return {"rss_bytes": 150_000_000, "open_fds": 40, "work_dir_bytes": 1_000_000, "ledger_bytes": 100_000}

    def scrape_metrics(self):
        self.scrapes += 1
        return EXPOSITION.format(n=self.scrapes, sum=self.scrapes * 10.0)

    drain_polls = 30  # reads it takes the engine to catch up once the load stops

    def lag(self, partitions):
        if not self.producer_stopped:
            self.end += 300
            return {"end": self.end, "committed": max(0, self.end - 100)}
        self.drain_polls -= 1
        return {"end": self.end, "committed": self.end if self.drain_polls <= 0 else self.end - 100}

    def partition_offsets(self, partitions):
        return {p: {"committed": 50 + p, "end": 50 + p} for p in range(partitions)}

    def vtopctl(self, *args, timeout=None, json_report=False):
        self.calls.append(args)
        command = args[0]
        if command == "audit-coverage":
            if not self.audit_ok:
                return soak_lab.Result(1, "hole at offsets 10..19", "")
            lines = []
            for p in range(self.partitions):
                start = self.audit_prefix_from
                end = 50 + p + self.audit_end_short
                unit = f"kafka soak-topic[{p}] group vtop-soak"
                lines.append(f"ok   {unit}: 3 committed range(s) over [{start}, {end}), 0 duplicated")
                if self.audit_gap:
                    lines.append(
                        "     gap [40, 50): no records archived (compaction or transaction markers can leave such gaps)"
                    )
            return soak_lab.Result(0, "\n".join(lines) + "\ncoverage audit PASSED\n", "")
        if command == "scrub":
            ok = self.scrub_ok
            body = {"mode": "all", "checked": 50, "committed_rows": 50, "passed": ok, "findings": [] if ok else [{}]}
            return soak_lab.Result(0 if ok else 1, json.dumps(body, indent=2), "")
        if command == "prune-ledger":
            return soak_lab.Result(0, "pruned 3 committed ledger row(s); digest /x; audit record a", "")
        return soak_lab.Result(2, "", "unknown command")

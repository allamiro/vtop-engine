"""VTOP's node configurations for one cell of the Kafka comparison (#596).

A VTOP data-node process serves one range, and a Kafka partition is one
range. So N partitions are N ranges, each replicated to all three
containers. That is 3N processes, packed into the same three containers
(and the same three cgroups) Kafka's three brokers get. Partition p is led
by container (p mod 3) + 1 and followed by the other two. With six
partitions, each container leads two ranges and follows four.

Every leader lists every partition in `kafka.partitions`, each under its own
broker id (p + 1), so a client's Metadata shows N brokers where Kafka's
shows 3. The report states this difference. Each process in a container
takes its own ports, offset by its partition index:

    native 9400+p   replica 9300+p   observability 9600+p   kafka 9092+p

With one partition, the generated configs are the committed static shape
(vtop/leader.yaml, follower-1.yaml and follower-2.yaml). A test compares
them parsed.

    configs = topology.vtop_configs(6)        # {"vtop-1/p0.yaml": "...", ...}
    topology.write(configs, run_dir)          # files and their hashes
"""
from __future__ import annotations

import hashlib
import os
from typing import NamedTuple

CONTAINERS = (1, 2, 3)
NODE_UUIDS = {1: "aaaaaaaa-0000-0000-0000-0000000000a1", 2: "aaaaaaaa-0000-0000-0000-0000000000a2",
              3: "aaaaaaaa-0000-0000-0000-0000000000a3"}
CLUSTER_ID = "11111111-2222-3333-4444-555555555555"
PRINCIPAL_ID = "aaaaaaaa-0000-0000-0000-0000000000ce"
TOPIC = "bench"
FENCING_EPOCH = 18
MAX_PARTITIONS = 9  # one hex digit of the range and segment uuids, and ports below the next family

NATIVE_BASE, REPLICA_BASE, METRICS_BASE, KAFKA_BASE = 9400, 9300, 9600, 9092


class Placement(NamedTuple):
    partition: int
    leader: int
    followers: tuple[int, int]


def placements(partitions: int) -> list[Placement]:
    """Who leads and who follows each partition: round-robin leaders."""
    if not 1 <= partitions <= MAX_PARTITIONS:
        raise ValueError(f"{partitions} partitions: the topology serves 1 to {MAX_PARTITIONS}")
    out = []
    for p in range(partitions):
        leader = CONTAINERS[p % len(CONTAINERS)]
        followers = tuple(c for c in CONTAINERS if c != leader)
        out.append(Placement(p, leader, followers))
    return out


def range_uuid(p: int) -> str:
    return f"aaaaaaaa-0000-0000-0000-0000000000c{p + 1}"


def segment_uuid(p: int) -> str:
    return f"aaaaaaaa-0000-0000-0000-0000000000d{p + 1}"


def _common(container: int, p: int, role: str) -> list[str]:
    return [
        f"role: {role}",
        f"node_uuid: {NODE_UUIDS[container]}",
        f"cluster_id: {CLUSTER_ID}",
        # One data directory per range: the image's home is the volume.
        f"data_dir: /home/vtop/data{'' if p == 0 else f'-p{p}'}",
        f"fencing_epoch: {FENCING_EPOCH}",
        f'range: {{ topic: "{TOPIC}", topic_epoch: 1, range_id: {range_uuid(p)}, range_generation: 0 }}',
        f"segment_id: {segment_uuid(p)}",
    ]


def _leader(place: Placement, partitions: int) -> str:
    p, c = place.partition, place.leader
    lines = _common(c, p, "leader") + [
        f'native_listen: "0.0.0.0:{NATIVE_BASE + p}"',
        f'replica_listen: "0.0.0.0:{REPLICA_BASE + p}"',
        "followers:",
    ]
    for f in place.followers:
        lines.append(f'  - {{ node_uuid: {NODE_UUIDS[f]}, addr: "vtop-{f}:{REPLICA_BASE + p}", '
                     f'server_name: "vtop-{f}" }}')
    lines += [
        "replica_transport: plaintext-on-any-interface",
        "native_transport: plaintext-on-any-interface",
        f"principal_id: {PRINCIPAL_ID}",
        f'observability: {{ listen: "0.0.0.0:{METRICS_BASE + p}" }}',
        "kafka:",
        f'  listen: "0.0.0.0:{KAFKA_BASE + p}"',
        "  any_interface: true",
        f'  advertised_host: "vtop-{c}"',
        f"  advertised_port: {KAFKA_BASE + p}",
        f'  topic: "{TOPIC}"',
        f"  node_id: {p + 1}",
    ]
    if partitions > 1:
        lines += [f"  partition: {p}", "  partitions:"]
        for other in placements(partitions):
            lines.append(f"    - {{ partition: {other.partition}, node_id: {other.partition + 1}, "
                         f'host: "vtop-{other.leader}", port: {KAFKA_BASE + other.partition} }}')
    return "\n".join(lines) + "\n"


def _follower(container: int, p: int) -> str:
    lines = _common(container, p, "follower") + [
        f'replica_listen: "0.0.0.0:{REPLICA_BASE + p}"',
        "replica_transport: plaintext-on-any-interface",
        f'observability: {{ listen: "0.0.0.0:{METRICS_BASE + p}" }}',
    ]
    return "\n".join(lines) + "\n"


def vtop_configs(partitions: int) -> dict[str, str]:
    """Every process's config, keyed `vtop-<container>/p<partition>-<role>.yaml`.

    The role is in the name, so the container's supervisor can start the
    followers before the leaders.
    """
    configs = {}
    for place in placements(partitions):
        configs[f"vtop-{place.leader}/p{place.partition}-leader.yaml"] = _leader(place, partitions)
        for f in place.followers:
            configs[f"vtop-{f}/p{place.partition}-follower.yaml"] = _follower(f, place.partition)
    return dict(sorted(configs.items()))


def leader_map(partitions: int) -> dict[str, str]:
    """Partition to the container that leads it, for environment.json."""
    return {str(place.partition): f"vtop-{place.leader}" for place in placements(partitions)}


def write(configs: dict[str, str], directory: str) -> dict[str, str]:
    """Write the configs under `directory`, and return each file's sha256."""
    hashes = {}
    for name, text in configs.items():
        path = os.path.join(directory, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as handle:
            handle.write(text)
        hashes[name] = hashlib.sha256(text.encode()).hexdigest()
    return hashes

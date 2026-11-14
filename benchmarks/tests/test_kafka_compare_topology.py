"""VTOP's generated topology for the Kafka comparison (#596): N partitions are
N replicated ranges over the same three containers."""
import collections
import os

import pytest

from kafka_compare import topology

yaml = pytest.importorskip("yaml")

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(os.path.dirname(HERE), "kafka_compare", "vtop")


def parsed(configs):
    return {name: yaml.safe_load(text) for name, text in configs.items()}


def test_the_generated_one_partition_config_equals_the_committed_static_shape():
    generated = parsed(topology.vtop_configs(1))
    with open(os.path.join(STATIC, "leader.yaml")) as handle:
        leader = yaml.safe_load(handle)
    assert generated["vtop-1/p0-leader.yaml"] == leader
    for container, name in ((2, "follower-1.yaml"), (3, "follower-2.yaml")):
        with open(os.path.join(STATIC, name)) as handle:
            assert generated[f"vtop-{container}/p0-follower.yaml"] == yaml.safe_load(handle)
    assert len(generated) == 3


def test_six_partitions_generate_18_configs_with_unique_uuids_ports_and_broker_ids():
    configs = parsed(topology.vtop_configs(6))
    assert len(configs) == 18
    ranges = {c["range"]["range_id"] for c in configs.values()}
    segments = {c["segment_id"] for c in configs.values()}
    assert len(ranges) == len(segments) == 6
    leaders = [c for c in configs.values() if c["role"] == "leader"]
    assert sorted(c["kafka"]["node_id"] for c in leaders) == [1, 2, 3, 4, 5, 6]
    for container in topology.CONTAINERS:
        mine = [c for name, c in configs.items() if name.startswith(f"vtop-{container}/")]
        ports = [c["replica_listen"] for c in mine] + [c["observability"]["listen"] for c in mine]
        ports += [c["native_listen"] for c in mine if "native_listen" in c]
        ports += [c["kafka"]["listen"] for c in mine if "kafka" in c]
        assert len(ports) == len(set(ports)), f"vtop-{container} reuses a port: {ports}"
        dirs = [c["data_dir"] for c in mine]
        assert len(dirs) == len(set(dirs))
        assert {c["node_uuid"] for c in mine} == {topology.NODE_UUIDS[container]}


def test_each_container_leads_two_ranges_and_follows_four():
    roles = collections.Counter(
        (name.split("/")[0], c["role"]) for name, c in parsed(topology.vtop_configs(6)).items())
    for container in topology.CONTAINERS:
        assert roles[(f"vtop-{container}", "leader")] == 2
        assert roles[(f"vtop-{container}", "follower")] == 4


def test_every_leader_lists_all_six_partitions_at_their_leaders():
    configs = parsed(topology.vtop_configs(6))
    expected = [{"partition": p, "node_id": p + 1, "host": f"vtop-{p % 3 + 1}", "port": 9092 + p}
                for p in range(6)]
    for name, c in configs.items():
        if c["role"] != "leader":
            continue
        assert c["kafka"]["partitions"] == expected, name
        assert c["kafka"]["partition"] == int(name.split("/p")[1].split("-")[0])
        assert c["kafka"]["advertised_host"] == name.split("/")[0]


def test_a_leaders_followers_are_the_other_containers_at_its_replica_port():
    for name, c in parsed(topology.vtop_configs(6)).items():
        if c["role"] != "leader":
            continue
        p = c["kafka"]["partition"]
        hosts = {f["addr"] for f in c["followers"]}
        me = name.split("/")[0]
        assert hosts == {f"vtop-{x}:{9300 + p}" for x in (1, 2, 3) if f"vtop-{x}" != me}


@pytest.mark.parametrize("partitions", [0, 10])
def test_a_partition_count_outside_the_topology_is_refused(partitions):
    with pytest.raises(ValueError, match="1 to 9"):
        topology.vtop_configs(partitions)


def test_written_configs_are_hashed(tmp_path):
    configs = topology.vtop_configs(2)
    hashes = topology.write(configs, str(tmp_path))
    assert set(hashes) == set(configs)
    assert (tmp_path / "vtop-2" / "p1-leader.yaml").read_text() == configs["vtop-2/p1-leader.yaml"]
    assert topology.leader_map(2) == {"0": "vtop-1", "1": "vtop-2"}

"""The netem middlebox's place in the lab's compose file (#477).

The middlebox is the one container in this lab that holds a capability, and
that is the whole reason to lint it: a privilege added for one service has a
way of spreading to its neighbours, and the neighbours here are the very
services whose measurements the capability exists to shape. These tests read
the compose file itself rather than a rendered copy, so they hold whether or
not docker is installed and whether or not PyYAML is.

The reader below is deliberately small and deliberately strict: it understands
exactly the shape this file is written in, and it FAILS rather than returning
an empty answer when it cannot find what it expects. A lint that quietly finds
no services would pass every assertion in this module while checking nothing.
"""
from __future__ import annotations

import os
import re

COMPOSE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docker-compose.benchmark.yml")

SERVICE_RE = re.compile(r"^  ([a-z][a-z0-9_-]*):\s*$")
TOP_LEVEL_RE = re.compile(r"^([a-z][a-z0-9_-]*):\s*$")


def read_services() -> dict[str, list[str]]:
    """Every service in the compose file, mapped to its own lines.

    Raises rather than returning nothing: an empty result would make each
    assertion below vacuously true.
    """
    with open(COMPOSE) as fh:
        lines = fh.read().splitlines()
    services: dict[str, list[str]] = {}
    section = None
    current = None
    for line in lines:
        top = TOP_LEVEL_RE.match(line)
        if top:
            section = top.group(1)
            current = None
            continue
        if section != "services":
            continue
        found = SERVICE_RE.match(line)
        if found:
            current = found.group(1)
            services[current] = []
            continue
        if current is not None:
            services[current].append(line)
    if not services:
        raise AssertionError(
            f"{COMPOSE} parsed to no services at all: this lint's reader no longer "
            "understands the file's shape, and every assertion below would pass "
            "without checking anything")
    return services


def value_of(body: list[str], key: str) -> str | None:
    for line in body:
        stripped = line.strip()
        if stripped.startswith(f"{key}:"):
            return stripped.split(":", 1)[1].strip()
    return None


def test_the_middlebox_is_the_only_service_that_holds_a_capability():
    # The privilege is the point of the review (#477): tc, ifb and the
    # forwarding rules need NET_ADMIN, and nothing else in this lab may
    # acquire it as a side effect of the middlebox existing. If a second
    # service ever needs it, that is a decision someone should have to make
    # in front of this assertion.
    services = read_services()
    with_cap_add = {name for name, body in services.items() if value_of(body, "cap_add")}
    assert with_cap_add == {"netem"}, (
        f"exactly one service may hold a capability in the benchmark lab; found "
        f"{sorted(with_cap_add)}. A shaped measurement is only trustworthy while the "
        "shaping is confined to the box that does the shaping")
    assert "NET_ADMIN" in (value_of(services["netem"], "cap_add") or ""), (
        "the middlebox needs NET_ADMIN or it cannot install a qdisc, and a netem run "
        "would then measure an unshaped link under a shaped name")


def test_the_engine_and_the_store_still_drop_every_capability():
    # The two services whose numbers the lab produces. Their hardening is
    # what makes the middlebox's capability a bounded exception rather than
    # a loosening of the lab.
    services = read_services()
    for name in ("vtop-engine", "minio", "netem-probe"):
        assert name in services, f"{name} is not in the benchmark compose file any more"
        assert value_of(services[name], "cap_drop") == "[ALL]", (
            f"{name} must keep cap_drop: [ALL]; the middlebox exists precisely so that "
            "the measured services never need a capability of their own")
        assert value_of(services[name], "cap_add") is None, (
            f"{name} has gained a capability: the shaping belongs on the middlebox, and "
            "a capability here is either unnecessary or a hole")


def test_the_netem_services_are_behind_their_own_profile():
    # An unshaped scenario must not pay for the middlebox, and — more to the
    # point — must not have a forwarding box with NET_ADMIN sitting in its
    # lab at all.
    services = read_services()
    for name in ("netem", "netem-probe"):
        assert value_of(services[name], "profiles") == '["netem"]', (
            f"{name} must stay on the netem profile, so a lab brought up for an unshaped "
            "run holds no privileged container")


def test_the_middlebox_publishes_no_port():
    # Docker's userland proxy TERMINATES TCP for a published port. Publishing
    # one here would put a second TCP-terminating hop into a path whose entire
    # purpose is to be L3 — the same defect that makes toxiproxy unable to
    # answer this milestone's question. lib/netem.py refuses a host-mode
    # scenario for the same reason; this keeps the compose file from quietly
    # offering the bypass that refusal exists to prevent.
    services = read_services()
    assert value_of(services["netem"], "ports") is None, (
        "the netem middlebox must not publish a port: a published port is reached "
        "through docker's userland TCP proxy, and a run through it is not the L3 path "
        "the qdiscs shape")


def test_the_store_is_reachable_on_the_middlebox_side_at_a_fixed_address():
    # The middlebox DNATs to a literal address rather than to a name (see the
    # compose comment): a host on two networks answers its own name two ways,
    # and a middlebox forwarding to the wrong one measures the wrong hop.
    services = read_services()
    body = "\n".join(services["minio"])
    assert "netem-store" in body and "10.77.0.10" in body, (
        "MinIO must carry a fixed address on the middlebox's store-side network, or the "
        "DNAT target in the middlebox's start-up rules names nothing")
    netem_body = "\n".join(services["netem"])
    assert "10.77.0.10:9000" in netem_body, (
        "the middlebox's DNAT rule must name the store's store-side address; if the two "
        "drift apart the forwarded connection lands somewhere nobody chose")


def test_the_middlebox_endpoint_is_recognised_as_the_lab_store():
    # The lab's credential fallback keys off "is this endpoint the lab's own
    # store" (#477). The middlebox forwards at L3 to that same MinIO, so a
    # netem run must get the lab credentials — without this the engine
    # authenticates with nothing and every upload fails, which reads as a
    # transport result rather than the configuration mistake it is.
    from lib import engine

    through_the_box = {"backend": "s3_native", "shaping_driver": "netem",
                       "endpoint_url": "http://netem:9200"}
    assert engine._shaped_by_the_bundled_middlebox(through_the_box), (
        "an endpoint naming the bundled middlebox is the lab's store one hop out, and "
        "must be handed the lab's credentials")

    # ... and ONLY that box. An endpoint pointed straight at the store while
    # the scenario claims netem would go around every qdisc; lib/netem.py
    # refuses it, and the credential decision must not disagree.
    around_the_box = {"backend": "s3_native", "shaping_driver": "netem",
                      "endpoint_url": "http://minio:9000"}
    assert not engine._shaped_by_the_bundled_middlebox(around_the_box), (
        "an endpoint that bypasses the middlebox is not a netem run, and the credential "
        "decision must not quietly bless one")

    # A toxiproxy scenario must not be judged by the middlebox rule either.
    proxied = {"backend": "s3_native", "shaping_driver": "toxiproxy",
               "endpoint_url": "http://netem:9200"}
    assert not engine._shaped_by_the_bundled_middlebox(proxied), (
        "the driver decides which box is in the path; a toxiproxy scenario naming the "
        "middlebox's port is a mistake, not a lab endpoint")

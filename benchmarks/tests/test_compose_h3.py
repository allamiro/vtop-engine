"""The h3 proxy's shape in the benchmark compose file is a lint, not a habit (#484).

Two things about this service are load-bearing and neither is visible in any
number the benchmark produces.

The first is the hardening baseline. Every service in the lab drops all
capabilities, refuses privilege escalation, runs on a read-only filesystem,
caps pids and memory, and publishes only to loopback. A proxy is the service
most likely to lose that quietly — it is the one thing in the lab whose job is
to accept connections — and a lab that reaches for `cap_add` once teaches the
next person that the baseline is negotiable.

The second is the profile. The h3 proxy exists for one epic's measurements
(#475). If it ever landed in the default profile, every unrelated `compose up`
would start it, and every unrelated scenario would pay for a container it never
uses — while the numbers stayed plausible enough that nobody would look.

The third is the one the review round after this service landed found: the
authority. nginx knows the port it bound and nothing about the port it is
PUBLISHED on, so the compose file has to hand that number in, and everything
downstream of it — the Alt-Svc advertisement, the Host header reconstructed for
HTTP/3 — has to use it rather than the listener's own. Getting that wrong does
not break the proxy: it makes the store reject every signature, which reads as
a credential problem. The plumbing that carries the number is asserted here;
`verify-h3.sh` asserts the result against a running proxy.

Written as assertions over the parsed file rather than over `docker compose
config`, so it runs in CI without a docker daemon.
"""

import os
import posixpath
import re

import pytest

yaml = pytest.importorskip(
    "yaml", reason="the compose file is nested YAML; the flat fallback parser cannot read it")

BENCH_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE_PATH = os.path.join(BENCH_DIR, "docker-compose.benchmark.yml")
TEMPLATE_PATH = os.path.join(BENCH_DIR, "nginx-h3.conf.template")

SERVICE = "h3-proxy"
PROFILE = "h3"
# The port nginx LISTENS on, inside the container. Fixed: the TCP and UDP
# publishes must arrive on the same number or Alt-Svc, which names exactly one
# port for HTTP/3, would send clients where nothing is listening. Only the host
# side of the publish moves, through VTOP_H3_PORT.
PORT = 9443
# The variable that carries the moved host port INTO nginx, which cannot
# discover it. Everything that must follow the override reads the same
# expression with the same default.
PUBLISHED_PORT_EXPR = "${VTOP_H3_PORT:-" + str(PORT) + "}"
# The identity the proxy runs as, and the default it falls back to. Spelled out
# because gen-h3-certs.sh has to predict this exact resolution to know whether
# the container will be able to read the 0600 key it just minted.
IDENTITY_EXPR = "${VTOP_BENCH_UID:-1000}:${VTOP_BENCH_GID:-1000}"


@pytest.fixture(scope="module")
def compose():
    with open(COMPOSE_PATH, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    assert doc and doc.get("services"), (
        f"{COMPOSE_PATH} parsed to nothing with services in it; this module "
        "would then assert nothing at all"
    )
    return doc


@pytest.fixture(scope="module")
def proxy(compose):
    services = compose["services"]
    assert SERVICE in services, (
        f"the benchmark compose file has no {SERVICE!r} service, so the lab has "
        "no HTTP/3 counterparty and #484's target does not exist"
    )
    return services[SERVICE]


def test_the_h3_proxy_drops_every_capability(proxy):
    assert proxy.get("cap_drop") == ["ALL"], (
        "the proxy must drop ALL capabilities like every other lab service; it "
        f"declares cap_drop={proxy.get('cap_drop')!r}. Binding 9443 needs none "
        "of them, and a lab that grants one teaches that the baseline is "
        "negotiable"
    )


def test_the_h3_proxy_refuses_privilege_escalation(proxy):
    assert "no-new-privileges:true" in (proxy.get("security_opt") or []), (
        "no-new-privileges:true is missing from the proxy's security_opt "
        f"({proxy.get('security_opt')!r}); a setuid binary inside the image "
        "would otherwise be a way back out of the capability drop"
    )


def test_the_h3_proxy_runs_on_a_read_only_filesystem(proxy):
    assert proxy.get("read_only") is True, (
        "the proxy must run read_only; it declares "
        f"read_only={proxy.get('read_only')!r}"
    )
    # nginx wants a pid file and five temp paths whatever it is asked to do.
    # tmpfs is how read_only stays true — if these ever move to a volume, the
    # service has acquired durable state nobody audits.
    assert "/tmp" in (proxy.get("tmpfs") or []), (
        "a read-only nginx needs a writable /tmp for its pid file and temp "
        f"paths; the proxy declares tmpfs={proxy.get('tmpfs')!r}"
    )


def test_the_h3_proxy_does_not_run_as_root(proxy):
    # Not part of the stated baseline, but load-bearing HERE: a root nginx
    # master chowns its temp paths at startup, which needs the CAP_CHOWN that
    # cap_drop has just removed — so this service does not merely prefer
    # non-root, it does not start as root. The lint keeps a well-meant
    # simplification from turning into a boot failure nobody can explain.
    assert proxy.get("user"), (
        "the proxy must declare a non-root user; as root it would try to chown "
        "its temp paths without CAP_CHOWN and fail before it listens"
    )
    # And the exact expression, because gen-h3-certs.sh MIRRORS it. That script
    # refuses an identity compose would resolve to something the key's owner is
    # not, which means predicting this fallback — so a default changed here and
    # nowhere else turns its refusals into false ones (and its silences into
    # missed ones) with nothing to say why.
    assert proxy["user"] == IDENTITY_EXPR, (
        f"the proxy declares user={proxy['user']!r}; gen-h3-certs.sh predicts "
        f"{IDENTITY_EXPR!r} when it decides whether the identity compose will "
        "use can read a 0600 private key. The two must be the same expression, "
        "default included"
    )


def test_the_h3_proxy_is_bounded_in_pids_and_memory(proxy):
    assert isinstance(proxy.get("pids_limit"), int) and proxy["pids_limit"] > 0, (
        f"the proxy needs a pids_limit; it declares {proxy.get('pids_limit')!r}"
    )
    assert proxy.get("mem_limit"), (
        "the proxy needs a mem_limit, so a runaway proxy cannot starve the "
        "engine whose throughput is being measured beside it"
    )


def test_the_h3_proxy_publishes_only_to_loopback(proxy):
    ports = proxy.get("ports") or []
    assert ports, "the proxy publishes nothing, so nothing can reach the lab target"
    for mapping in ports:
        assert str(mapping).startswith("${VTOP_BIND_ADDR:-127.0.0.1}:"), (
            f"port mapping {mapping!r} does not bind through "
            "${VTOP_BIND_ADDR:-127.0.0.1}; the lab's default is loopback and "
            "re-exposing it must stay a deliberate, single-variable act"
        )


def test_the_h3_proxy_publishes_udp_and_tcp_on_the_same_port(proxy):
    # The CONTAINER port is what must match on both protocols: Alt-Svc names one
    # port for HTTP/3, and a UDP listener on a different number than the TCP one
    # would leave half of every upgrade unreachable. The HOST port is
    # overridable, because 9443 is an unremarkable choice for somebody else's
    # TLS service and a collision leaves the profile unable to start with an
    # error about a port rather than about the lab.
    ports = [str(p) for p in (proxy.get("ports") or [])]
    tcp = [p for p in ports if p.endswith(f":{PORT}/tcp")]
    udp = [p for p in ports if p.endswith(f":{PORT}/udp")]
    assert tcp and udp, (
        f"the proxy must publish container port {PORT} on BOTH tcp and udp; it publishes "
        f"{ports!r}. Without the UDP publish there is no QUIC path at all, and "
        "the lab silently becomes a second TCP proxy"
    )
    for spec in tcp + udp:
        assert f"{PORT}:{PORT}" in spec or PUBLISHED_PORT_EXPR in spec, (
            f"the host side of {spec!r} must default to {PORT}: the bundled scenario's "
            "endpoint_url and the harness both name that number when nothing is "
            "overridden, so a different default would leave every unconfigured run "
            "dialling a port the lab does not publish"
        )


def test_the_h3_proxy_pins_its_image(proxy):
    image = str(proxy.get("image", ""))
    assert ":" in image, (
        f"the proxy image {image!r} carries no tag, so it floats on :latest — "
        "the exact failure #507 fixed everywhere else in this file"
    )
    tag = image.rsplit(":", 1)[1]
    assert tag not in ("latest", "master", "main", "edge"), (
        f"the proxy image {image!r} is pinned to a moving tag; #507 was a "
        "withdrawn floating tag breaking every `compose up` in its pull phase, "
        "and a moving tag also cannot be bumped by Dependabot"
    )


def test_the_h3_proxy_refuses_to_start_without_its_tls_material(proxy):
    # HTTP/3 has no plaintext form, so a missing certificate is not a degraded
    # proxy — it is no proxy. Compose MKDIRs a missing bind source in the short
    # syntax, which would mount an empty directory where the certificate
    # belongs and fail deep inside nginx's TLS setup instead.
    mounts = [v for v in (proxy.get("volumes") or [])
              if isinstance(v, dict) and str(v.get("source", "")).endswith("tls")]
    assert mounts, (
        "the proxy does not bind-mount ./tls in the long syntax; the short "
        "`src:dst` form creates a missing source as a directory, which turns "
        "'you never ran gen-h3-certs.sh' into an obscure TLS failure"
    )
    mount = mounts[0]
    assert (mount.get("bind") or {}).get("create_host_path") is False, (
        f"the ./tls mount {mount!r} must set bind.create_host_path: false, so "
        "`up` refuses instead of inventing an empty certificate directory"
    )
    assert mount.get("read_only") is True, (
        "the proxy only ever reads its certificate; the mount must be read_only"
    )


def test_the_h3_proxy_stays_on_its_own_profile(proxy):
    assert proxy.get("profiles") == [PROFILE], (
        f"the proxy must sit on the {PROFILE!r} profile alone; it declares "
        f"profiles={proxy.get('profiles')!r}. Without a profile every plain "
        "`compose up` in this repository starts an HTTP/3 proxy that only one "
        "epic's measurements use"
    )


def test_no_other_service_joins_the_h3_profile(compose):
    joined = sorted(name for name, svc in compose["services"].items()
                    if PROFILE in ((svc or {}).get("profiles") or []))
    assert joined == [SERVICE], (
        f"the {PROFILE!r} profile should start exactly the proxy; it would "
        f"start {joined}. The profile is what keeps the h3 target off every "
        "unrelated run"
    )


def test_the_default_profile_still_starts_only_the_store(compose):
    # A service with no `profiles` key is in the default profile. The lab's
    # default must stay "MinIO and its bucket init" — that is what every
    # unprofiled scenario in the suite was measured against, and adding to it
    # would change those numbers without changing a single scenario file.
    default = sorted(name for name, svc in compose["services"].items()
                     if not ((svc or {}).get("profiles")))
    assert default == ["minio", "minio-init"], (
        f"the default profile now starts {default}; every scenario that does "
        "not name a profile was measured against minio + minio-init alone"
    )


def test_the_h3_proxy_waits_for_the_store_it_fronts(proxy):
    depends = proxy.get("depends_on") or {}
    assert (depends.get("minio") or {}).get("condition") == "service_healthy", (
        "the proxy must wait for MinIO to be healthy "
        f"(depends_on={depends!r}); a proxy that starts first answers the "
        "lab's first requests with 502s that read as transport failures"
    )


# --------------------------------------------------------------------------
# The authority: the one number nginx cannot find out for itself
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def template():
    with open(TEMPLATE_PATH, encoding="utf-8") as fh:
        return fh.read()


def _directive_lines(text):
    """The template's directives, with comments and blank lines dropped.

    Every port number in this file that is allowed to be a literal lives in a
    comment explaining a number that used to be wrong, so a lint that read the
    raw text would be reading the explanation rather than the configuration.
    """
    lines = []
    for raw in text.splitlines():
        body = raw.split("#", 1)[0].strip()
        if body:
            lines.append(body)
    return lines


def test_the_proxy_is_told_the_host_port_it_is_published_on(proxy):
    env = proxy.get("environment") or {}
    assert env.get("VTOP_H3_ADVERTISED_PORT") == PUBLISHED_PORT_EXPR, (
        "the proxy must be handed the PUBLISHED port as "
        f"VTOP_H3_ADVERTISED_PORT={PUBLISHED_PORT_EXPR!r}; it declares "
        f"{env.get('VTOP_H3_ADVERTISED_PORT')!r}. nginx sees only the port it "
        "bound, so without this it advertises Alt-Svc for a port nothing is "
        "published on and reconstructs an HTTP/3 client's signed Host with the "
        "wrong port — which MinIO answers with SignatureDoesNotMatch, a failure "
        "that reads as a credential bug. The expression must be the publish's "
        "own, so the two cannot drift"
    )


def test_the_config_nginx_reads_is_the_one_the_template_renders_to(proxy):
    # Three declarations have to agree — where the template is mounted, where
    # the image's envsubst pass writes it, and which file nginx is started with.
    # If they disagree nginx starts against a path that does not exist and the
    # profile never comes up, with an error about a missing file rather than
    # about a lab that was wired together wrong.
    env = proxy.get("environment") or {}
    template_dir = env.get("NGINX_ENVSUBST_TEMPLATE_DIR", "/etc/nginx/templates")
    output_dir = env.get("NGINX_ENVSUBST_OUTPUT_DIR", "/etc/nginx/conf.d")
    suffix = env.get("NGINX_ENVSUBST_TEMPLATE_SUFFIX", ".template")

    mounted = [str(v) for v in (proxy.get("volumes") or []) if isinstance(v, str)]
    templates = [m for m in mounted
                 if m.split(":")[1].startswith(template_dir + "/")
                 and m.split(":")[1].endswith(suffix)]
    assert len(templates) == 1, (
        f"exactly one {suffix} file must be mounted under {template_dir}; the "
        f"proxy mounts {mounted!r}. That mount is the only way the published "
        "port reaches nginx"
    )
    target = templates[0].split(":")[1]
    assert templates[0].endswith(":ro"), (
        f"the template mount {templates[0]!r} must be read-only; nothing in the "
        "container has any business editing the lab's configuration"
    )
    rendered = posixpath.join(output_dir, posixpath.basename(target)[:-len(suffix)])
    command = [str(c) for c in (proxy.get("command") or [])]
    assert rendered in command, (
        f"the envsubst pass renders {target} to {rendered}, but the proxy is "
        f"started with {command!r} — nginx would read a different file than the "
        "one with the published port substituted into it, or none at all"
    )
    assert any(output_dir == t or output_dir.startswith(str(t).rstrip("/") + "/")
               for t in (proxy.get("tmpfs") or [])), (
        f"the rendered config goes to {output_dir}, which is not one of the "
        f"proxy's tmpfs paths ({proxy.get('tmpfs')!r}); read_only makes every "
        "other directory unwritable and the image's template pass answers that "
        "by rendering nothing at all"
    )


def test_the_template_pass_substitutes_only_the_labs_own_placeholders(proxy, template):
    # envsubst replaces every variable name it is handed, and unfiltered it is
    # handed every name in the container's environment — while the file it is
    # rendering is made of nginx's own $host, $status, $remote_addr. The filter
    # is what keeps the templating pass from eating the configuration, and a
    # placeholder outside it would silently survive into the rendered file.
    env = proxy.get("environment") or {}
    pattern = env.get("NGINX_ENVSUBST_FILTER")
    assert pattern, (
        "NGINX_ENVSUBST_FILTER is unset, so the image's template pass would "
        "substitute every name in the environment — including any that collides "
        f"with one of nginx's own variables in {os.path.basename(TEMPLATE_PATH)}"
    )
    placeholders = sorted(set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", template)))
    assert placeholders, (
        f"{os.path.basename(TEMPLATE_PATH)} carries no ${{...}} placeholder at "
        "all, so it is not a template and the published port reaches nginx "
        "nowhere — this whole mechanism would be inert"
    )
    for name in placeholders:
        assert re.search(pattern, name), (
            f"the template's ${{{name}}} does not match NGINX_ENVSUBST_FILTER "
            f"{pattern!r}, so it is never substituted; nginx then reads it as a "
            "variable of its own and refuses to start with 'unknown variable'"
        )
        assert name in env, (
            f"the template expects ${{{name}}} but the proxy service does not "
            f"set it (it sets {sorted(env)}), so it renders empty"
        )


def _published_port_variable(directives):
    """The variable the template derives a client-facing port from.

    Discovered rather than spelled out, so this lints the WIRING: it is
    whichever `map` resolves to the ${...} placeholder compose fills with the
    published port. A rename stays fine; losing the connection does not.
    """
    for index, line in enumerate(directives):
        opened = re.match(r"^map\s+\$\S+\s+\$([A-Za-z_][A-Za-z0-9_]*)\s*\{", line)
        if not opened:
            continue
        body = []
        for inner in directives[index + 1:]:
            if inner.startswith("}"):
                break
            body.append(inner)
        if any("${" in entry for entry in body):
            return opened.group(1)
    return None


def test_the_advertised_and_forwarded_authority_never_name_a_fixed_port(template):
    # The defect this exists for, in the two places it appeared: Alt-Svc told
    # every upgrading client to retry on the container's port, and the Host
    # rebuilt for HTTP/3 (where nginx leaves $http_host empty) carried that same
    # port into the signature MinIO recomputes. Both are correct only while the
    # publish happens to use the listener's number, which VTOP_H3_PORT exists to
    # stop being true.
    directives = _directive_lines(template)
    advertised = [line for line in directives if "Alt-Svc" in line]
    assert len(advertised) == 1, (
        f"expected exactly one Alt-Svc directive, found {advertised!r}; it is "
        "the only way a TCP client learns the HTTP/3 listener exists"
    )
    rebuilt = [line for line in directives
               if line.startswith('""') and "$host" in line]
    assert len(rebuilt) == 1, (
        f"expected exactly one empty-$http_host fallback, found {rebuilt!r}; it "
        "is what an HTTP/3 request's Host header is rebuilt from, because nginx "
        "leaves $http_host empty on HTTP/3"
    )
    published = _published_port_variable(directives)
    assert published, (
        "no map in the template resolves to the ${...} placeholder compose "
        "fills with the published port, so nothing in this file can follow "
        "VTOP_H3_PORT and the whole template mechanism is inert"
    )
    for line in advertised + rebuilt:
        assert str(PORT) not in line, (
            f"{line!r} names the container port {PORT} literally. That is the "
            "port nginx listens on, not the one the client dialled, and under "
            "VTOP_H3_PORT they differ: the advertisement then points at nothing "
            "and the rebuilt Host fails the client's SigV4 signature with "
            "SignatureDoesNotMatch"
        )
        assert f"${published}" in line, (
            f"{line!r} does not take its port from ${published}, the one value "
            "in this file that follows the publish. $server_port is the trap "
            "here: it is the port nginx BOUND, correct only while the publish "
            "happens to use the same number, which is exactly what VTOP_H3_PORT "
            "stops being true"
        )

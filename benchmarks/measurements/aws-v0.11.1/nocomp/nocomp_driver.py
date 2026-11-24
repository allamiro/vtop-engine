#!/usr/bin/env python3
"""nocomp bench driver (Linux, stdlib only): a port of the paper repo's rq3-cost-ablation/code/diag_nocomp.py and
diag_rusage.py. Engine config = lib/vtoplab.py Run._write_config; steady state = exp_cost.one_run_mode (bytes committed
by the last size-sealed batch over the time of that commit); CPU = wait4 rusage of `vtopctl run` (user, system).
  gen <dest> <bytes> <seed>       deterministic BGL-shaped synthetic text (MT19937 getrandbits only)
  unzip <zip> <member> <dest>
  campaign <out> <input> <dataset> <reps> <name=vtopctl,...> <placement=dir,...>
"""
import gzip, hashlib, json, os, random, shutil, signal, sqlite3, statistics, subprocess, sys, time
from pathlib import Path

VARIANTS = {"none-nocomp-w1": ("none", "none", 1), "none-nocomp": ("none", "none", 8),   # (checksum, compression, width)
            "sha256-nocomp-w1": ("sha256", "none", 1), "sha256-nocomp": ("sha256", "none", 8), "size-only": ("none", "zstd", 8)}
CONFIG = """# nocomp bench: equivalent of vtoplab.Run._write_config ({name})
engine:
  name: lab
  tenant: lab
  state_store: "sqlite://{db}"
  work_dir: {work}
batching:
  max_records: 1000000
  max_bytes: 16777216
  max_batch_age_seconds: 2
  idle_poll_interval_ms: 200
  max_concurrent_batches: {width}
compression:
  type: {comp}
  level: 3
checksum:
  algorithm: {ck}
sources:
  kafka: {{enabled: false, bootstrap_servers: ["localhost:9092"], consumer_group: lab}}
  file: {{enabled: true, paths: ["{glob}"]}}
  syslog_spool: {{enabled: false, paths: ["{glob}"]}}
upload:
  backend: localfs
  local_path: {objects}
  bucket: archive
  require_strong_verification: {strong}
"""
MSGS = ["instruction cache parity error corrected", "generating core.{a}", "data TLB error interrupt",
        "{a} double-hummer alignment exceptions", "CE sym {b}, at 0x{h}, mask 0x{m}", "NodeCard is not fully functional",
        "total of {b} ddr error(s) detected and corrected", "rts: kernel terminated for reason {a}",
        "machine check interrupt (bit=0x{m}): L2 dcache unit data parity error", "program interrupt: fp cr update.............{b}",
        "ciod: failed to read message prefix on control stream (CioStream socket to 172.16.{b}.{c}:{a}",
        "critical input interrupt (unit=0x{m} bit=0x{m}): warning for torus z+ wire", "Lustre mount FAILED : bglio{b} : block_id : location",
        "ciod: Error loading /bgl/apps/run{a}/a.out: invalid or missing program image, No such file or directory",
        "idoproxydb hit ASSERT condition: ASSERT expression=0 Source file=idotransportmgr.cpp Source line={a} Function=int IdoTransportMgr::SendPacket(IdoUdpMgr*, BglCtlPavTrace*)"]
COMPS = ["RAS KERNEL INFO"] * 10 + ["RAS KERNEL FATAL", "RAS APP FATAL", "RAS MMCS ERROR", "RAS DISCOVERY WARNING", "RAS LINKCARD FATAL", "RAS MONITOR FAILURE"]


def gen(dest, target, seed):
    g = random.Random(int(seed)).getrandbits
    t, n, buf, day = 1117838570, 0, [], {}
    with open(dest, "w", newline="\n") as fh:
        while n < int(target):
            t += g(1)
            if t not in day:
                day = {t: (time.strftime("%Y.%m.%d", time.gmtime(t)), time.strftime("%Y-%m-%d-%H.%M.%S", time.gmtime(t)))}
            d, ts = day[t]
            node = f"R{g(6):02d}-M{g(1)}-N{g(4):X}-C:J{2 + g(4):02d}-U{1 + 10 * g(1):02d}"
            msg = MSGS[g(8) % len(MSGS)].format(a=g(12), b=g(8), c=g(8), h=f"{g(32):08x}", m=f"{g(8):02x}")
            line = f"{'-' if g(4) else 'KERNDTLB'} {t} {d} {node} {ts}.{g(20) % 1000000:06d} {node} {COMPS[g(8) % len(COMPS)]} {msg}\n"
            buf.append(line); n += len(line)
            if len(buf) >= 20000:
                fh.write("".join(buf)); buf = []
        fh.write("".join(buf))


def committed(db):
    if not db.exists():
        return 0
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
        try:
            rows = con.execute("SELECT progress_end_json FROM batches WHERE state='source_committed'").fetchall()
        finally:
            con.close()
    except sqlite3.OperationalError:
        return 0
    return max((int(json.loads(r[0])["end_byte"]) for r in rows), default=0)


def reap(p):
    """wait4 with WNOHANG; (status, rusage) or None while running."""
    pid, st, ru = os.wait4(p.pid, os.WNOHANG)
    if pid:
        p.returncode = os.waitstatus_to_exitcode(st)
        return st, ru
    return None


def fsinfo(path):
    return subprocess.run(["findmnt", "-no", "FSTYPE,SOURCE,OPTIONS", "-T", str(path)], capture_output=True, text=True).stdout.strip()


def one(out, build, vtopctl, place, pdir, inp, ds, vn, rep):
    ck, comp, width = VARIANTS[vn]
    root = pdir / "run"
    shutil.rmtree(root, ignore_errors=True)
    objects, state, work = root / "objects", root / "state", root / "work"
    for d in (objects, state, work):
        d.mkdir(parents=True)
    db, cfg, rid = state / "vtop.db", root / "config.yaml", f"{place}-{build}-{vn}-r{rep}"
    cfg.write_text(CONFIG.format(name=vn, db=db, work=work, width=width, comp=comp, ck=ck, glob=inp.parent / "*.log",
                                 objects=objects, strong=str(ck != "none").lower()))
    shutil.copy(cfg, out / "configs" / f"{rid}.yaml")
    size = inp.stat().st_size
    os.sync(); time.sleep(2)                      # settle: the previous run's writeback, a quiet start
    load1, started = os.getloadavg()[0], time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    log = root / "engine.jsonl"
    t0 = time.perf_counter()
    with open(log, "wb") as lf:
        p = subprocess.Popen([vtopctl, "run", "--config", str(cfg)], stdout=lf, stderr=subprocess.STDOUT,
                             env=dict(os.environ, VTOP_LOG_FORMAT="json"))
    series, last, done = [], 0, None
    while True:
        c = committed(db)
        if c != last:
            series.append((time.perf_counter() - t0, c)); last = c
        done = reap(p)
        if done or c >= size or time.perf_counter() - t0 > 600:
            break
        time.sleep(0.01)
    wall = time.perf_counter() - t0
    early_exit = done is not None
    if not done:
        p.send_signal(signal.SIGTERM)
        t1 = time.time()
        while not (done := reap(p)):
            if time.time() - t1 > 60:
                p.kill(); _, st, ru = os.wait4(p.pid, 0); done = (st, ru); break
            time.sleep(0.01)
    st, ru = done
    states = {}
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
        states = dict(con.execute("SELECT state, COUNT(*) FROM batches GROUP BY state").fetchall()); con.close()
    except sqlite3.Error as e:
        states = {"error": str(e)}
    objs = [f for f in objects.rglob("*") if f.is_file() and not f.name.endswith(".manifest.json")]
    steady = series[-2] if len(series) >= 2 else (series[-1] if series else (None, 0))
    gib, cpu = size / 2**30, ru.ru_utime + ru.ru_stime
    rec = {"mode": "run", "dataset": ds, "variant": vn, "checksum": ck, "compression": comp, "concurrency": width,
           "batch_mib": 16, "rep": rep, "build": build, "placement": place, "store_fs": fsinfo(objects), "bytes": size,
           "wall_s": round(wall, 4), "cpu_s": round(cpu, 4), "user_s": round(ru.ru_utime, 4), "sys_s": round(ru.ru_stime, 4),
           "cpu_s_per_gib": round(cpu / gib, 4), "user_s_per_gib": round(ru.ru_utime / gib, 4),
           "sys_s_per_gib": round(ru.ru_stime / gib, 4), "max_rss_mib": round(ru.ru_maxrss / 1024, 1),  # Linux: KiB
           "minflt": ru.ru_minflt, "majflt": ru.ru_majflt, "nvcsw": ru.ru_nvcsw, "nivcsw": ru.ru_nivcsw,
           "inblock": ru.ru_inblock, "oublock": ru.ru_oublock, "mib_per_s_end_to_end": round(size / 2**20 / wall, 2),
           "steady_bytes": steady[1], "steady_s": None if steady[0] is None else round(steady[0], 4),
           "mib_per_s": round(steady[1] / 2**20 / steady[0], 2) if steady[0] else None, "commits_observed": len(series),
           "drained": last >= size, "states": states, "engine_exit": os.waitstatus_to_exitcode(st), "engine_exited_early": early_exit,
           "object_files": len(objs), "object_bytes": sum(f.stat().st_size for f in objs), "load1_before": round(load1, 2),
           "started_at": started}
    with open(log, "rb") as fi, gzip.open(out / "engine-logs" / f"{rid}.jsonl.gz", "wb") as fo:
        shutil.copyfileobj(fi, fo)
    shutil.rmtree(root, ignore_errors=True)
    return rec


def summary(recs):
    keys = ["mib_per_s", "cpu_s_per_gib", "user_s_per_gib", "sys_s_per_gib", "max_rss_mib"]
    cells, lines = {}, ["placement build variant n drained | median " + " ".join(keys)]
    for r in recs:
        cells.setdefault((r["placement"], r["build"], r["variant"]), []).append(r)
    med = lambda rs, k: statistics.median([r[k] for r in rs if r[k] is not None] or [float("nan")])
    ratio = lambda a, b: a / b if b else float("nan")
    for (pl, b, v), rs in cells.items():
        lines.append(f"{pl} {b} {v} {len(rs)} {sum(r['drained'] for r in rs)} | " + " ".join(f"{med(rs, k):.3f}" for k in keys))
    for pl, b in sorted({(k[0], k[1]) for k in cells}):
        for ck in ("none", "sha256"):
            w1, w8 = cells.get((pl, b, f"{ck}-nocomp-w1")), cells.get((pl, b, f"{ck}-nocomp"))
            if w1 and w8:
                lines.append(f"{pl} {b} {ck}-nocomp W8/W1: sys/GiB x{ratio(med(w8, 'sys_s_per_gib'), med(w1, 'sys_s_per_gib')):.2f}, "
                             f"cpu/GiB x{ratio(med(w8, 'cpu_s_per_gib'), med(w1, 'cpu_s_per_gib')):.2f}, "
                             f"MiB/s x{ratio(med(w8, 'mib_per_s'), med(w1, 'mib_per_s')):.2f}")
    return "\n".join(lines) + "\n"


def campaign(out, inp, ds, reps, builds, places):
    out, src = Path(out), Path(inp)
    builds = [b.split("=", 1) for b in builds.split(",")]
    places = [(n, Path(d)) for n, d in (p.split("=", 1) for p in places.split(","))]
    (out / "configs").mkdir(exist_ok=True); (out / "engine-logs").mkdir(exist_ok=True)
    env = {"vtop_engine_ref": os.environ.get("NC_ENGINE_REF", ""), "vtop_engine_sha": os.environ.get("NC_ENGINE_SHA", ""),
           "builds": {}, "input_kind": os.environ.get("NC_INPUT_KIND", ds), "input_source": os.environ.get("NC_INPUT_SOURCE", ""),
           "input_bytes": src.stat().st_size, "uname": " ".join(os.uname()), "python": sys.version, "nproc": os.cpu_count(),
           "placements": {}, "variants": VARIANTS, "reps": int(reps), "order": "per rep: variants in the order above, builds alternated per variant"}
    for name, exe in builds:
        v = subprocess.run([exe, "--version"], capture_output=True, text=True)
        env["builds"][name] = {"vtopctl": exe, "vtopctl_version": (v.stdout or v.stderr).strip(),
                               "vtopctl_sha256": hashlib.sha256(Path(exe).read_bytes()).hexdigest()}
    env["vtopctl_version"] = env["builds"][builds[0][0]]["vtopctl_version"]
    inputs = {}
    for pl, d in places:
        shutil.rmtree(d, ignore_errors=True); (d / "in").mkdir(parents=True)
        dst = d / "in" / f"{ds}.log"
        try:
            os.link(src, dst)                     # same filesystem: no copy
        except OSError:
            shutil.copyfile(src, dst)
        inputs[pl] = dst
        env["placements"][pl] = {"dir": str(d), "fs": fsinfo(d)}
    (out / "environment.json").write_text(json.dumps(env, indent=1))
    recs, bad = [], 0
    for pl, d in places:
        for rep in range(int(reps)):
            for vn in VARIANTS:
                for b, exe in builds:
                    r = one(out, b, exe, pl, d, inputs[pl], ds, vn, rep)
                    recs.append(r); bad += not r["drained"]
                    with open(out / "results.jsonl", "a") as fh:
                        fh.write(json.dumps(r) + "\n")
                    print(f"{pl:5s} {b} {vn:16s} r{rep} {r['mib_per_s']} MiB/s user/GiB={r['user_s_per_gib']:.3f} "
                          f"sys/GiB={r['sys_s_per_gib']:.3f} rss={r['max_rss_mib']}MiB drained={r['drained']}", flush=True)
        shutil.rmtree(d, ignore_errors=True)
    (out / "summary.txt").write_text(summary(recs)); print(summary(recs), end="")
    return 1 if bad else 0


if __name__ == "__main__":
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "gen":
        gen(*args)
    elif cmd == "unzip":
        import zipfile
        with zipfile.ZipFile(args[0]) as z, z.open(args[1]) as fi, open(args[2], "wb") as fo:
            shutil.copyfileobj(fi, fo, 1 << 20)
    elif cmd == "campaign":
        sys.exit(campaign(*args))
    else:
        sys.exit(f"unknown command {cmd}")

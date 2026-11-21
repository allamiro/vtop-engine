"""Synthetic web-service log lines, JSONL, for the long-link rig.

Usage: python3 gen.py BYTES SEED > data.jsonl

The field entropy is tuned so that zstd level 3 compresses a 16 MiB batch
about 10.7 times (a 1.5 MiB object), close to the 11x an external benchmark
measured on its own data, so the rig's objects are the size that benchmark's
were: well under the multipart threshold, one PUT each.
"""

import json
import random
import sys

PATHS = ["/api/v1/orders", "/api/v1/users", "/api/v2/search", "/static/app.js", "/login", "/healthz",
         "/api/v1/cart", "/checkout", "/api/v1/items/{id}", "/metrics"]
METHODS = ["GET"] * 7 + ["POST"] * 2 + ["PUT"]
AGENTS = ["Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0", "curl/8.5.0", "Go-http-client/1.1",
          "python-requests/2.31", "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) Safari/605.1.15"]
LEVELS = ["INFO"] * 8 + ["WARN", "ERROR"]
SERVICES = ["checkout", "inventory", "gateway", "auth", "search"]


def main() -> None:
    target = int(sys.argv[1])
    r = random.Random(int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    written = 0
    ts = 1763700000.0
    out = sys.stdout
    while written < target:
        ts += r.randrange(1, 20) * 0.0001
        rec = {
            "ts": f"{ts:.6f}",
            "level": r.choice(LEVELS),
            "svc": r.choice(SERVICES),
            "host": f"ip-10-0-1-{r.randrange(40)}",
            "method": r.choice(METHODS),
            "path": r.choice(PATHS).replace("{id}", str(r.randrange(1000))),
            "status": r.choice([200] * 12 + [201, 204, 301, 404, 500]),
            "bytes": r.randrange(1, 10) * 10000,
            "latency_ms": int(r.expovariate(1 / 40.0)),
            "client": f"10.20.0.{r.randrange(100)}",
            "trace_id": f"00000{r.getrandbits(10):03x}",
            "ua": r.choice(AGENTS),
            "msg": "request completed",
        }
        line = json.dumps(rec, separators=(",", ":")) + "\n"
        out.write(line)
        written += len(line)


if __name__ == "__main__":
    main()

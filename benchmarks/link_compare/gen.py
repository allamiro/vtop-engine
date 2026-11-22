#!/usr/bin/env python3
"""The link comparison's workload (#747): N seeded, incompressible files and their sha256 manifest.

    python3 gen.py OUT_DIR FILES FILE_MIB SEED

Writes OUT_DIR/files/f000.bin ... and OUT_DIR/manifest.json
({name: [size, sha256]}), after removing what an earlier run left there. The
bytes come from a seeded generator, so a seed and a shape always give the same
files, and they do not compress, so no tool's compression can confound the
wire bytes. The harness's read-back compares every stored object against this
manifest; no tool's own report is ever read for it.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import sys

CHUNK = 1 << 20


def generate(out_dir: str, files: int, file_mib: float, seed: int) -> dict[str, list]:
    if files < 1 or file_mib <= 0:
        raise ValueError("at least one file of a positive size")
    target = os.path.join(out_dir, "files")
    shutil.rmtree(target, ignore_errors=True)
    os.makedirs(target)
    size = int(file_mib * CHUNK)
    manifest = {}
    for index in range(files):
        rng = random.Random(f"{seed}:{index}")
        name = f"f{index:03d}.bin"
        digest = hashlib.sha256()
        with open(os.path.join(target, name), "wb") as handle:
            left = size
            while left:
                block = rng.getrandbits(8 * min(CHUNK, left)).to_bytes(min(CHUNK, left), "little")
                handle.write(block)
                digest.update(block)
                left -= len(block)
        manifest[name] = [size, digest.hexdigest()]
    tmp = os.path.join(out_dir, "manifest.json.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=1, sort_keys=True)
    os.replace(tmp, os.path.join(out_dir, "manifest.json"))
    return manifest


def main(argv: list[str] | None = None) -> int:
    args = (argv if argv is not None else sys.argv[1:])
    if len(args) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    out_dir, files, file_mib, seed = args[0], int(args[1]), float(args[2]), int(args[3])
    manifest = generate(out_dir, files, file_mib, seed)
    # The manifest on stdout too: the harness reads it from here, the volume is the tools'.
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

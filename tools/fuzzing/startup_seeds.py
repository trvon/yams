#!/usr/bin/env python3
"""Create deterministic startup-fuzzer seeds, preserving any existing different files."""
import argparse
from pathlib import Path


def seeds():
    # Stable StartupAction vocabulary in startup_fuzz_support.h; high bit selects vectors.
    advance, fail, shutdown, finish, idle, busy, pressure, calm, tick, deadline, probe, cancel, reset = range(13)
    yield "ready", bytes([advance] * 7)
    yield "vectors", bytes([advance | 0x80] * 7)
    yield "pressure-before-ready", bytes(
        [pressure] + [advance] * 4 + [deadline, probe] + [advance] * 2
        + [deadline, cancel, deadline, shutdown, finish]
    )
    yield "idle-pressure", bytes([advance] * 7 + [busy, pressure, deadline, idle, tick, calm, probe])
    for phase in range(6):
        yield f"failure-{phase}", bytes(
            [advance] * phase + [fail, deadline, advance, shutdown, finish, advance, reset]
            + [advance] * 7
        )
    # IEEE-754 bits, little-endian, interpreted as (left,right) component pairs.
    bits = [0, 0x80000000, 0x7F800000, 0xFF800000, 0x7FC00000, 1, 0x7F7FFFFF, 0x3F800000]
    yield "float-boundaries", b"".join(x.to_bytes(4, "little") * 2 for x in bits)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    for name, data in seeds():
        path = args.directory / name
        if path.exists():
            if path.read_bytes() != data:
                raise ValueError(f"Refusing to replace existing different seed: {path}")
        else:
            with path.open("xb") as output:
                output.write(data)
        print(path)


if __name__ == "__main__":
    main()

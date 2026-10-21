#!/usr/bin/env python3
"""Check that an instrumented ELF library uses its compiler's TSan runtime."""

import argparse
import re
import subprocess
from pathlib import Path


def check_library(library: Path, compiler: str, readelf: str) -> None:
    symbols = subprocess.run(
        [readelf, "--dyn-syms", "--wide", str(library)],
        check=True, capture_output=True, text=True,
    ).stdout
    if not any(
        " UND " in line and line.split()[-1].split("@")[0] == "__tsan_func_entry"
        for line in symbols.splitlines()
    ):
        raise AssertionError(f"{library}: expected TSan function instrumentation")

    dynamic = subprocess.run(
        [readelf, "--dynamic", "--wide", str(library)],
        check=True, capture_output=True, text=True,
    ).stdout
    needed = re.findall(r"\(NEEDED\).*\[([^\]]+)\]", dynamic)
    foreign_prefix = "libtsan.so" if compiler == "clang" else "libclang_rt.tsan"
    foreign = [name for name in needed if name.startswith(foreign_prefix)]
    if foreign:
        raise AssertionError(
            f"{library}: {compiler}-instrumented library introduces foreign TSan "
            f"runtime(s): {', '.join(foreign)}; let the compiler driver select its runtime"
        )
    print(f"{library}: TSan instrumentation present; no foreign {compiler} runtime")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--compiler", choices=("clang", "gcc"), required=True)
    parser.add_argument("--readelf", required=True)
    options = parser.parse_args()
    check_library(options.library, options.compiler, options.readelf)

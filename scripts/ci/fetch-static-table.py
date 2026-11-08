#!/usr/bin/env python3
"""Fetch a static-embedding table for release packaging and verify its sha256.

The URL and sha256 come from the catalog in src/cli/static_tables.cpp, so the pin has one
source. Release jobs use this instead of running the freshly built yams-cli: that binary
cannot always run from the build tree (Windows load-time DLLs) or verify TLS on the runner
(macOS x86_64), which failed the 0.22.0 release.

Usage: fetch-static-table.py <table-name> <output-dir>
Prints the path of the verified file.
"""

import hashlib
import re
import subprocess
import sys
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[2] / "src" / "cli" / "static_tables.cpp"
LITERAL = re.compile(r'"((?:[^"\\]|\\.)*)"')
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def catalog_entry(name):
    source = CATALOG.read_text(encoding="utf-8")
    start = source.find('{"' + name + '",')
    if start < 0:
        sys.exit(f"static table {name!r} not found in {CATALOG}")
    end = source.find("},", start)
    literals = LITERAL.findall(source[start:end])
    # Entry layout: name, URL (one or more adjacent literals), sha256, description.
    for i, literal in enumerate(literals[1:], start=1):
        if SHA256.match(literal):
            return "".join(literals[1:i]), literal
    sys.exit(f"no sha256 found in the catalog entry for {name!r}")


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    name, out_dir = sys.argv[1], Path(sys.argv[2])
    url, expected = catalog_entry(name)
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / url.rsplit("/", 1)[-1]
    partial = target.with_name(target.name + ".part")

    print(f"Fetching {name}\n  from {url}", file=sys.stderr)
    subprocess.run(
        ["curl", "-fsSL", "--retry", "3", "--retry-all-errors", "-o", str(partial), url],
        check=True,
    )
    actual = sha256_of(partial)
    if actual != expected:
        partial.unlink()
        sys.exit(f"sha256 mismatch for {name}: expected {expected}, got {actual}")
    partial.replace(target)
    print(target)


if __name__ == "__main__":
    main()

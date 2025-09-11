#!/usr/bin/env python3
"""
YAMS interface change detector with version check:
 - Detects changes to public YAMS surfaces (headers/WIT/schemas)
 - If changes detected, requires either:
   * Interface version bump in docs/spec/interface_versions.json (per-interface), OR
   * Changelog/stability PBI update acknowledging the change

Configure base ref via env YAMS_SEMVER_BASE_REF (default: origin/main).
Usage (from repo root):
  python3 scripts/ci/check_yams_semver.py
Exit codes:
  0: no surface changes, or changes + version/changelog present
  1: surface changes detected without version/changelog touch
  2: git error
"""
import fnmatch
import json
import os
import pathlib
import re
import subprocess
import sys

SURFACE_PATTERNS = [
    "include/yams/plugins/*.h",
    "docs/spec/wit/*.wit",
    "docs/spec/schemas/*.json",
]

CHANGE_TOKENS = [
    "CHANGELOG.md",
    "docs/CHANGELOG.md",
    "docs/pbi/pbi-008-01-kernel-stability-program.md",
]

VERSION_FILE = "docs/spec/interface_versions.json"

def git_diff_names(base: str) -> list[str]:
    try:
        out = subprocess.check_output(["git", "diff", "--name-only", f"{base}..HEAD"], text=True)
        files = [l.strip() for l in out.splitlines() if l.strip()]
        return files
    except subprocess.CalledProcessError:
        out = subprocess.check_output(["git", "diff", "--name-only", "HEAD~1..HEAD"], text=True)
        return [l.strip() for l in out.splitlines() if l.strip()]

def any_match(paths: list[str], patterns: list[str]) -> bool:
    for p in paths:
        for pat in patterns:
            if fnmatch.fnmatch(p, pat):
                return True
    return False

def main() -> int:
    base = os.environ.get("YAMS_SEMVER_BASE_REF", "origin/main")
    try:
        changed = git_diff_names(base)
    except Exception as e:  # noqa: BLE001
        print(f"[yams-semver] git diff failed: {e}", file=sys.stderr)
        return 2

    if not changed:
        print("[yams-semver] no changes detected")
        return 0

    surf_changed = any_match(changed, SURFACE_PATTERNS)
    if not surf_changed:
        print("[yams-semver] no public-surface changes detected")
        return 0

    print("[yams-semver] public-surface changes detected:")
    for f in changed:
        for pat in SURFACE_PATTERNS:
            if fnmatch.fnmatch(f, pat):
                print(f"  - {f}")

    # Read version files at base and head
    def read_versions_at(ref: str):
        try:
            out = subprocess.check_output(["git", "show", f"{ref}:{VERSION_FILE}"], text=True)
            return json.loads(out)
        except Exception:
            return None

    base_versions = read_versions_at(base)
    head_versions = None
    try:
        with open(VERSION_FILE, "r", encoding="utf-8") as f:
            head_versions = json.load(f)
    except Exception:
        head_versions = None

    # Derive interface keys from changed files
    def derive_key(path: str) -> str | None:
        p = pathlib.Path(path)
        s = str(p)
        # C-ABI header for plugin ABI
        if s.endswith("include/yams/plugins/abi.h"):
            return "plugin_abi"
        # Plugin headers like include/yams/plugins/object_storage_v1.h
        if fnmatch.fnmatch(s, "include/yams/plugins/*.h"):
            return p.stem  # e.g., object_storage_v1
        # WIT files: docs/spec/wit/<iface>.wit
        if fnmatch.fnmatch(s, "docs/spec/wit/*.wit"):
            return p.stem
        # Schemas: docs/spec/schemas/<iface>.schema.json or <iface>.json
        if fnmatch.fnmatch(s, "docs/spec/schemas/*.json"):
            name = p.stem  # drops .json
            if name.endswith(".schema"):
                name = name[: -len(".schema")]
            return name
        return None

    expected_bumps = set(filter(None, (derive_key(f) for f in changed)))

    version_bumped = True
    missing = []
    if expected_bumps and base_versions and head_versions:
        for key in expected_bumps:
            if str(base_versions.get(key)) == str(head_versions.get(key)):
                version_bumped = False
                missing.append(key)

    # Accept if either (a) versions bumped or (b) changelog/stability doc touched
    changelog_touched = any(p in changed for p in CHANGE_TOKENS) or (VERSION_FILE in changed)
    if expected_bumps and not version_bumped and not changelog_touched:
        print("[yams-semver] ERROR: interface changed without version bump or changelog update"
            f"  Missing bumps for: {', '.join(missing)}"
            f"  Touch either {VERSION_FILE} with version increments and/or CHANGELOG.md",
            file=sys.stderr,
        )
        return 1

    if changelog_touched or version_bumped:
        print("[yams-semver] version bump/changelog present — OK")
        return 0

    print("[yams-semver] ERROR: public YAMS surfaces changed without changelog/version update."
        "  Please bump interface versions and update CHANGELOG.md or stability PBI."
        f"  Base ref: {base}",
        file=sys.stderr,
    )
    return 1

if __name__ == "__main__":
    raise SystemExit(main())

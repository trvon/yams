#!/usr/bin/env python3
"""Report where every reviewed configuration key is referenced.

Input keys come from two places: the YAMS_* environment allowlist, and the dotted
TOML keys literally resolved in ConfigResolver.cpp. For each key the report counts
references per tree bucket and assigns a verdict:

  KEEP        referenced by a test, bench, doc, script, or plugin
  CANDIDATE   referenced only by product code (and the allowlists): nothing outside
              src/ exercises or documents it, so removal needs no consumer migration
  NOT_IN_SRC  the key is listed but no product code mentions it (stale allowlist)

A verdict is evidence for review, not a decision. Key deletions must cite the
CANDIDATE rows of this report in the change description.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

SRC_ROOTS = ("src", "include", "tools")
BUCKET_ROOTS = {
    "src": ("src", "include", "tools"),
    "test": ("tests",),
    "bench": ("tests/benchmarks", "scripts"),
    "docs": ("docs",),
    "plugin": ("plugins",),
}
SKIP_DIRS = {"build", "builddir", "third_party", "subprojects", "external", "site",
             "public", "__pycache__", ".git", "node_modules"}
TEXT_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".py", ".md", ".toml",
                 ".txt", ".yml", ".yaml", ".json", ".sh", ".build", ".cfg", ".ini", ".lean"}
# Allowlists list every key by construction; they are not consumers.
SKIP_FILES = {"tests/scripts/production_environment_keys.txt",
              "tests/scripts/raw_environment_allowlist.txt",
              "tests/scripts/portability_allowlist.txt",
              "tests/scripts/configuration_reader_allowlist.txt"}
ENV_TOKEN_RE = re.compile(r"(?<![A-Z0-9_])(YAMS_[A-Z0-9_]+)(?![A-Z0-9_])")
TOML_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_.])([a-z_]+(?:\.[a-z_]+)+)(?![A-Za-z0-9_.])")
TOML_LITERAL_RE = re.compile(r"\"([a-z_]+(?:\.[a-z_]+)+)\"")


def read_allowlist(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")]


def resolver_toml_keys(resolver: Path) -> list[str]:
    return sorted(set(TOML_LITERAL_RE.findall(resolver.read_text(encoding="utf-8",
                                                                  errors="ignore"))))


def tracked_files(root: Path) -> set[str] | None:
    """Paths git tracks under root, or None when root is not a git work tree.

    Verdicts must be reproducible by a reviewer, so untracked local files (ignored bench
    harnesses, scratch scripts) never count as consumers.
    """
    if not (root / ".git").exists():
        # Not a repository root (a synthetic tree under a temp or build dir): scan it plainly.
        return None
    # Git exports GIT_DIR/GIT_WORK_TREE to hooks; inside a pre-push hook that would make git
    # answer for the hook's repository instead of `root`. Ask about `root` alone.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    try:
        top = subprocess.run(["git", "-C", str(root), "rev-parse", "--show-toplevel"],
                             capture_output=True, check=True, text=True, env=env).stdout.strip()
        if Path(top).resolve() != root.resolve():
            return None
        out = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], capture_output=True,
                             check=True, text=True, env=env).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return {entry for entry in out.split("\0") if entry}


def iter_text_files(root: Path, relative_roots: tuple[str, ...], tracked: set[str] | None):
    for relative in relative_roots:
        base = root / relative
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            rel = path.relative_to(root).as_posix()
            if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
                continue
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            if rel in SKIP_FILES:
                continue
            if tracked is not None and rel not in tracked:
                continue
            yield path


def bucket_counts(root: Path, relative_roots: tuple[str, ...],
                  exclude: tuple[str, ...] = (),
                  tracked: set[str] | None = None) -> tuple[Counter, Counter]:
    env: Counter = Counter()
    toml: Counter = Counter()
    for path in iter_text_files(root, relative_roots, tracked):
        rel = path.relative_to(root).as_posix()
        if any(rel.startswith(prefix) for prefix in exclude):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        env.update(ENV_TOKEN_RE.findall(text))
        toml.update(TOML_TOKEN_RE.findall(text))
    return env, toml


def build_report(root: Path, env_keys: list[str], toml_keys: list[str]) -> list[dict]:
    tracked = tracked_files(root)
    counts = {
        "src": bucket_counts(root, BUCKET_ROOTS["src"], tracked=tracked),
        # tests/ minus benchmarks, which are their own bucket.
        "test": bucket_counts(root, BUCKET_ROOTS["test"], exclude=("tests/benchmarks/",),
                              tracked=tracked),
        "bench": bucket_counts(root, BUCKET_ROOTS["bench"], tracked=tracked),
        "docs": bucket_counts(root, BUCKET_ROOTS["docs"], tracked=tracked),
        "plugin": bucket_counts(root, BUCKET_ROOTS["plugin"], tracked=tracked),
    }
    rows = []
    for kind, keys in (("env", env_keys), ("toml", toml_keys)):
        idx = 0 if kind == "env" else 1
        for key in keys:
            row = {"kind": kind, "key": key}
            for bucket, pair in counts.items():
                row[bucket] = pair[idx][key]
            if row["src"] == 0:
                row["verdict"] = "NOT_IN_SRC"
            elif row["test"] or row["bench"] or row["docs"] or row["plugin"]:
                row["verdict"] = "KEEP"
            else:
                row["verdict"] = "CANDIDATE"
            rows.append(row)
    rows.sort(key=lambda r: (r["kind"], r["verdict"], r["key"]))
    return rows


COLUMNS = ("kind", "key", "src", "test", "bench", "docs", "plugin", "verdict")


def write_tsv(rows: list[dict], out) -> None:
    out.write("\t".join(COLUMNS) + "\n")
    for row in rows:
        out.write("\t".join(str(row[c]) for c in COLUMNS) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--environment-allowlist", type=Path,
                        default=Path("tests/scripts/production_environment_keys.txt"))
    parser.add_argument("--resolver", type=Path,
                        default=Path("src/daemon/components/ConfigResolver.cpp"))
    parser.add_argument("--prefix", default="",
                        help="only report keys starting with this prefix")
    parser.add_argument("--verdict", default="",
                        help="only report rows with this verdict")
    args = parser.parse_args()
    root = args.root.resolve()
    env_allowlist = args.environment_allowlist
    if not env_allowlist.is_absolute():
        env_allowlist = root / env_allowlist
    resolver = args.resolver
    if not resolver.is_absolute():
        resolver = root / resolver
    env_keys = read_allowlist(env_allowlist) if env_allowlist.is_file() else []
    toml_keys = resolver_toml_keys(resolver) if resolver.is_file() else []
    rows = build_report(root, env_keys, toml_keys)
    if args.prefix:
        rows = [r for r in rows if r["key"].startswith(args.prefix)]
    if args.verdict:
        rows = [r for r in rows if r["verdict"] == args.verdict]
    write_tsv(rows, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

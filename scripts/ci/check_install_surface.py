#!/usr/bin/env python3
"""Check the staged YAMS install surface against a checked-in policy.

The runtime install is a contract: every installed file, exported symbol and
missing hardening flag is something a package ships. This script stages a
`meson install --destdir` for a build directory (or reads an already staged
tree), then asserts it against scripts/ci/install_surface_policy.json:

  * paths:     every installed path matches an allowed pattern, no forbidden
               pattern (headers, static archives, pkg-config, CMake files) and
               every required path is present
  * sizes:     per-binary size budgets
  * exports:   per-object exported-symbol budgets and forbidden third-party
               exports (sqlite3_, SSL_, curl_, boost::, spdlog::, ...)
  * rpath:     only $ORIGIN / @loader_path / @executable_path relative entries
  * hardening: ELF PIE, BIND_NOW, GNU_RELRO, non-executable stack; Mach-O PIE
  * stripped:  no debug sections or symbol tables in shipped binaries

Usage:
  scripts/ci/check_install_surface.py --build-dir build/release
  scripts/ci/check_install_surface.py --stage /path/to/destdir --prefix /usr

With --build-dir the script stages into a temporary directory, runs the debug
split (scripts/split-debug-symbols.sh) exactly like the release workflow and
then checks. Pass --no-split to check the raw `meson install` output.

Only the Python standard library and binutils/cctools are required: readelf
and nm on Linux (llvm-readelf/llvm-nm are accepted), otool and nm on macOS.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = REPO_ROOT / "scripts" / "ci" / "install_surface_policy.json"
SPLIT_SCRIPT = REPO_ROOT / "scripts" / "split-debug-symbols.sh"

ELF_MAGIC = b"\x7fELF"
MACHO_MAGICS = {
    b"\xfe\xed\xfa\xce",
    b"\xce\xfa\xed\xfe",
    b"\xfe\xed\xfa\xcf",
    b"\xcf\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",  # fat/universal
}


# --------------------------------------------------------------------------- helpers


def glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a path glob with `**` support into an anchored regex."""
    out = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
            continue
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
            continue
        if c == "*":
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        else:
            out.append(re.escape(c))
        i += 1
    return re.compile("^" + "".join(out) + "$")


class PatternSet:
    def __init__(self, patterns: list[str], subst: dict[str, str]):
        self.raw = [expand(p, subst) for p in patterns]
        self.compiled = [(p, glob_to_regex(p)) for p in self.raw]

    def match(self, rel: str) -> str | None:
        for raw, rx in self.compiled:
            if rx.match(rel):
                return raw
        return None


def expand(pattern: str, subst: dict[str, str]) -> str:
    for key, value in subst.items():
        pattern = pattern.replace("{" + key + "}", value)
    return pattern


def run(cmd: list[str], check: bool = True) -> str:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")
    return proc.stdout


def which_any(*names: str) -> str | None:
    for name in names:
        found = shutil.which(name)
        if found:
            return found
    return None


def human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{n} B"


def binary_kind(path: Path) -> str | None:
    try:
        with path.open("rb") as fh:
            magic = fh.read(4)
    except OSError:
        return None
    if magic == ELF_MAGIC:
        return "elf"
    if magic in MACHO_MAGICS:
        return "macho"
    return None


# --------------------------------------------------------------------------- inspection


@dataclass
class BinaryInfo:
    rel: str
    kind: str  # elf | macho
    size: int
    role: str = "unknown"  # executable | shared
    machine: str = ""
    exports: list[str] = field(default_factory=list)
    pie: bool | None = None
    bind_now: bool | None = None
    relro: bool | None = None
    nx: bool | None = None
    cet: bool | None = None
    fortify_imports: int = 0
    rpaths: list[str] = field(default_factory=list)
    debug_sections: list[str] = field(default_factory=list)
    has_symtab: bool = False


class ElfInspector:
    def __init__(self) -> None:
        self.readelf = which_any("readelf", "llvm-readelf", "eu-readelf")
        self.nm = which_any("nm", "llvm-nm")
        if not self.readelf or not self.nm:
            raise SystemExit("readelf and nm are required to inspect ELF files")

    def inspect(self, path: Path, rel: str) -> BinaryInfo:
        info = BinaryInfo(rel=rel, kind="elf", size=path.stat().st_size)
        header = run([self.readelf, "-W", "-h", str(path)])
        etype = re.search(r"^\s*Type:\s*(\S+)", header, re.M)
        machine = re.search(r"^\s*Machine:\s*(.+)$", header, re.M)
        info.machine = machine.group(1).strip() if machine else ""

        phdrs = run([self.readelf, "-W", "-l", str(path)], check=False)
        has_interp = re.search(r"^\s*INTERP\s", phdrs, re.M) is not None
        info.relro = re.search(r"^\s*GNU_RELRO\s", phdrs, re.M) is not None
        stack = re.search(r"^\s*GNU_STACK\s.*\s(R?W?E?)\s+0x[0-9a-f]+\s*$", phdrs, re.M)
        info.nx = bool(stack) and "E" not in stack.group(1)

        dyn = run([self.readelf, "-W", "-d", str(path)], check=False)
        flags_line = " ".join(
            line for line in dyn.splitlines() if "(FLAGS" in line or "(BIND_NOW)" in line
        )
        info.bind_now = bool(re.search(r"\bBIND_NOW\b|\bNOW\b", flags_line))
        is_pie_flag = "PIE" in flags_line
        for match in re.finditer(r"\((?:RUNPATH|RPATH)\).*\[(.*)\]", dyn):
            info.rpaths.extend(p for p in match.group(1).split(":") if p)

        etype_value = etype.group(1) if etype else ""
        if has_interp or etype_value == "EXEC":
            info.role = "executable"
            info.pie = etype_value == "DYN" or is_pie_flag
        else:
            info.role = "shared"
            info.pie = None

        sections = run([self.readelf, "-W", "-S", str(path)], check=False)
        names = re.findall(r"^\s*\[\s*\d+\]\s+(\S+)", sections, re.M)
        info.debug_sections = sorted(
            {n for n in names if n.startswith(".debug_") or n.startswith(".zdebug_")}
        )
        info.has_symtab = ".symtab" in names

        notes = run([self.readelf, "-W", "-n", str(path)], check=False)
        if "X86-64" in info.machine or "x86-64" in info.machine.lower():
            feature = re.search(r"x86 feature:\s*(.*)$", notes, re.M)
            info.cet = bool(feature) and "IBT" in feature.group(1) and "SHSTK" in feature.group(1)

        exported = run([self.nm, "-D", "--defined-only", "-C", str(path)], check=False)
        info.exports = parse_nm(exported)
        undefined = run([self.nm, "-D", "--undefined-only", str(path)], check=False)
        info.fortify_imports = len(re.findall(r"__\w+_chk\b", undefined))
        return info


class MachOInspector:
    def __init__(self) -> None:
        self.otool = which_any("otool", "llvm-otool")
        self.nm = which_any("nm", "llvm-nm")
        if not self.otool or not self.nm:
            raise SystemExit("otool and nm are required to inspect Mach-O files")

    def inspect(self, path: Path, rel: str) -> BinaryInfo:
        info = BinaryInfo(rel=rel, kind="macho", size=path.stat().st_size)
        header = run([self.otool, "-hv", str(path)], check=False)
        if " EXECUTE " in header:
            info.role = "executable"
            info.pie = " PIE" in header
        else:
            info.role = "shared"
        load = run([self.otool, "-l", str(path)], check=False)
        for match in re.finditer(r"cmd LC_RPATH\n\s*cmdsize \d+\n\s*path (\S+)", load):
            info.rpaths.append(match.group(1))
        info.debug_sections = sorted(set(re.findall(r"sectname (__debug_\w+)", load)))
        stabs = run([self.nm, "-ap", str(path)], check=False)
        info.has_symtab = any(" - " in line for line in stabs.splitlines())
        exported = run([self.nm, "-gUjC", str(path)], check=False)
        info.exports = [line.strip() for line in exported.splitlines() if line.strip()]
        undefined = run([self.nm, "-guj", str(path)], check=False)
        info.fortify_imports = len(re.findall(r"___\w+_chk\b", undefined))
        return info


def parse_nm(text: str) -> list[str]:
    names = []
    for line in text.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3:
            _, typ, name = parts
        elif len(parts) == 2:
            typ, name = parts
        else:
            continue
        if typ in ("U", "w", "v"):
            continue
        names.append(name)
    return names


# --------------------------------------------------------------------------- staging


def meson_buildoptions(build_dir: Path) -> dict[str, object]:
    raw = run(["meson", "introspect", "--buildoptions", str(build_dir)])
    return {opt["name"]: opt["value"] for opt in json.loads(raw)}


def stage_build(build_dir: Path, destdir: Path, tags: str | None) -> None:
    cmd = ["meson", "install", "-C", str(build_dir), "--destdir", str(destdir), "--no-rebuild"]
    if tags:
        cmd += ["--tags", tags]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        raise SystemExit(f"meson install failed for {build_dir}")


def split_debug(install_root: Path, debug_dir: Path) -> None:
    if not SPLIT_SCRIPT.exists():
        print(f"note: {SPLIT_SCRIPT.relative_to(REPO_ROOT)} not found; skipping debug split")
        return
    proc = subprocess.run(
        ["bash", str(SPLIT_SCRIPT), str(install_root), str(debug_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        raise SystemExit("debug split failed")


# --------------------------------------------------------------------------- checks


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def first_budget(rules: list[dict], rel: str, subst: dict[str, str], key: str, kind: str = ""):
    for rule in rules:
        if kind and kind not in rule.get("kinds", [kind]):
            continue
        if glob_to_regex(expand(rule["pattern"], subst)).match(rel):
            return rule[key], rule["pattern"]
    return None, None


def collect_tree(install_root: Path) -> tuple[list[str], dict[str, Path]]:
    rels: list[str] = []
    paths: dict[str, Path] = {}
    for dirpath, dirnames, filenames in os.walk(install_root):
        dirnames.sort()
        base = Path(dirpath)
        for name in sorted(filenames) + sorted(d for d in dirnames if (base / d).is_symlink()):
            p = base / name
            rel = p.relative_to(install_root).as_posix()
            rels.append(rel)
            paths[rel] = p
    return sorted(set(rels)), paths


def check(
    destdir: Path,
    prefix: str,
    policy: dict,
    subst: dict[str, str],
    report: Report,
) -> list[BinaryInfo]:
    install_root = destdir / prefix.lstrip("/")
    if not install_root.is_dir():
        report.error(f"install root missing: {install_root}")
        return []

    # Anything staged outside the prefix (e.g. /etc) must be explicitly allowed.
    outside_allowed = PatternSet(policy.get("allowed_outside_prefix", []), subst)
    for dirpath, _, filenames in os.walk(destdir):
        for name in filenames:
            p = Path(dirpath) / name
            try:
                p.relative_to(install_root)
            except ValueError:
                rel_abs = "/" + p.relative_to(destdir).as_posix()
                if not outside_allowed.match(rel_abs):
                    report.error(f"path: installed outside prefix: {rel_abs}")

    rels, paths = collect_tree(install_root)
    allowed = PatternSet(policy["paths"]["allowed"], subst)
    forbidden = PatternSet(policy["paths"].get("forbidden", []), subst)
    forbidden_hits: dict[str, list[str]] = {}
    for rel in rels:
        hit = forbidden.match(rel)
        if hit:
            forbidden_hits.setdefault(hit, []).append(rel)
        elif not allowed.match(rel):
            report.error(f"path: not in the allowed runtime set: {rel}")
        p = paths[rel]
        if p.is_symlink():
            target = (p.parent / os.readlink(p)).resolve()
            try:
                target.relative_to(install_root.resolve())
            except ValueError:
                report.error(f"path: symlink escapes the install prefix: {rel} -> {os.readlink(p)}")
            if not target.exists():
                report.error(f"path: dangling symlink: {rel} -> {os.readlink(p)}")

    for pattern, hits in sorted(forbidden_hits.items()):
        size = sum(paths[h].lstat().st_size for h in hits)
        sample = ", ".join(hits[:3])
        report.error(
            f"path: {len(hits)} forbidden path(s) ({human(size)}) match {pattern}: {sample}"
            f"{', ...' if len(hits) > 3 else ''}"
        )

    for req in policy["paths"].get("required", []):
        rx = glob_to_regex(expand(req, subst))
        if not any(rx.match(r) for r in rels):
            report.error(f"path: required runtime path missing: {expand(req, subst)}")

    # Binary inspection
    inspectors: dict[str, object] = {}
    infos: list[BinaryInfo] = []
    for rel in rels:
        p = paths[rel]
        if p.is_symlink() or not p.is_file():
            continue
        kind = binary_kind(p)
        if kind is None:
            continue
        if kind not in inspectors:
            inspectors[kind] = ElfInspector() if kind == "elf" else MachOInspector()
        infos.append(inspectors[kind].inspect(p, rel))  # type: ignore[attr-defined]

    hard = policy.get("hardening", {})
    strip_policy = policy.get("stripped", {})
    forbidden_exports = [re.compile(x) for x in policy.get("forbidden_exports", [])]
    export_rules = policy.get("export_budgets", [])
    size_rules = policy.get("size_budgets", [])
    exempt = PatternSet(policy.get("third_party_exempt", []), subst)

    for info in infos:
        is_third_party = exempt.match(info.rel) is not None

        budget, pat = first_budget(size_rules, info.rel, subst, "max_bytes", info.kind)
        if budget is not None and info.size > budget:
            report.error(
                f"size: {info.rel} is {human(info.size)} > budget {human(budget)} ({pat})"
            )

        # Mach-O executables keep every non-hidden global in their export trie
        # whether or not anything can bind to it; the dynamic surface that
        # matters on macOS is the dylib/bundle export list.
        check_exports = not (info.kind == "macho" and info.role == "executable")
        budget, pat = first_budget(export_rules, info.rel, subst, "max")
        if check_exports and budget is not None and len(info.exports) > budget:
            report.error(
                f"exports: {info.rel} exports {len(info.exports)} symbols > budget {budget} ({pat})"
            )
        if check_exports and not is_third_party:
            leaked: dict[str, list[str]] = {}
            for name in info.exports:
                for rx in forbidden_exports:
                    if rx.search(name):
                        leaked.setdefault(rx.pattern, []).append(name)
                        break
            for pattern, names in sorted(leaked.items()):
                sample = ", ".join(n if len(n) <= 60 else n[:57] + "..." for n in names[:3])
                report.error(
                    f"exports: {info.rel} leaks {len(names)} symbol(s) matching /{pattern}/ "
                    f"(e.g. {sample})"
                )

        for rp in info.rpaths:
            if not (rp.startswith("$ORIGIN") or rp.startswith("${ORIGIN}") or rp.startswith("@")):
                report.error(f"rpath: {info.rel} has non-relative RUNPATH/RPATH entry {rp!r}")

        if strip_policy.get("no_debug_sections") and info.debug_sections:
            report.error(
                f"stripped: {info.rel} ships debug sections ({', '.join(info.debug_sections[:4])}"
                f"{'...' if len(info.debug_sections) > 4 else ''})"
            )
        if strip_policy.get("no_symtab") and info.has_symtab and not is_third_party:
            report.error(f"stripped: {info.rel} ships a full symbol table (.symtab / stabs)")

        if info.kind == "elf" and not is_third_party:
            if hard.get("pie") and info.role == "executable" and not info.pie:
                report.error(f"hardening: {info.rel} is not PIE")
            if hard.get("bind_now") and not info.bind_now:
                report.error(f"hardening: {info.rel} lacks BIND_NOW (full RELRO)")
            if hard.get("relro") and not info.relro:
                report.error(f"hardening: {info.rel} lacks a GNU_RELRO segment")
            if hard.get("nx") and info.nx is False:
                report.error(f"hardening: {info.rel} has an executable or missing GNU_STACK")
            cet_mode = hard.get("cet_x86_64", "off")
            if info.cet is False and cet_mode != "off":
                msg = f"hardening: {info.rel} lacks x86 IBT/SHSTK notes (-fcf-protection)"
                (report.error if cet_mode == "require" else report.warn)(msg)
            fortify_mode = hard.get("fortify_imports", "off")
            if info.role == "executable" and info.fortify_imports == 0 and fortify_mode != "off":
                msg = f"hardening: {info.rel} imports no __*_chk functions (_FORTIFY_SOURCE inert?)"
                (report.error if fortify_mode == "require" else report.warn)(msg)
        if info.kind == "macho" and hard.get("pie") and info.role == "executable" and not info.pie:
            report.error(f"hardening: {info.rel} is not PIE (MH_PIE)")

    return infos


def print_table(infos: list[BinaryInfo], rels_total: int, total_bytes: int) -> None:
    print(f"\nStaged runtime: {rels_total} paths, {human(total_bytes)} total")
    if not infos:
        return
    width = max(len(i.rel) for i in infos)
    print(
        f"{'binary'.ljust(width)}  {'bytes':>11}  {'exports':>7}  pie  now  relro  nx  cet  "
        f"chk  debug  symtab  rpath"
    )

    def yn(v):
        return "-" if v is None else ("y" if v else "n")

    for i in sorted(infos, key=lambda x: x.rel):
        print(
            f"{i.rel.ljust(width)}  {i.size:>11}  {len(i.exports):>7}  {yn(i.pie):>3}  "
            f"{yn(i.bind_now):>3}  {yn(i.relro):>5}  {yn(i.nx):>2}  {yn(i.cet):>3}  "
            f"{i.fortify_imports:>3}  {('y' if i.debug_sections else 'n'):>5}  "
            f"{('y' if i.has_symtab else 'n'):>6}  {':'.join(i.rpaths) or '-'}"
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--build-dir", type=Path, help="Meson build dir to stage with meson install")
    src.add_argument("--stage", type=Path, help="already staged DESTDIR root to check")
    ap.add_argument("--prefix", help="install prefix inside the stage (default: from meson)")
    ap.add_argument("--libdir", help="libdir relative to prefix (default: from meson, else lib)")
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--tags", help="meson install --tags subset to stage (default: everything)")
    ap.add_argument("--no-split", action="store_true", help="skip the debug split before checking")
    ap.add_argument("--keep-stage", type=Path, help="stage into this directory and keep it")
    ap.add_argument("--json", type=Path, help="write a machine-readable report here")
    args = ap.parse_args(argv)

    policy = json.loads(args.policy.read_text())
    prefix = args.prefix
    libdir = args.libdir
    tmp: tempfile.TemporaryDirectory[str] | None = None

    if args.build_dir:
        opts = meson_buildoptions(args.build_dir)
        prefix = prefix or str(opts["prefix"])
        libdir = libdir or str(opts["libdir"])
        if args.keep_stage:
            if args.keep_stage.exists():
                shutil.rmtree(args.keep_stage)
            destdir = args.keep_stage.resolve()
        else:
            tmp = tempfile.TemporaryDirectory(prefix="yams-install-surface-")
            destdir = Path(tmp.name) / "stage"
        stage_build(args.build_dir, destdir, args.tags)
        if not args.no_split:
            split_debug(destdir / prefix.lstrip("/"), destdir.parent / "debug-symbols")
    else:
        destdir = args.stage.resolve()
        if not prefix:
            ap.error("--prefix is required with --stage")
        libdir = libdir or "lib"

    if os.path.isabs(libdir):
        libdir = os.path.relpath(libdir, prefix)
    subst = {"libdir": libdir.strip("/")}
    report = Report()
    try:
        infos = check(destdir, prefix, policy, subst, report)
        install_root = destdir / prefix.lstrip("/")
        rels, paths = collect_tree(install_root) if install_root.is_dir() else ([], {})
        total = sum(paths[r].lstat().st_size for r in rels)
        print(f"Install surface check: {install_root}  (policy {args.policy.name}, "
              f"libdir={subst['libdir']}, host={platform.system()}/{platform.machine()})")
        print_table(infos, len(rels), total)
        if args.json:
            args.json.write_text(
                json.dumps(
                    {
                        "errors": report.errors,
                        "warnings": report.warnings,
                        "files": rels,
                        "total_bytes": total,
                        "binaries": [
                            {
                                "path": i.rel,
                                "bytes": i.size,
                                "exports": len(i.exports),
                                "pie": i.pie,
                                "bind_now": i.bind_now,
                                "relro": i.relro,
                                "nx": i.nx,
                                "cet": i.cet,
                                "fortify_imports": i.fortify_imports,
                                "debug_sections": bool(i.debug_sections),
                                "symtab": i.has_symtab,
                                "rpaths": i.rpaths,
                            }
                            for i in infos
                        ],
                    },
                    indent=2,
                )
            )
    finally:
        if tmp is not None:
            tmp.cleanup()

    for w in report.warnings:
        print(f"WARN  {w}")
    for e in report.errors:
        print(f"FAIL  {e}")
    if report.errors:
        print(f"\ninstall surface: RED ({len(report.errors)} violation(s), {len(report.warnings)} warning(s))")
        return 1
    print(f"\ninstall surface: GREEN ({len(report.warnings)} warning(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())

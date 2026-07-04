#!/usr/bin/env python3
import datetime as _dt
import os
import pathlib
import re
import subprocess
import sys


def _run_git(repo_root: str, git_exe: str, args: list[str]) -> str:
    try:
        proc = subprocess.run(
            [git_exe, "-c", f"safe.directory={repo_root}", *args],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
    except OSError:
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _compute_effective_version(
    repo_root: str, git_exe: str, project_version: str, override_version: str
) -> str:
    if override_version:
        return override_version
    tag = _run_git(
        repo_root, git_exe, ["describe", "--tags", "--match", "v*", "--abbrev=0"]
    )
    if tag.startswith("v"):
        return tag[1:]
    if tag:
        return tag
    return project_version


def _now_timestamp() -> str:
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    if epoch:
        try:
            dt = _dt.datetime.fromtimestamp(int(epoch), _dt.timezone.utc)
        except (OSError, OverflowError, ValueError):
            dt = _dt.datetime.now(_dt.timezone.utc)
    else:
        dt = _dt.datetime.now(_dt.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_define(text: str, name: str) -> str:
    m = re.search(rf'#define\s+{re.escape(name)}\s+"([^"]*)"', text)
    return m.group(1) if m else ""


def main() -> int:
    if len(sys.argv) != 7:
        print(
            "usage: generate_version_header.py <template> <output> <repo_root> "
            "<project_version> <override_version> <git_exe>",
            file=sys.stderr,
        )
        return 2

    template_path = pathlib.Path(sys.argv[1])
    output_path = pathlib.Path(sys.argv[2])
    repo_root = sys.argv[3]
    project_version = sys.argv[4]
    override_version = sys.argv[5]
    git_exe = sys.argv[6]

    template = template_path.read_text()
    effective_version = _compute_effective_version(
        repo_root, git_exe, project_version, override_version
    )
    git_describe = _run_git(
        repo_root, git_exe, ["describe", "--tags", "--always", "--dirty"]
    )
    git_commit = _run_git(repo_root, git_exe, ["rev-parse", "--short=8", "HEAD"])
    build_timestamp = _now_timestamp()

    if output_path.exists():
        existing = output_path.read_text()
        if (
            _extract_define(existing, "YAMS_EFFECTIVE_VERSION") == effective_version
            and _extract_define(existing, "YAMS_PROJECT_VERSION") == project_version
            and _extract_define(existing, "YAMS_OVERRIDE_VERSION") == override_version
            and _extract_define(existing, "YAMS_GIT_DESCRIBE") == git_describe
            and _extract_define(existing, "YAMS_GIT_COMMIT") == git_commit
        ):
            preserved = _extract_define(existing, "YAMS_BUILD_TIMESTAMP")
            if preserved:
                build_timestamp = preserved

    rendered = (
        template.replace("@YAMS_EFFECTIVE_VERSION@", effective_version)
        .replace("@YAMS_PROJECT_VERSION@", project_version)
        .replace("@YAMS_OVERRIDE_VERSION@", override_version)
        .replace("@YAMS_BUILD_TIMESTAMP@", build_timestamp)
        .replace("@YAMS_GIT_DESCRIBE@", git_describe)
        .replace("@YAMS_GIT_COMMIT@", git_commit)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.read_text() == rendered:
        return 0
    output_path.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

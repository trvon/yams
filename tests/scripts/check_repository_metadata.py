#!/usr/bin/env python3
"""Validate canonical and mirrored repository metadata."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

FORGEJO_URL = "https://git.trevon.dev/trevon/yams"
GITHUB_URL = "https://github.com/trvon/yams"
SOURCEHUT_MARKERS = ("https://sr.ht/~trvon/yams", "https://git.sr.ht/~trevon/yams")

EXPECTED_TEXT = {
    "README.md": (GITHUB_URL, FORGEJO_URL),
    "docs/index.md": (GITHUB_URL, FORGEJO_URL),
    "CITATION.cff": (
        f'repository-code: "{GITHUB_URL}"',
        f'repository-artifact: "{FORGEJO_URL}"',
    ),
    "mkdocs.yml": (
        f"repo_url: {FORGEJO_URL}",
        f"link: {GITHUB_URL}",
        f"link: {FORGEJO_URL}",
    ),
    "scripts/build-deb.sh": (
        f"Homepage: {FORGEJO_URL}",
        f"URL: {FORGEJO_URL}",
    ),
}

# These references describe historical releases or the still-active newsletter list.
SOURCEHUT_ALLOWLIST = {
    "CHANGELOG.md",
    "docs/newsletter.md",
    "tests/scripts/check_repository_metadata.py",
}
SKIP_PARTS = {".git", "build", "builddir", "node_modules", "site", "subprojects", "third_party"}
RETIRED_PATHS = (".build.yml", "scripts/srht-collect-artifacts.sh")


def tracked_files(root: Path) -> list[str] | None:
    """Paths git tracks under root (relative, POSIX), or None when root is not a checkout root.

    The policy is about what the repository publishes. Walking the filesystem also read
    untracked, gitignored copies of the tree (agent worktrees under .claude/worktrees/, local
    caches), so an old copy of a file failed the check for a clean checkout.
    """
    # A hook exports GIT_DIR/GIT_INDEX_FILE for the repository it runs in; --root must win.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}

    def git(*args: str) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "-C", str(root), *args],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
            )
        except OSError:
            return None
        if completed.returncode != 0:
            return None
        return completed.stdout.decode("utf-8", errors="surrogateescape")

    toplevel = git("rev-parse", "--show-toplevel")
    # Only when root is the checkout itself: a plain directory nested inside some other
    # repository (a temporary tree, an exported tarball) is walked instead.
    if toplevel is None or Path(toplevel.strip()).resolve() != root:
        return None
    listing = git("ls-files", "-z")
    if listing is None:
        return None
    return sorted(name for name in listing.split("\0") if name)


def ignored_dot_directories(root: Path) -> tuple[set[str], set[str]]:
    """Dot-directories the root .gitignore names literally: (any depth, root only).

    Used only without git. Glob patterns and negations are not interpreted; a plain entry
    such as `.claude/` or `/.cache` is enough to keep local tool state out of the walk.
    """
    anywhere: set[str] = set()
    root_only: set[str] = set()
    try:
        lines = (root / ".gitignore").read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return anywhere, root_only
    for line in lines:
        pattern = line.strip()
        if not pattern or pattern.startswith(("#", "!")):
            continue
        anchored = pattern.startswith("/")
        name = pattern.strip("/")
        if not name.startswith(".") or "/" in name or any(c in name for c in "*?[\\"):
            continue
        (root_only if anchored else anywhere).add(name)
    return anywhere, root_only


def walked_files(root: Path):
    anywhere, root_only = ignored_dot_directories(root)
    # Prune before descent: filtering rglob results still traverses build/cache trees.
    for directory, directories, filenames in os.walk(root, followlinks=False):
        skipped = SKIP_PARTS | anywhere | (root_only if Path(directory) == root else set())
        directories[:] = sorted(name for name in directories if name not in skipped)
        for name in sorted(filenames):
            path = Path(directory) / name
            if name not in SKIP_PARTS and path.is_file():
                yield path


def repository_files(root: Path):
    """Files the SourceHut scan reads: tracked files in a checkout, else a pruned walk."""
    tracked = tracked_files(root)
    if tracked is None:
        yield from walked_files(root)
        return
    for relative in tracked:
        if SKIP_PARTS.intersection(relative.split("/")):
            continue
        path = root / relative
        # Submodule gitlinks are directories; a file deleted in the working tree is absent.
        if path.is_file():
            yield path


def find_text_markers(path: Path, markers: tuple[str, ...]) -> set[str]:
    """Scan strict UTF-8 with bounded memory, including cross-chunk matches.

    Consume the whole file even after finding every marker: invalid UTF-8 near
    EOF must still reject the file exactly as the former read_text did.
    """
    found: set[str] = set()
    overlap = max((len(marker) for marker in markers), default=1) - 1
    tail = ""
    with path.open("r", encoding="utf-8") as stream:
        while chunk := stream.read(65536):
            text = tail + chunk
            found.update(marker for marker in markers if marker in text)
            tail = text[-overlap:] if overlap else ""
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    failures: list[str] = []

    for relative, required in EXPECTED_TEXT.items():
        path = root / relative
        if not path.is_file():
            failures.append(f"missing metadata file: {relative}")
            continue
        found = find_text_markers(path, required)
        for value in required:
            if value not in found:
                failures.append(f"{relative}: missing {value!r}")

    for relative in RETIRED_PATHS:
        if (root / relative).exists():
            failures.append(f"retired SourceHut path remains: {relative}")

    for path in repository_files(root):
        relative = path.relative_to(root).as_posix()
        if relative in SOURCEHUT_ALLOWLIST:
            continue
        try:
            found = find_text_markers(path, SOURCEHUT_MARKERS)
        except UnicodeDecodeError:
            continue
        for marker in SOURCEHUT_MARKERS:
            if marker in found:
                failures.append(f"{relative}: retired repository URL remains: {marker}")

    if failures:
        for failure in failures:
            print(failure)
        return 1

    print("repository metadata policy: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

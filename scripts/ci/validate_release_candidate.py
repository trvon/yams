#!/usr/bin/env python3
"""Validate an immutable experimental-to-main stable-release candidate."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


class CandidateError(RuntimeError):
    """The selected commits cannot form a stable release candidate."""


@dataclass(frozen=True)
class CandidateReport:
    base_sha: str
    candidate_sha: str
    commit_count: int
    current_version: str
    latest_stable_tag: str

    def markdown(self) -> str:
        return "\n".join(
            (
                "## Release candidate preflight",
                "",
                "| Check | Value |",
                "|---|---|",
                f"| Base | `{self.base_sha}` |",
                f"| Candidate | `{self.candidate_sha}` |",
                f"| Candidate commits | {self.commit_count} |",
                f"| Current stable version | `{self.current_version}` |",
                f"| Latest reachable stable tag | `{self.latest_stable_tag}` |",
                "",
                (
                    "The candidate contains the complete base history, all version surfaces "
                    "agree, and no release version has been pre-bumped."
                ),
                "",
            )
        )


def run_git(
    root: Path, *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise CandidateError(f"git {' '.join(arguments)} failed: {detail}")
    return result


def resolve_commit(root: Path, revision: str) -> str:
    result = run_git(root, "rev-parse", "--verify", f"{revision}^{{commit}}")
    sha = result.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        raise CandidateError(
            f"revision did not resolve to a full commit SHA: {revision}"
        )
    return sha


def read_blob(root: Path, revision: str, path: str) -> str:
    return run_git(root, "show", f"{revision}:{path}").stdout


def require_version(match: re.Match[str] | None, surface: str) -> str:
    if match is None or SEMVER.fullmatch(match.group(1)) is None:
        raise CandidateError(f"could not read a stable semantic version from {surface}")
    return match.group(1)


def versions_at(root: Path, revision: str) -> dict[str, str]:
    try:
        manifest = json.loads(
            read_blob(root, revision, ".release-please-manifest.json")
        )
        manifest_version = manifest["."]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise CandidateError("invalid root Release Please manifest") from error
    if (
        not isinstance(manifest_version, str)
        or SEMVER.fullmatch(manifest_version) is None
    ):
        raise CandidateError("invalid root version in Release Please manifest")

    meson = require_version(
        re.search(
            r"project\s*\(.*?version\s*:\s*['\"]([0-9]+\.[0-9]+\.[0-9]+)['\"]",
            read_blob(root, revision, "meson.build"),
            re.DOTALL,
        ),
        "meson.build",
    )
    conan = require_version(
        re.search(
            r"^\s*version\s*=\s*['\"]([0-9]+\.[0-9]+\.[0-9]+)['\"]",
            read_blob(root, revision, "conanfile.py"),
            re.MULTILINE,
        ),
        "conanfile.py",
    )
    citation = require_version(
        re.search(
            r"^version\s*:\s*['\"]?([0-9]+\.[0-9]+\.[0-9]+)['\"]?",
            read_blob(root, revision, "CITATION.cff"),
            re.MULTILINE,
        ),
        "CITATION.cff",
    )
    return {
        "manifest": manifest_version,
        "meson": meson,
        "conan": conan,
        "citation": citation,
    }


def require_aligned_versions(versions: dict[str, str], revision_name: str) -> str:
    unique = set(versions.values())
    if len(unique) != 1:
        detail = ", ".join(f"{name}={value}" for name, value in versions.items())
        raise CandidateError(f"{revision_name} version surfaces disagree: {detail}")
    return next(iter(unique))


def semver_key(version: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in version.split("."))  # type: ignore[return-value]


def latest_stable_tag(root: Path, revision: str) -> str:
    result = run_git(root, "tag", "--merged", revision, "--list", "v[0-9]*")
    candidates: list[tuple[tuple[int, int, int], str]] = []
    for tag in result.stdout.splitlines():
        version = tag.removeprefix("v")
        if SEMVER.fullmatch(version):
            candidates.append((semver_key(version), tag))
    if not candidates:
        raise CandidateError("base has no reachable stable vMAJOR.MINOR.PATCH tag")
    return max(candidates)[1]


def validate_candidate(root: Path, base: str, candidate: str) -> CandidateReport:
    root = root.resolve()
    base_sha = resolve_commit(root, base)
    candidate_sha = resolve_commit(root, candidate)
    if base_sha == candidate_sha:
        raise CandidateError("candidate contains no commits beyond the base")
    ancestry = run_git(
        root, "merge-base", "--is-ancestor", base_sha, candidate_sha, check=False
    )
    if ancestry.returncode != 0:
        raise CandidateError("candidate must contain the complete base history")

    commit_count = int(
        run_git(
            root, "rev-list", "--count", f"{base_sha}..{candidate_sha}"
        ).stdout.strip()
    )
    if commit_count <= 0:
        raise CandidateError("candidate contains no releasable commit range")

    base_version = require_aligned_versions(versions_at(root, base_sha), "base")
    candidate_version = require_aligned_versions(
        versions_at(root, candidate_sha), "candidate"
    )
    if candidate_version != base_version:
        raise CandidateError(
            f"candidate version must remain at {base_version} until Release Please prepares it; "
            f"found {candidate_version}"
        )

    stable_tag = latest_stable_tag(root, base_sha)
    if stable_tag.removeprefix("v") != base_version:
        raise CandidateError(
            f"base manifest {base_version} does not match latest stable tag {stable_tag}"
        )

    return CandidateReport(
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        commit_count=commit_count,
        current_version=base_version,
        latest_stable_tag=stable_tag,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args()

    try:
        report = validate_candidate(args.root, args.base, args.candidate)
    except CandidateError as error:
        parser.exit(1, f"release candidate rejected: {error}\n")

    json_text = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    markdown = report.markdown()
    if args.json_output:
        args.json_output.write_text(json_text, encoding="utf-8")
    else:
        print(json_text, end="")
    if args.markdown_output:
        args.markdown_output.write_text(markdown, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

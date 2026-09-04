#!/usr/bin/env python3
"""Generate and validate the immutable GitHub release manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

SCHEMA_VERSION = 1
FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
NIGHTLY_TAG_RE = re.compile(r"^experimental-nightly-[0-9]{8}-[0-9a-f]{40}$")
CHECKSUM_RE = re.compile(r"^([0-9a-fA-F]{64})[ \t]+[*]?([^\r\n]+)$")
DISTRIBUTABLE_SUFFIXES = (
    ".pkg.tar.zst",
    ".tar.gz",
    ".AppImage",
    ".zip",
    ".deb",
    ".rpm",
    ".pkg",
    ".msi",
)
ARCHITECTURE_PATTERNS = {
    "x86_64": (
        re.compile(r"(?<![A-Za-z0-9])x86_64(?![A-Za-z0-9])", re.IGNORECASE),
        re.compile(r"(?<![A-Za-z0-9])amd64(?![A-Za-z0-9])", re.IGNORECASE),
        re.compile(r"(?<![A-Za-z0-9])x64(?![A-Za-z0-9])", re.IGNORECASE),
    ),
    "aarch64": (
        re.compile(r"(?<![A-Za-z0-9])aarch64(?![A-Za-z0-9])", re.IGNORECASE),
        re.compile(r"(?<![A-Za-z0-9])arm64(?![A-Za-z0-9])", re.IGNORECASE),
        re.compile(r"(?<![A-Za-z0-9])armv8(?![A-Za-z0-9])", re.IGNORECASE),
    ),
}


def positive_int(value: str | int, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be a positive integer") from error
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def normalize_architecture(filename: str) -> str:
    matches = {
        architecture
        for architecture, patterns in ARCHITECTURE_PATTERNS.items()
        if any(pattern.search(filename) for pattern in patterns)
    }
    if not matches:
        raise ValueError(f"unknown architecture in asset filename: {filename}")
    if len(matches) != 1:
        raise ValueError(f"ambiguous architecture in asset filename: {filename}")
    return matches.pop()


def read_checksums(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"missing checksum manifest: {path}")

    checksums: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not raw_line.strip():
            continue
        match = CHECKSUM_RE.fullmatch(raw_line)
        if match is None:
            raise ValueError(f"invalid SHA256SUMS line {line_number}")
        digest, filename = match.groups()
        if Path(filename).name != filename or filename in {".", ".."}:
            raise ValueError(f"invalid asset name in SHA256SUMS: {filename}")
        if filename in checksums:
            raise ValueError(f"duplicate asset name in SHA256SUMS: {filename}")
        checksums[filename] = digest.lower()

    if not checksums:
        raise ValueError("SHA256SUMS contains no distributable assets")
    return checksums


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_run_url(url: str, repository: str, run_id: int, label: str) -> None:
    expected = f"/repos/{repository}/actions/runs/{run_id}"
    browser_expected = f"/{repository}/actions/runs/{run_id}"
    if expected not in url and browser_expected not in url:
        raise ValueError(f"{label} URL is not tied to exact run ID {run_id}: {url}")
    if "/latest" in url or "/branches/" in url:
        raise ValueError(f"{label} URL is mutable: {url}")


def validate_published_at(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("published_at must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError("published_at must include a timezone")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def release_asset_url(
    github_server_url: str, repository: str, tag: str, filename: str
) -> str:
    if not github_server_url.startswith("https://"):
        raise ValueError("GitHub server URL must use HTTPS")
    if repository.count("/") != 1 or any(part == "" for part in repository.split("/")):
        raise ValueError(f"invalid GitHub repository: {repository}")
    if tag.lower() in {"latest", "nightly", "stable"} or "/" in tag:
        raise ValueError(f"mutable release tag is not allowed: {tag}")
    url = (
        f"{github_server_url.rstrip('/')}/{repository}/releases/download/"
        f"{quote(tag, safe='._-')}/{quote(filename, safe='._-')}"
    )
    if "/releases/latest" in url or "/releases/download/latest/" in url:
        raise ValueError(f"mutable release URL is not allowed: {url}")
    return url


def build_manifest(
    *,
    assets_dir: Path,
    checksums_path: Path,
    channel: str,
    version: str,
    tag: str,
    source_sha: str,
    source_ref: str,
    repository: str,
    github_server_url: str,
    release_run_id: str | int,
    release_run_attempt: str | int,
    release_run_url: str,
    tests_run_id: str | int | None,
    tests_run_attempt: str | int | None,
    tests_run_url: str | None,
    ipc_protocol_version: str | int,
    p2p_protocol_version: str | int,
    published_at: str,
) -> dict[str, object]:
    if channel not in {"stable", "nightly", "weekly"}:
        raise ValueError("channel must be stable, nightly, or weekly")
    if FULL_SHA_RE.fullmatch(source_sha) is None:
        raise ValueError("source_sha must be a full 40-character source SHA")
    if not source_ref.startswith("refs/"):
        raise ValueError("source_ref must be a full refs/... reference")
    if channel == "stable" and source_ref != f"refs/tags/{tag}":
        raise ValueError("stable manifest source_ref must match its immutable tag")
    if channel != "stable" and source_ref != "refs/heads/experimental":
        raise ValueError(
            "non-stable manifest source_ref must be refs/heads/experimental"
        )
    if channel == "nightly" and NIGHTLY_TAG_RE.fullmatch(tag) is None:
        raise ValueError(
            "nightly tag must match experimental-nightly-YYYYMMDD-<full source SHA>"
        )
    if channel == "nightly" and not tag.endswith(source_sha):
        raise ValueError("nightly tag source SHA does not match source_sha")

    run_id = positive_int(release_run_id, "release workflow run ID")
    run_attempt = positive_int(release_run_attempt, "release workflow run attempt")
    validate_run_url(release_run_url, repository, run_id, "release workflow")

    tests_workflow: dict[str, object] | None = None
    if channel != "stable":
        if (
            tests_run_id is None
            or tests_run_id == ""
            or tests_run_attempt is None
            or tests_run_attempt == ""
            or not tests_run_url
        ):
            raise ValueError("non-stable manifest requires an exact Tests workflow run")
        tests_id = positive_int(tests_run_id, "Tests workflow run ID")
        tests_attempt = positive_int(tests_run_attempt, "Tests workflow run attempt")
        validate_run_url(tests_run_url, repository, tests_id, "Tests workflow")
        if not tests_run_url.endswith(f"/attempts/{tests_attempt}"):
            raise ValueError("Tests workflow URL must identify the exact run attempt")
        tests_workflow = {
            "run_id": tests_id,
            "run_attempt": tests_attempt,
            "url": tests_run_url,
        }

    checksums = read_checksums(checksums_path)
    distributables = {
        path.name
        for path in assets_dir.iterdir()
        if path.is_file() and path.name.endswith(DISTRIBUTABLE_SUFFIXES)
    }
    unchecksummed = sorted(distributables - checksums.keys())
    if unchecksummed:
        raise ValueError(
            "distributable assets missing from SHA256SUMS: " + ", ".join(unchecksummed)
        )
    unexpected = sorted(checksums.keys() - distributables)
    if unexpected:
        missing = [name for name in unexpected if not (assets_dir / name).is_file()]
        if missing:
            raise ValueError(
                "missing asset referenced by SHA256SUMS: " + ", ".join(missing)
            )
        raise ValueError(
            "SHA256SUMS includes non-distributable assets: " + ", ".join(unexpected)
        )

    assets: list[dict[str, object]] = []
    for filename, expected_digest in sorted(checksums.items()):
        path = assets_dir / filename
        if not path.is_file():
            raise ValueError(f"missing asset referenced by SHA256SUMS: {filename}")
        actual_digest = sha256_file(path)
        if actual_digest != expected_digest:
            raise ValueError(
                f"checksum mismatch for {filename}: expected {expected_digest}, "
                f"got {actual_digest}"
            )
        assets.append(
            {
                "name": filename,
                "filename": filename,
                "size_bytes": path.stat().st_size,
                "architecture": normalize_architecture(filename),
                "sha256": actual_digest,
                "url": release_asset_url(github_server_url, repository, tag, filename),
            }
        )

    ipc_version = positive_int(ipc_protocol_version, "IPC protocol version")
    p2p_version = positive_int(p2p_protocol_version, "P2P protocol version")
    return {
        "schema_version": SCHEMA_VERSION,
        "channel": channel,
        "version": version,
        "tag": tag,
        "published_at": validate_published_at(published_at),
        "source_sha": source_sha,
        "source_ref": source_ref,
        "release_workflow_run_id": run_id,
        "release_workflow_run_attempt": run_attempt,
        "release_workflow_run_url": release_run_url,
        "tests_workflow_run_id": (
            tests_workflow["run_id"] if tests_workflow is not None else None
        ),
        "tests_workflow_run_attempt": (
            tests_workflow["run_attempt"] if tests_workflow is not None else None
        ),
        "tests_workflow_run_url": (
            tests_workflow["url"] if tests_workflow is not None else None
        ),
        "ipc_protocol_version": ipc_version,
        "p2p_protocol_version": p2p_version,
        "assets": assets,
    }


def read_protocol_version(path: Path, symbol: str) -> int:
    if not path.is_file():
        raise ValueError(f"protocol header does not exist: {path}")
    pattern = re.compile(rf"\b{re.escape(symbol)}\b\s*=\s*([0-9]+)\s*;")
    matches = pattern.findall(path.read_text(encoding="utf-8"))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one {symbol} definition in {path}")
    return positive_int(matches[0], symbol)


def write_manifest(output: Path, manifest: dict[str, object]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets-dir", type=Path, required=True)
    parser.add_argument("--checksums", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--github-server-url", default="https://github.com")
    parser.add_argument("--release-run-id", required=True)
    parser.add_argument("--release-run-attempt", required=True)
    parser.add_argument("--release-run-url", required=True)
    parser.add_argument("--tests-run-id", default="")
    parser.add_argument("--tests-run-attempt", default="")
    parser.add_argument("--tests-run-url", default="")
    parser.add_argument("--ipc-protocol-header", type=Path, required=True)
    parser.add_argument("--p2p-protocol-header", type=Path, required=True)
    parser.add_argument("--published-at", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = build_manifest(
            assets_dir=args.assets_dir,
            checksums_path=args.checksums,
            channel=args.channel,
            version=args.version,
            tag=args.tag,
            source_sha=args.source_sha,
            source_ref=args.source_ref,
            repository=args.repository,
            github_server_url=args.github_server_url,
            release_run_id=args.release_run_id,
            release_run_attempt=args.release_run_attempt,
            release_run_url=args.release_run_url,
            tests_run_id=args.tests_run_id,
            tests_run_attempt=args.tests_run_attempt,
            tests_run_url=args.tests_run_url,
            ipc_protocol_version=read_protocol_version(
                args.ipc_protocol_header, "PROTOCOL_VERSION"
            ),
            p2p_protocol_version=read_protocol_version(
                args.p2p_protocol_header, "kP2pProtocolVersion"
            ),
            published_at=args.published_at,
        )
        write_manifest(args.output, manifest)
    except (OSError, ValueError) as error:
        print(f"release manifest error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

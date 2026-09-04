#!/usr/bin/env python3
"""Publish one validated package-repository channel to R2 in fail-closed order."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

RELEASE_CHANNELS = frozenset({"stable", "nightly", "weekly"})
PACKAGE_SUFFIXES = (".deb", ".rpm", ".pkg.tar.zst")
REPOSITORY_DIRS = ("aptrepo", "yumrepo", "archrepo")
MISSING_OBJECT_RE = re.compile(
    r"(?:404|object[^\n]*not found|key[^\n]*does not exist|specified key does not exist)",
    re.IGNORECASE,
)


class PublicationError(RuntimeError):
    """A publication precondition or storage operation failed."""


@dataclass(frozen=True)
class PublicationObject:
    source: Path
    key: str
    immutable: bool
    phase: str
    content_type: str


@dataclass(frozen=True)
class ValidatedManifest:
    release_channel: str
    publication_channel: str
    release_run_id: int
    release_run_attempt: int


class ObjectStore(Protocol):
    def get(self, key: str) -> bytes | None:
        raise NotImplementedError

    def put(self, key: str, source: Path, content_type: str) -> None:
        raise NotImplementedError


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_type(key: str) -> str:
    suffixes = (
        (".json", "application/json"),
        (".key", "application/pgp-keys"),
        (".txt", "text/plain"),
        (".xml", "application/xml"),
        (".gz", "application/gzip"),
        (".deb", "application/vnd.debian.binary-package"),
        (".rpm", "application/x-rpm"),
        (".pkg.tar.zst", "application/zstd"),
        (".db", "application/gzip"),
        (".files", "application/gzip"),
        (".asc", "application/pgp-signature"),
    )
    for suffix, value in suffixes:
        if key.endswith(suffix):
            return value
    return "application/octet-stream"


def validate_manifest(
    manifest_path: Path,
    expected_channel: str,
    selected_run_id: int,
    selected_run_attempt: int,
) -> ValidatedManifest:
    if expected_channel not in {"stable", "experimental"}:
        raise PublicationError("expected channel must be stable or experimental")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise PublicationError(f"missing release manifest: {manifest_path}") from error
    except json.JSONDecodeError as error:
        raise PublicationError(f"invalid release manifest JSON: {error}") from error
    if not isinstance(manifest, dict):
        raise PublicationError("release manifest must be a JSON object")

    release_channel = manifest.get("channel")
    if release_channel not in RELEASE_CHANNELS:
        raise PublicationError("release manifest has missing or invalid channel")
    publication_channel = "stable" if release_channel == "stable" else "experimental"
    if publication_channel != expected_channel:
        raise PublicationError(
            f"release manifest channel {release_channel!r} does not match "
            f"requested {expected_channel!r} publication"
        )

    run_id = manifest.get("release_workflow_run_id")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise PublicationError(
            "release manifest has missing or invalid workflow run ID"
        )
    if run_id != selected_run_id:
        raise PublicationError(
            f"release manifest run ID {run_id} does not match selected run {selected_run_id}"
        )
    run_attempt = manifest.get("release_workflow_run_attempt")
    if (
        isinstance(run_attempt, bool)
        or not isinstance(run_attempt, int)
        or run_attempt <= 0
    ):
        raise PublicationError(
            "release manifest has missing or invalid workflow run attempt"
        )
    if run_attempt != selected_run_attempt:
        raise PublicationError(
            f"release manifest run attempt {run_attempt} does not match selected "
            f"attempt {selected_run_attempt}"
        )

    source_sha = manifest.get("source_sha")
    source_ref = manifest.get("source_ref")
    if (
        not isinstance(source_sha, str)
        or re.fullmatch(r"[0-9a-f]{40}", source_sha) is None
    ):
        raise PublicationError("release manifest has invalid source SHA")
    if not isinstance(source_ref, str):
        raise PublicationError("release manifest has missing source ref")
    if (
        publication_channel == "experimental"
        and source_ref != "refs/heads/experimental"
    ):
        raise PublicationError(
            "experimental manifest must come from refs/heads/experimental"
        )
    if publication_channel == "stable" and not source_ref.startswith("refs/tags/"):
        raise PublicationError("stable manifest must come from a tag ref")

    return ValidatedManifest(release_channel, publication_channel, run_id, run_attempt)


def _safe_relative_files(root: Path) -> list[Path]:
    files: list[Path] = []
    if not root.is_dir():
        return files
    resolved_root = root.resolve()
    for path in root.rglob("*"):
        if path.is_symlink():
            resolved = path.resolve()
            if resolved_root not in resolved.parents or not resolved.is_file():
                raise PublicationError(
                    f"refusing unsafe symlink in repository artifact: {path}"
                )
        if path.is_file():
            files.append(path)
    return sorted(files)


def build_publication_plan(
    repo_root: Path,
    manifest_path: Path,
    expected_channel: str,
    selected_run_id: int,
    selected_run_attempt: int,
    public_key_path: Path | None = None,
) -> list[PublicationObject]:
    validated = validate_manifest(
        manifest_path, expected_channel, selected_run_id, selected_run_attempt
    )
    prefix = "" if validated.publication_channel == "stable" else "experimental/"
    objects: list[PublicationObject] = []

    for directory in REPOSITORY_DIRS:
        root = repo_root / directory
        for path in _safe_relative_files(root):
            relative = path.relative_to(root).as_posix()
            key = f"{prefix}{directory}/{relative}"
            is_package = key.endswith(PACKAGE_SUFFIXES)
            objects.append(
                PublicationObject(
                    source=path,
                    key=key,
                    immutable=is_package,
                    phase="payload" if is_package else "metadata",
                    content_type=content_type(key),
                )
            )

    if not objects:
        raise PublicationError("no package repository objects were found")

    if public_key_path is not None:
        if not public_key_path.is_file():
            raise PublicationError(f"missing public signing key: {public_key_path}")
        key = (
            "gpg.key"
            if validated.publication_channel == "stable"
            else ("experimental/aptrepo/gpg.key")
        )
        objects.append(
            PublicationObject(
                source=public_key_path,
                key=key,
                immutable=False,
                phase="metadata",
                content_type=content_type(key),
            )
        )

    latest_key = (
        "latest.json"
        if validated.publication_channel == "stable"
        else ("experimental/latest.json")
    )
    objects.append(
        PublicationObject(
            source=manifest_path,
            key=latest_key,
            immutable=False,
            phase="latest",
            content_type="application/json",
        )
    )

    phase_order = {"payload": 0, "metadata": 1, "latest": 2}
    return sorted(objects, key=lambda item: (phase_order[item.phase], item.key))


def publish(plan: list[PublicationObject], store: ObjectStore) -> None:
    latest_seen = False
    for item in plan:
        if latest_seen:
            raise PublicationError(
                "publication plan contains objects after latest.json"
            )
        if item.phase == "latest":
            latest_seen = True

        if item.immutable:
            existing = store.get(item.key)
            if existing is not None:
                existing_digest = sha256_bytes(existing)
                source_digest = sha256_file(item.source)
                if existing_digest == source_digest:
                    print(f"immutable object already matches; skipping {item.key}")
                    continue
                raise PublicationError(
                    f"immutable object conflict for {item.key}: "
                    f"existing sha256={existing_digest}, source sha256={source_digest}"
                )
        store.put(item.key, item.source, item.content_type)

    if not latest_seen:
        raise PublicationError("publication plan did not contain latest.json")


class WranglerObjectStore:
    def __init__(self, wrangler: Path, bucket: str) -> None:
        self.wrangler = wrangler
        self.bucket = bucket

    def _target(self, key: str) -> str:
        return f"{self.bucket}/{key}"

    def get(self, key: str) -> bytes | None:
        with tempfile.NamedTemporaryFile(delete=False) as temporary:
            output = Path(temporary.name)
        try:
            result = subprocess.run(
                [
                    str(self.wrangler),
                    "r2",
                    "object",
                    "get",
                    self._target(key),
                    "--remote",
                    "--file",
                    str(output),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return output.read_bytes()
            diagnostic = f"{result.stdout}\n{result.stderr}".strip()
            if MISSING_OBJECT_RE.search(diagnostic):
                return None
            raise PublicationError(
                f"failed to check immutable object {key!r}; refusing overwrite: {diagnostic}"
            )
        finally:
            output.unlink(missing_ok=True)

    def put(self, key: str, source: Path, content_type: str) -> None:
        print(f"uploading {key}", file=sys.stderr)
        result = subprocess.run(
            [
                str(self.wrangler),
                "r2",
                "object",
                "put",
                self._target(key),
                "--remote",
                "--file",
                str(source),
                "--content-type",
                content_type,
                "--force",
            ],
            check=False,
        )
        if result.returncode != 0:
            raise PublicationError(f"R2 upload failed for {key}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--expected-channel", choices=("stable", "experimental"), required=True
    )
    parser.add_argument("--selected-run-id", type=int, required=True)
    parser.add_argument("--selected-run-attempt", type=int, required=True)
    parser.add_argument("--bucket", default="yams-repository")
    parser.add_argument("--wrangler", type=Path, required=True)
    parser.add_argument("--public-key", type=Path)
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        plan = build_publication_plan(
            args.repo_root,
            args.manifest,
            args.expected_channel,
            args.selected_run_id,
            args.selected_run_attempt,
            args.public_key,
        )
        if args.plan_only:
            print(
                json.dumps(
                    [
                        {
                            "key": item.key,
                            "immutable": item.immutable,
                            "phase": item.phase,
                            "content_type": item.content_type,
                        }
                        for item in plan
                    ],
                    indent=2,
                )
            )
            return 0
        if not os.environ.get("CLOUDFLARE_API_TOKEN") or not os.environ.get(
            "CLOUDFLARE_ACCOUNT_ID"
        ):
            raise PublicationError("Cloudflare API token/account missing")
        publish(plan, WranglerObjectStore(args.wrangler, args.bucket))
    except (OSError, PublicationError) as error:
        print(f"repository publication error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later
"""Delete old nightly and weekly prereleases and their tags. Stable releases are never touched.

Keeps the newest --keep published releases per channel. Read-only unless --apply is given.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

REPOSITORY = "trvon/yams"
MAX_PAGES = 10
ORPHAN_TAG_MIN_AGE_DAYS = 2

# Channel tag shapes. Anything else (v*, yams-v*, unknown) is out of scope.
CHANNELS = {
    "nightly": (
        re.compile(r"^experimental-nightly-(\d{8})-[0-9a-f]{40}$"),
        re.compile(r"^nightly-(\d{8})-[0-9a-f]{7,40}$"),
    ),
    "weekly": (re.compile(r"^weekly-(\d{4})w(\d{2})-[0-9a-f]{7,40}$"),),
}


class APIError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"GitHub API returned HTTP {status}")


def channel_of(tag):
    """Return (channel, date) for a prerelease tag, or None when the tag is out of scope."""
    if not isinstance(tag, str):
        return None
    for channel, patterns in CHANNELS.items():
        for pattern in patterns:
            match = pattern.match(tag)
            if not match:
                continue
            if channel == "weekly":
                year, week = int(match.group(1)), int(match.group(2))
                try:
                    return channel, dt.date.fromisocalendar(year, week, 1)
                except ValueError:
                    return None
            try:
                return channel, dt.datetime.strptime(match.group(1), "%Y%m%d").date()
            except ValueError:
                return None
    return None


def select(releases, tags, keep, today):
    """Plan deletions.

    releases: [{"id", "tag_name", "draft", "prerelease", "published_at"}]
    tags: tag names that exist in the repository
    Returns (release_ids, tag_names) to delete.
    """
    if keep < 1:
        raise ValueError("keep must be at least 1")
    by_channel = {}
    release_tags = set()
    for release in releases:
        tag = release.get("tag_name")
        release_tags.add(tag)
        scoped = channel_of(tag)
        if scoped is None or release.get("draft") or not release.get("published_at"):
            continue
        by_channel.setdefault(scoped[0], []).append(release)

    delete_ids, delete_tags, kept_tags = [], [], set()
    for channel_releases in by_channel.values():
        channel_releases.sort(key=lambda r: (r["published_at"], r["id"]), reverse=True)
        for release in channel_releases[:keep]:
            kept_tags.add(release["tag_name"])
        for release in channel_releases[keep:]:
            delete_ids.append(release["id"])
            delete_tags.append(release["tag_name"])

    # Tags left behind by deleted or never-created releases. Skip recent ones so a release
    # that is still being created keeps its tag.
    cutoff = today - dt.timedelta(days=ORPHAN_TAG_MIN_AGE_DAYS)
    for tag in tags:
        scoped = channel_of(tag)
        if scoped is None or tag in release_tags or tag in kept_tags:
            continue
        if scoped[1] <= cutoff:
            delete_tags.append(tag)
    return delete_ids, sorted(set(delete_tags))


class GitHubAPI:
    def __init__(self, token):
        if not token:
            raise RuntimeError("GITHUB_TOKEN is required")
        self.token = token

    def call(self, method, path):
        request = urllib.request.Request(
            f"https://api.github.com/repos/{REPOSITORY}/{path}",
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "yams-prune-prereleases",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                body = response.read(8 * 1024 * 1024)
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            raise APIError(error.code) from None

    def paged(self, path):
        items = []
        for page in range(1, MAX_PAGES + 1):
            sep = "&" if "?" in path else "?"
            batch = self.call("GET", f"{path}{sep}per_page=100&page={page}")
            if not batch:
                break
            items.extend(batch)
            if len(batch) < 100:
                break
        return items


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", type=int, default=3, help="releases to keep per channel")
    parser.add_argument("--apply", action="store_true", help="delete; default is a dry run")
    args = parser.parse_args(argv)
    if args.keep < 1:
        parser.error("--keep must be at least 1")

    api = GitHubAPI(os.environ.get("GITHUB_TOKEN"))
    releases = api.paged("releases")
    tags = []
    for prefix in ("nightly-", "experimental-nightly-", "weekly-"):
        refs = api.call("GET", f"git/matching-refs/tags/{urllib.parse.quote(prefix)}") or []
        tags.extend(ref["ref"].removeprefix("refs/tags/") for ref in refs)

    delete_ids, delete_tags = select(releases, tags, args.keep, dt.datetime.now(dt.timezone.utc).date())
    names = {r["id"]: r["tag_name"] for r in releases}
    mode = "apply" if args.apply else "dry-run"
    print(f"[{mode}] keep={args.keep} releases={len(delete_ids)} tags={len(delete_tags)}")
    for release_id in delete_ids:
        print(f"release {names[release_id]}")
        if args.apply:
            api.call("DELETE", f"releases/{release_id}")
    for tag in delete_tags:
        print(f"tag {tag}")
        if args.apply:
            try:
                api.call("DELETE", f"git/refs/tags/{urllib.parse.quote(tag)}")
            except APIError as error:
                if error.status != 422:  # already gone
                    raise
    return 0


if __name__ == "__main__":
    sys.exit(main())

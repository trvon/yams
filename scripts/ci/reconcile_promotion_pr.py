#!/usr/bin/env python3
"""Main-owned draft visibility only. Default to a read-only plan; never promote."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPOSITORY = "trvon/yams"
START = "<!-- yams:draft-promotion:start -->"
END = "<!-- yams:draft-promotion:end -->"
MAX_PAGES = 5


class PolicyError(RuntimeError):
    """Refuse ambiguous or untrusted reconciliation without mutating a PR."""


class APIError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(
            f"GitHub API returned HTTP {status}; no permissions/settings were changed"
        )


def object_value(value):
    if not isinstance(value, dict):
        raise PolicyError("Expected a JSON object")
    return value


def sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise PolicyError("Expected an immutable commit SHA")
    return value


def trusted_context(env, event):
    expected = REPOSITORY + "/.github/workflows/draft-promotion.yml@refs/heads/main"
    if (
        env.get("GITHUB_REPOSITORY") != REPOSITORY
        or env.get("GITHUB_REF") != "refs/heads/main"
        or env.get("GITHUB_WORKFLOW_REF") != expected
    ):
        raise PolicyError(
            "Reconciliation must run from the canonical main-owned workflow"
        )
    sha(env.get("GITHUB_WORKFLOW_SHA"))
    repo = object_value(object_value(event).get("repository"))
    if repo.get("full_name") != REPOSITORY or repo.get("default_branch") != "main":
        raise PolicyError("Unexpected repository/default branch")
    name = env.get("GITHUB_EVENT_NAME")
    if name == "workflow_dispatch":
        return True
    if name != "workflow_run" or event.get("action") != "requested":
        raise PolicyError("Expected a requested Tests run or main-only manual recovery")
    run = object_value(event.get("workflow_run"))
    if (
        run.get("name") != "Tests"
        or run.get("path") != ".github/workflows/tests.yml"
        or run.get("event") != "push"
        or run.get("head_branch") != "experimental"
        or object_value(run.get("head_repository")).get("full_name") != REPOSITORY
    ):
        raise PolicyError(
            "Expected a canonical experimental push to the Tests workflow"
        )
    sha(run.get("head_sha"))
    # Success, mergeability and ancestry are intentionally NOT creation conditions.
    return False


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise APIError(code)  # Never forward the authorization header elsewhere.


class GitHubAPI:
    def __init__(self, token):
        if not token:
            raise PolicyError("GITHUB_TOKEN is required")
        self.token = token
        self.opener = urllib.request.build_opener(NoRedirect())

    def call(self, method, path, data=None):
        request = urllib.request.Request(
            f"https://api.github.com/repos/{REPOSITORY}/{path}",
            method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "yams-draft-promotion",
            },
        )
        try:
            with self.opener.open(request, timeout=20) as response:
                body = response.read(4 * 1024 * 1024 + 1)
                if len(body) > 4 * 1024 * 1024:
                    raise PolicyError(
                        "GitHub response exceeded the bounded response budget"
                    )
                return json.loads(body)
        except urllib.error.HTTPError as error:
            # Do not log request headers, response bodies, credentials or human PR text.
            raise APIError(error.code) from None


def exact_pull(pr, state):
    pr = object_value(pr)
    head, base = object_value(pr.get("head")), object_value(pr.get("base"))
    if (
        pr.get("state") != state
        or head.get("ref") != "experimental"
        or base.get("ref") != "main"
        or object_value(head.get("repo")).get("full_name") != REPOSITORY
        or object_value(base.get("repo")).get("full_name") != REPOSITORY
        or type(pr.get("number")) is not int
        or pr["number"] <= 0
    ):
        raise PolicyError(
            "PR response does not identify the exact canonical branch pair"
        )
    return pr


def list_pulls(api, state):
    matches = []
    for page in range(1, MAX_PAGES + 1):
        query = urllib.parse.urlencode(
            {
                "state": state,
                "base": "main",
                "head": "trvon:experimental",
                "per_page": 100,
                "page": page,
            }
        )
        rows = api.call("GET", "pulls?" + query)
        if not isinstance(rows, list):
            raise PolicyError("Expected a PR list")
        matches.extend(exact_pull(pr, state) for pr in rows)
        if len(rows) < 100:
            return matches
    raise PolicyError(
        "PR pagination budget exhausted; reconcile manually instead of guessing"
    )


def owned_section(base, head, ahead, behind):
    return f"""{START}
## Automated promotion visibility (not approval)

Observed `main`: `{base}`; `experimental`: `{head}`.
Experimental is {ahead} commits ahead and {behind} behind this observed main.
This snapshot is informational, not a validation of the current PR head.

Promotion, merge, version selection and publication remain maintainer decisions.
Release Please owns version changes; this automation never marks a PR ready or merges it.

**Check-trigger warning:** PRs created with `GITHUB_TOKEN` do not trigger the normal
PR-opened workflows. A maintainer must verify all required checks for the current head;
if absent, manually **close and reopen** this PR using their own authenticated account.
Do not treat this draft or the upstream Tests event as release approval.

This is an initial snapshot. Future runs leave existing PR text and state untouched.
{END}"""


def reconcile(api, env, event, *, apply=False):
    manual = trusted_context(env, event)  # Before even read-only API requests.
    base = sha(object_value(api.call("GET", "git/ref/heads/main"))["object"]["sha"])
    head = sha(
        object_value(api.call("GET", "git/ref/heads/experimental"))["object"]["sha"]
    )
    # GitHub serves changed-file patches only on page 1. Page 2 retains summary counts,
    # even with zero/one commits, without downloading arbitrary candidate file patches.
    comparison = object_value(
        api.call("GET", f"compare/{base}...{head}?per_page=1&page=2")
    )
    ahead, behind = comparison.get("ahead_by"), comparison.get("behind_by")
    if any(type(n) is not int or n < 0 for n in (ahead, behind)):
        raise PolicyError("Invalid comparison counts")
    summary = {"base": base, "head": head, "ahead": ahead, "behind": behind}
    if ahead == 0:
        return {**summary, "action": "no-changes"}
    section = owned_section(base, head, ahead, behind)

    def existing_summary(pulls):
        if len(pulls) != 1:
            raise PolicyError("Ambiguous open promotion PRs")
        # GitHub already updates the PR's branch diff on push. Do not PATCH metadata:
        # full-body writes can overwrite concurrent human edits, even within a
        # workflow concurrency group. No undocumented conditional-write assumption.
        return {**summary, "action": "unchanged", "number": pulls[0]["number"]}

    opened = list_pulls(api, "open")
    if opened:
        return existing_summary(opened)
    if not manual:
        closed = list_pulls(api, "closed")
        if any(sha(pr["head"].get("sha")) == head for pr in closed):
            return {**summary, "action": "closed-current-head"}
    if not apply:
        return {**summary, "action": "would-create"}
    try:
        created = api.call(
            "POST",
            "pulls",
            {
                "title": "chore: promote experimental to main",
                "head": "experimental",
                "base": "main",
                "draft": True,
                "body": section,
            },
        )
        pr = exact_pull(created, "open")
        if pr.get("draft") is not True:
            raise PolicyError(
                "Creation response did not confirm a draft; inspect the PR manually"
            )
        return {**summary, "action": "created", "number": pr["number"]}
    except APIError as error:
        if error.status != 422:
            raise
        # A maintainer/another run may have created the PR after our initial read.
        opened = list_pulls(api, "open")
        if not opened:
            raise
        return existing_summary(opened)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create a missing draft visibility PR; never update an existing PR",
    )
    args = parser.parse_args()
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
        trusted_context(os.environ, event)
        result = reconcile(
            GitHubAPI(os.environ.get("GITHUB_TOKEN")),
            os.environ,
            event,
            apply=args.apply,
        )
    except (
        PolicyError,
        APIError,
        urllib.error.URLError,
        ValueError,
        KeyError,
        TypeError,
        OSError,
    ) as error:
        parser.exit(1, f"Draft reconciliation failed: {error}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Offline contract tests: no GitHub credentials or network are used."""

import copy
import importlib.util
import unittest
import urllib.error
from email.message import Message
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "promotion", ROOT / "scripts/ci/reconcile_promotion_pr.py"
)
assert SPEC is not None and SPEC.loader is not None
promotion = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(promotion)
REPO = "trvon/yams"
MAIN = "a" * 40
HEAD = "b" * 40


def context():
    return {
        "GITHUB_REPOSITORY": REPO,
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_WORKFLOW_REF": REPO
        + "/.github/workflows/draft-promotion.yml@refs/heads/main",
        "GITHUB_WORKFLOW_SHA": MAIN,
        "GITHUB_EVENT_NAME": "workflow_run",
    }


def event():
    return {
        "action": "requested",
        "repository": {"full_name": REPO, "default_branch": "main"},
        "workflow_run": {
            "name": "Tests",
            "path": ".github/workflows/tests.yml",
            "event": "push",
            "head_branch": "experimental",
            "head_sha": HEAD,
            "head_repository": {"full_name": REPO},
            "conclusion": "failure",
        },
    }


def pull(body="Maintainer notes", draft=False):
    return {
        "number": 123,
        "state": "open",
        "body": body,
        "title": "Human title",
        "draft": draft,
        "head": {"ref": "experimental", "sha": HEAD, "repo": {"full_name": REPO}},
        "base": {"ref": "main", "repo": {"full_name": REPO}},
    }


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.open = []
        self.closed = []
        self.ahead = 161
        self.race = False

    def call(self, method, path, data=None):
        self.calls.append((method, path, copy.deepcopy(data)))
        if path == "git/ref/heads/main":
            return {"object": {"sha": MAIN}}
        if path == "git/ref/heads/experimental":
            return {"object": {"sha": HEAD}}
        if path.startswith("compare/"):
            self.assert_compare = path
            return {"ahead_by": self.ahead, "behind_by": 2, "status": "diverged"}
        if path.startswith("pulls?"):
            return copy.deepcopy(self.closed if "state=closed" in path else self.open)
        if method == "POST" and path == "pulls":
            assert data is not None
            if self.race:
                self.race = False
                self.open = [pull()]
                raise promotion.APIError(422)
            self.open = [pull(data["body"], draft=data["draft"])]
            return copy.deepcopy(self.open[0])
        if method == "PATCH" and path == "pulls/123":
            assert data is not None
            self.open[0]["body"] = data["body"]
            return copy.deepcopy(self.open[0])
        raise AssertionError((method, path, data))

    def writes(self):
        return [c for c in self.calls if c[0] != "GET"]


class PromotionTests(unittest.TestCase):
    def run_reconcile(self, api, **kwargs):
        return promotion.reconcile(api, context(), event(), **kwargs)

    def test_default_is_read_only_plan(self):
        api = FakeAPI()
        result = self.run_reconcile(api)
        self.assertEqual(result["action"], "would-create")
        self.assertFalse(api.writes())

    def test_divergent_failed_tests_create_only_draft(self):
        api = FakeAPI()
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "created")
        self.assertEqual(len(api.writes()), 1)
        _, path, payload = api.writes()[0]
        self.assertEqual(path, "pulls")
        self.assertEqual(set(payload), {"title", "body", "head", "base", "draft"})
        self.assertTrue(payload["draft"])
        self.assertEqual((payload["head"], payload["base"]), ("experimental", "main"))
        self.assertIn("GITHUB_TOKEN", payload["body"])
        self.assertIn("close and reopen", payload["body"])
        self.assertIn(MAIN + "..." + HEAD, api.assert_compare)
        self.assertIn("page=2", api.assert_compare)

    def test_second_run_is_noop_and_preserves_ready_human_pr(self):
        api = FakeAPI()
        api.open = [pull()]
        original = copy.deepcopy(api.open)
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "unchanged")
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "unchanged")
        self.assertEqual(api.open, original)
        self.assertFalse(api.writes())

    def test_no_ahead_commits_does_not_create_or_close(self):
        api = FakeAPI()
        api.ahead = 0
        api.open = [pull()]
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "no-changes")
        self.assertFalse(api.writes())

    def test_closed_same_head_is_respected_unless_manually_requested(self):
        api = FakeAPI()
        closed = pull()
        closed["state"] = "closed"
        api.closed = [closed]
        self.assertEqual(
            self.run_reconcile(api, apply=True)["action"], "closed-current-head"
        )
        self.assertFalse(api.writes())
        env = context()
        env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        result = promotion.reconcile(api, env, event(), apply=True)
        self.assertEqual(result["action"], "created")

    def test_trust_failures_do_not_even_read_api(self):
        variants = []
        for key, value in [
            ("GITHUB_REF", "refs/heads/experimental"),
            ("GITHUB_REPOSITORY", "fork/yams"),
            ("GITHUB_WORKFLOW_REF", "fork/yams/workflow@refs/heads/main"),
            ("GITHUB_WORKFLOW_SHA", "main"),
            ("GITHUB_EVENT_NAME", "pull_request_target"),
        ]:
            env = context()
            env[key] = value
            variants.append((env, event()))
        for key, value in [
            ("event", "pull_request"),
            ("head_branch", "main"),
            ("head_repository", {"full_name": "fork/yams"}),
            ("path", ".github/workflows/other.yml"),
            ("name", "Pretend Tests"),
            ("head_sha", "bad"),
        ]:
            ev = event()
            ev["workflow_run"][key] = value
            variants.append((context(), ev))
        ev = event()
        ev["action"] = "completed"
        variants.append((context(), ev))
        ev = event()
        ev["repository"]["default_branch"] = "experimental"
        variants.append((context(), ev))
        for env, ev in variants:
            with self.subTest(env=env, event=ev):
                api = FakeAPI()
                with self.assertRaises(promotion.PolicyError):
                    promotion.reconcile(api, env, ev, apply=True)
                self.assertFalse(api.calls)

    def test_even_malformed_existing_markers_are_left_untouched(self):
        for body in [
            "notes " + promotion.START,
            promotion.END + promotion.START,
            promotion.START + promotion.START + promotion.END,
        ]:
            api = FakeAPI()
            api.open = [pull(body)]
            self.assertEqual(self.run_reconcile(api, apply=True)["action"], "unchanged")
            self.assertEqual(api.open[0]["body"], body)
            self.assertFalse(api.writes())

    def test_fork_or_ambiguous_api_pr_response_fails_closed(self):
        for pulls in [[pull(), pull()], [pull()]]:
            api = FakeAPI()
            api.open = pulls
            if len(pulls) == 1:
                pulls[0]["head"]["repo"]["full_name"] = "fork/yams"
            with self.assertRaises(promotion.PolicyError):
                self.run_reconcile(api, apply=True)
            self.assertFalse(api.writes())

    def test_creation_race_rereads_without_duplicate_creation(self):
        api = FakeAPI()
        api.race = True
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "unchanged")
        self.assertEqual(sum(c[0] == "POST" for c in api.calls), 1)
        self.assertEqual(len(api.writes()), 1)  # No follow-up PATCH to the winning PR.

    def test_concurrent_human_edit_and_closure_cannot_be_overwritten(self):
        api = FakeAPI()
        api.open = [pull()]
        call = api.call

        def interleaved(method, path, data=None):
            result = call(method, path, data)
            if path.startswith("pulls?") and "state=open" in path:
                api.open[0].update(
                    body="New human decision", state="closed", title="Do not promote"
                )
            return (
                result  # Return the older observation, not the concurrent human edit.
            )

        api.call = interleaved
        self.assertEqual(self.run_reconcile(api, apply=True)["action"], "unchanged")
        self.assertFalse(api.writes())
        self.assertEqual(api.open[0]["body"], "New human decision")
        self.assertEqual(api.open[0]["state"], "closed")
        self.assertEqual(api.open[0]["title"], "Do not promote")

    def test_creation_response_must_confirm_draft_without_auto_remediation(self):
        for draft in [False, None, "true", 1]:
            with self.subTest(draft=draft):
                api = FakeAPI()
                call = api.call

                def unconfirmed(method, path, data=None, *, call=call, draft=draft):
                    result = call(method, path, data)
                    if method == "POST":
                        result["draft"] = draft
                        if draft is None:
                            del result["draft"]
                    return result

                api.call = unconfirmed
                with self.assertRaisesRegex(promotion.PolicyError, "confirm a draft"):
                    self.run_reconcile(api, apply=True)
                self.assertEqual([c[0] for c in api.writes()], ["POST"])

    def test_malformed_transport_json_is_rejected(self):
        api = promotion.GitHubAPI("fixture-token-never-sent")
        api.opener = MagicMock()
        api.opener.open.return_value.__enter__.return_value.read.return_value = (
            b"not-json"
        )
        with self.assertRaises(ValueError):
            api.call("GET", "git/ref/heads/main")

    def test_invalid_counts_and_pagination_exhaustion_fail_closed(self):
        api = FakeAPI()
        api.ahead = True  # bool is not a valid integer count.
        with self.assertRaises(promotion.PolicyError):
            self.run_reconcile(api, apply=True)
        self.assertFalse(api.writes())
        api = FakeAPI()
        api.open = [pull() for _ in range(100)]
        with self.assertRaisesRegex(promotion.PolicyError, "pagination budget"):
            self.run_reconcile(api, apply=True)
        self.assertFalse(api.writes())
        self.assertEqual(
            sum(p.startswith("pulls?") for _, p, _ in api.calls), promotion.MAX_PAGES
        )

    def test_permission_failure_does_not_report_creation_or_change_settings(self):
        api = FakeAPI()
        call = api.call

        def denied(method, path, data=None):
            if method == "POST":
                raise promotion.APIError(403)
            return call(method, path, data)

        api.call = denied
        with self.assertRaises(promotion.APIError) as raised:
            self.run_reconcile(api, apply=True)
        self.assertEqual(raised.exception.status, 403)
        self.assertFalse(api.open)

    def test_transport_budget_redirect_and_error_redaction(self):
        api = promotion.GitHubAPI("fixture-token-never-sent")
        api.opener = MagicMock()
        response = api.opener.open.return_value.__enter__.return_value
        response.read.return_value = b"x" * (4 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(promotion.PolicyError, "response budget"):
            api.call("GET", "git/ref/heads/main")
        response.read.assert_called_once_with(4 * 1024 * 1024 + 1)
        self.assertEqual(api.opener.open.call_args.kwargs["timeout"], 20)
        with self.assertRaises(promotion.APIError):
            promotion.NoRedirect().redirect_request(
                None, None, 302, "Found", {}, "https://example.invalid/"
            )
        api.opener.open.side_effect = urllib.error.HTTPError(
            "https://api.github.com/",
            403,
            "secret fixture-token-never-sent",
            Message(),
            None,
        )
        with self.assertRaises(promotion.APIError) as raised:
            api.call("GET", "git/ref/heads/main")
        self.assertNotIn("fixture-token", str(raised.exception))
        self.assertEqual(raised.exception.status, 403)

    def test_workflow_authority_and_main_owned_checkout(self):
        text = (ROOT / ".github/workflows/draft-promotion.yml").read_text()
        self.assertIn("types: [requested]", text)
        self.assertIn("ref: ${{ github.workflow_sha }}", text)
        self.assertIn("persist-credentials: false", text)
        self.assertIn("pull-requests: write", text)
        self.assertIn("cancel-in-progress: false", text)
        for forbidden in [
            "pull_request_target:",
            "contents: write",
            "actions: write",
            "head.sha",
            "secrets.PAT",
            "--merge",
        ]:
            self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()

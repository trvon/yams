#!/usr/bin/env python3
"""Contracts for the provider-independent YAMS reporting example."""

import hashlib
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "research_note.py"
spec = importlib.util.spec_from_file_location("research_note", SCRIPT)
assert spec is not None and spec.loader is not None
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def source(content='Quoted "result"\nsecond line\n'):
    encoded = content.encode("utf-8")
    return {
        "schema_version": 1,
        "hash": hashlib.sha256(encoded).hexdigest(),
        "name": "checkpoint.md",
        "path": "notes/checkpoint.md",
        "size": len(encoded),
        "mime_type": "text/markdown",
        "metadata": {"task": "research", "phase": "checkpoint"},
        "content": content,
        "has_content": True,
        "encoding": "utf-8",
        "representation": "original",
        "truncated": False,
    }


def entry(document, path=None):
    return {
        "hash": document["hash"],
        "path": path or document["path"],
        "metadata": document["metadata"],
        "tags": ["checkpoint"],
    }


class ResearchNoteTests(unittest.TestCase):
    def test_paginates_using_page_length_not_page_local_total(self):
        first, second = source("first\n"), source("second\n")
        client = Mock()
        client.json.side_effect = [
            {"documents": [entry(first)], "total": 1},
            {"documents": [entry(second)], "total": 1},
            {"documents": [], "total": 0},
            first,
            second,
        ]
        result = report.collect_sources(
            client, ["--metadata", "task=research"], page_size=1
        )
        self.assertEqual(result["sources"], [first, second])
        calls = client.json.call_args_list
        self.assertEqual(
            [call.args[call.args.index("--offset") + 1] for call in calls[:3]],
            ["0", "1", "2"],
        )
        self.assertIn("task=research", calls[0].args)
        self.assertEqual(calls[3].args, ("get", "--hash", first["hash"]))

    def test_same_content_is_fetched_once_without_losing_selected_metadata(self):
        document = source()
        a, b = entry(document), entry(document, "other/note.md")
        b["metadata"] = {"task": "other"}
        client = Mock()
        client.json.side_effect = [{"documents": [a, b]}, document]
        result = report.collect_sources(client, [])
        self.assertEqual(result["documents"], [a, b])
        self.assertEqual(result["sources"], [document])
        self.assertEqual(client.json.call_count, 2)

    def test_repeated_full_page_fails_instead_of_looping_forever(self):
        client = Mock()
        page = {"documents": [entry(source())]}
        client.json.side_effect = [page, page]
        with self.assertRaisesRegex(ValueError, "pagination"):
            report.collect_sources(client, [], page_size=1)

    def test_missing_identity_and_incomplete_list_are_errors(self):
        for payload in ({}, {"documents": [{}]}, {"documents": "not an array"}):
            with self.subTest(payload=payload):
                client = Mock()
                client.json.return_value = payload
                with self.assertRaises(ValueError):
                    report.collect_sources(client, [])

    def test_hydration_must_match_the_selected_full_hash_and_original_bytes(self):
        for change in (
            {"hash": "a" * 64},
            {"content": "not the stored artifact"},
            {"truncated": True},
            {"representation": "extracted"},
            {"has_content": False},
            {"size": 999},
        ):
            with self.subTest(change=change):
                original = source()
                modified = original | change
                client = Mock()
                client.json.side_effect = [{"documents": [entry(original)]}, modified]
                with self.assertRaises(ValueError):
                    report.collect_sources(client, [])

    def test_explicit_limit_fails_rather_than_silently_truncating(self):
        document = source()
        client = Mock()
        client.json.return_value = {
            "documents": [entry(document), entry(document, "other.md")]
        }
        with self.assertRaisesRegex(ValueError, "limit"):
            report.collect_sources(client, [], max_documents=1)

    def test_client_preserves_corpus_scope_and_never_uses_a_shell(self):
        completed = subprocess.CompletedProcess(
            [], 0, '{"documents": []}', "diagnostic"
        )
        with patch.object(report.subprocess, "run", return_value=completed) as run:
            client = report.YamsClient("/opt/yams", "/corpora/my corpus", 10)
            self.assertEqual(client.json("list"), {"documents": []})
        self.assertEqual(
            run.call_args.args[0],
            ["/opt/yams", "--data-dir", "/corpora/my corpus", "--json", "list"],
        )
        self.assertFalse(run.call_args.kwargs["shell"])
        self.assertEqual(run.call_args.kwargs["timeout"], 10)

    def test_default_report_preserves_source_and_handles_backtick_fences(self):
        document = source("text\n```\nuntrusted source\n")
        result = report.render_report({"sources": [document]}, "Weekly work")
        self.assertIn(document["hash"], result)
        self.assertIn(document["content"], result)
        self.assertIn("````text", result)

    def test_model_command_receives_sources_without_shell_interpolation(self):
        bundle = {"sources": [source()]}
        completed = subprocess.CompletedProcess([], 0, "Draft with a citation", "")
        with patch.object(report.subprocess, "run", return_value=completed) as run:
            text = report.render_report(
                bundle,
                "Weekly work",
                "model --name 'local model'",
                "Use a decisions section.",
            )
        self.assertEqual(run.call_args.args[0], ["model", "--name", "local model"])
        self.assertFalse(run.call_args.kwargs["shell"])
        self.assertIn(bundle["sources"][0]["hash"], run.call_args.kwargs["input"])
        self.assertIn("Use a decisions section.", run.call_args.kwargs["input"])
        self.assertIn("not been verified", text)

    def test_failure_does_not_replace_existing_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.md"
            output.write_text("previous report", encoding="utf-8")
            with patch.object(
                report.YamsClient, "json", side_effect=ValueError("missing source")
            ):
                code = report.main(["--output", str(output)])
            self.assertNotEqual(code, 0)
            self.assertEqual(output.read_text(), "previous report")
            self.assertFalse(Path(str(output) + ".sources.json").exists())

    def test_relative_window_is_resolved_once(self):
        now = report.datetime(2026, 10, 29, 12, tzinfo=report.timezone.utc)
        self.assertEqual(report.resolve_since("7d", now), "2026-10-22T12:00:00Z")
        with self.assertRaises(ValueError):
            report.resolve_since("not a date", now)


if __name__ == "__main__":
    unittest.main()

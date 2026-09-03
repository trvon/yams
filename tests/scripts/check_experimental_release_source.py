#!/usr/bin/env python3
"""Static and scenario checks for immutable experimental release sources."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Scenario:
    event: str
    channel: str
    event_sha: str
    experimental_sha: str
    expected_sha: str
    expected_channel: str


def resolve_source(scenario: Scenario) -> tuple[str, str]:
    channel = scenario.channel or "nightly"
    use_experimental = scenario.event == "schedule" or (
        scenario.event == "workflow_dispatch" and channel != "stable"
    )
    source_sha = scenario.experimental_sha if use_experimental else scenario.event_sha
    effective_channel = (
        "stable"
        if scenario.event == "push"
        else channel
        if channel in {"stable", "weekly", "nightly"}
        else "nightly"
    )
    return source_sha, effective_channel


def scenario_failures() -> list[str]:
    cases = (
        Scenario("schedule", "", "main-sha", "exp-sha", "exp-sha", "nightly"),
        Scenario(
            "workflow_dispatch",
            "nightly",
            "selected-sha",
            "exp-sha",
            "exp-sha",
            "nightly",
        ),
        Scenario(
            "workflow_dispatch",
            "",
            "selected-sha",
            "exp-sha",
            "exp-sha",
            "nightly",
        ),
        Scenario(
            "workflow_dispatch",
            "weekly",
            "selected-sha",
            "exp-sha",
            "exp-sha",
            "weekly",
        ),
        Scenario(
            "workflow_dispatch", "stable", "tag-sha", "exp-sha", "tag-sha", "stable"
        ),
        Scenario("push", "", "tag-sha", "exp-sha", "tag-sha", "stable"),
        # Divergent history is intentionally reduced to exact SHA inequality; ancestry is irrelevant.
        Scenario(
            "schedule",
            "",
            "divergent-main",
            "divergent-exp",
            "divergent-exp",
            "nightly",
        ),
    )
    failures = []
    for case in cases:
        actual = resolve_source(case)
        expected = (case.expected_sha, case.expected_channel)
        if actual != expected:
            failures.append(
                f"scenario failed: {case}: expected {expected}, got {actual}"
            )

    def should_release(
        last_sha: str, current_sha: str, compare_available: bool
    ) -> bool:
        del (
            compare_available
        )  # Comparison enriches logs; SHA inequality is authoritative.
        return last_sha != current_sha

    if not should_release("divergent-last", "divergent-exp", compare_available=False):
        failures.append("divergent histories must release when exact SHAs differ")
    if should_release("same", "same", compare_available=False):
        failures.append("equal release SHAs must not publish again")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")
    warm = (root / ".github/workflows/matrix-warm.yml").read_text(encoding="utf-8")

    failures = scenario_failures()
    required_release = (
        "name: Resolve immutable release source",
        "ref: 'heads/experimental'",
        "needs: [resolve-release, check-commits]",
        "needs: [resolve-release, warm]",
        "needs: [resolve-release, check-commits, build-release]",
        "ref: ${{ needs.resolve-release.outputs.source_sha }}",
        "target_commitish: ${{ needs.resolve-release.outputs.source_sha }}",
        "group: release-publish-${{ needs.resolve-release.outputs.channel }}-${{ needs.resolve-release.outputs.source_sha }}",
        "cancel-in-progress: false",
        "if (lastSha !== currentSha)",
        "comparison was unavailable",
    )
    required_warm = (
        "checkout_ref:",
        "ref: ${{ inputs.checkout_ref || github.sha }}",
    )
    failures.extend(
        f"release workflow missing control: {value}"
        for value in required_release
        if value not in release
    )
    failures.extend(
        f"warm workflow missing control: {value}"
        for value in required_warm
        if value not in warm
    )

    if release.count("ref: 'heads/experimental'") != 1:
        failures.append("experimental branch must be resolved exactly once")
    if "ref: 'heads/main'" in release:
        failures.append("nightly freshness must not resolve main")
    if release.count("DATE_COMPACT=$(date -u +%Y%m%d)") != 1:
        failures.append("nightly metadata must be derived exactly once")
    if release.count("YEAR=$(date -u +%G)") != 1:
        failures.append("weekly metadata must be derived exactly once")
    if release.count('VERSION="${TAG#yams-v}"') != 1:
        failures.append("stable metadata must be derived exactly once")
    if release.count('BASE_NUMERIC_VERSION="$(git describe') != 1:
        failures.append("base version must be derived exactly once")
    if "github.event.workflow_run.head_sha || github.ref" in release:
        failures.append("release checkout still permits mutable ref drift")

    if failures:
        print("\n".join(failures))
        return 1

    print("experimental release source policy: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

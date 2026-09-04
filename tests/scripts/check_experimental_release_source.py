#!/usr/bin/env python3
"""Static and scenario checks for immutable experimental release sources."""

from __future__ import annotations

import argparse
import re
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


def nightly_tag(date: str, source_sha: str) -> str:
    if re.fullmatch(r"[0-9a-f]{40}", source_sha) is None:
        raise ValueError("nightly tags require a full source SHA")
    if re.fullmatch(r"[0-9]{8}", date) is None:
        raise ValueError("nightly tags require a compact UTC date")
    return f"experimental-nightly-{date}-{source_sha}"


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

    source_sha = "a" * 40
    if nightly_tag("20260101", source_sha) != (
        "experimental-nightly-20260101-" + source_sha
    ):
        failures.append("nightly tag is not immutable")
    try:
        nightly_tag("20260101", "aaaaaaa")
        failures.append("short SHA was accepted for an immutable nightly tag")
    except ValueError:
        pass
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
        'TAG_OUT="experimental-nightly-${DATE_COMPACT}-${SOURCE_SHA}"',
        "immutableNightly = /^experimental-nightly-[0-9]{8}-[0-9a-f]{40}$/",
        "Nightly tag ${tagName} did not resolve safely to a commit",
        "name: Require successful Tests run for exact experimental source",
        "workflow_id: 'tests.yml'",
        "run.status === 'completed'",
        "run.conclusion === 'success'",
        "run.head_sha === sourceSha",
        "tests_run_id: ${{ steps.tests.outputs.tests_run_id }}",
        "tests_run_attempt: ${{ steps.tests.outputs.tests_run_attempt }}",
        "tests_run_url: ${{ steps.tests.outputs.tests_run_url }}",
        "runs_on: ubuntu-24.04-arm",
        "arch_name: x86_64",
        "arch_name: aarch64",
        "docker_platform: linux/amd64",
        "docker_platform: linux/arm64",
        "name: Inspect and install-validate Arch package natively",
        'bash scripts/local-ci/package-validate.sh --only arch --arch "$ARCH_PKG_PATH"',
        "name: Generate and validate release manifest (latest.json)",
        "scripts/ci/generate_release_manifest.py",
        '--source-sha "${{ needs.resolve-release.outputs.source_sha }}"',
        '--tests-run-id "${{ needs.resolve-release.outputs.tests_run_id }}"',
        '--tests-run-attempt "${{ needs.resolve-release.outputs.tests_run_attempt }}"',
        "subject-checksums: assets/SHA256SUMS",
        "uses: actions/attest@11bbd243972067817e9ed160cb123cab3601f436",
        "predicate-type: https://yamsmemory.ai/attestations/release-source/v1",
        "predicate-path: release-provenance.json",
        "id-token: write",
        "attestations: write",
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
    if (
        release.count('TAG_OUT="experimental-nightly-${DATE_COMPACT}-${SOURCE_SHA}"')
        != 1
    ):
        failures.append("nightly tag must be derived exactly once from the full SHA")
    if release.count("YEAR=$(date -u +%G)") != 1:
        failures.append("weekly metadata must be derived exactly once")
    if release.count('VERSION="${TAG#yams-v}"') != 1:
        failures.append("stable metadata must be derived exactly once")
    if release.count('BASE_NUMERIC_VERSION="$(git describe') != 1:
        failures.append("base version must be derived exactly once")
    if "github.event.workflow_run.head_sha || github.ref" in release:
        failures.append("release checkout still permits mutable ref drift")

    arch_x86 = release.find("- os: arch-linux-hosted-x86_64")
    arch_arm = release.find("- os: arch-linux-hosted-aarch64")
    upload = release.find("- name: Upload validated release artifacts")
    arch_validation = release.find(
        "- name: Inspect and install-validate Arch package natively"
    )
    linux_validation = release.find("- name: Validate Linux package install")
    if min(arch_x86, arch_arm, arch_validation, linux_validation, upload) < 0:
        failures.append("release package lane ordering controls are incomplete")
    elif not (arch_x86 < arch_arm < arch_validation < linux_validation < upload):
        failures.append("package validation must complete before artifact upload")

    manifest = release.find("- name: Generate and validate release manifest")
    attestation = release.find(
        "- name: Attest validated release distributables to exact source"
    )
    draft_release = release.find("- name: Create or update GitHub Release")
    publish_release = release.find("- name: Publish GitHub Release")
    if min(manifest, attestation, draft_release, publish_release) < 0:
        failures.append("manifest, attestation, or release publication step missing")
    elif not (manifest < attestation < draft_release < publish_release):
        failures.append(
            "attestation must follow manifest and precede release publication"
        )

    create_release = release[release.find("  create-release:") :]
    if create_release.count("id-token: write") != 1:
        failures.append("create-release must own the only id-token:write grant")
    if release[: release.find("  create-release:")].count("id-token: write") != 0:
        failures.append("id-token:write must not be granted outside create-release")
    if re.search(r"actions/attest(?:-build-provenance)?@(?:v|main|master)", release):
        failures.append("provenance action must be pinned to a full commit SHA")
    if "actions/attest-build-provenance@" in release:
        failures.append(
            "default event-SHA provenance must not describe experimental assets"
        )
    if re.search(r"\bqemu\b|emulat", release, re.IGNORECASE):
        failures.append("Arch lanes must not claim or configure emulation")

    if failures:
        print("\n".join(failures))
        return 1

    print("experimental release source policy: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

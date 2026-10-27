#!/usr/bin/env python3
"""Exercise package script functions without installing packages or running services."""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHECKS = ROOT / "scripts/local-ci/package-validate-checks.sh"
BUILDER = ROOT / "scripts/build-deb.sh"


def shell(source: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", "-c", source], text=True, capture_output=True,
                          env={**os.environ, **(env or {})}, timeout=10)


class PackageVersions(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if int(shell("printf '%s' \"${BASH_VERSINFO[0]}\"").stdout) < 4:
            raise unittest.SkipTest("builder requires Bash 4+; run this suite on Linux")
        source = BUILDER.read_text()
        cls.functions = source[source.index("get_base_version() {"):source.index("prepare_stage_docs() {")]

    def version(self, surface: str, value: str) -> list[str]:
        result = shell(self.functions + f'\ncompute_{surface} "{value}"', {"BASE_VERSION": "0.20.5"})
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.splitlines()

    def test_stable_versions_unchanged(self) -> None:
        self.assertEqual(self.version("deb_version", "0.20.5"), ["0.20.5"])
        self.assertEqual(self.version("rpm_nv", "0.20.5"), ["0.20.5", "1"])

    def test_nightly_deb_is_after_base_stable(self) -> None:
        version = self.version("deb_version", "nightly-20261005-5406C399")[0]
        self.assertEqual(version, "0.20.5+nightly.20261005+g5406c399")
        if shutil.which("dpkg"):
            self.assertEqual(shell(f'dpkg --compare-versions 0.20.5 lt "{version}"').returncode, 0)
            self.assertEqual(shell(f'dpkg --compare-versions "{version}" lt 0.20.6').returncode, 0)

    def test_weekly_deb_is_after_base_stable(self) -> None:
        self.assertEqual(self.version("deb_version", "weekly-2026w41-5406c399"),
                         ["0.20.5+weekly.2026w41+g5406c399"])

    def test_nightly_rpm_is_after_base_release(self) -> None:
        self.assertEqual(self.version("rpm_nv", "nightly-20261005-5406c399"),
                         ["0.20.5", "1.1.nightly.20261005.g5406c399"])

    def test_weekly_rpm_is_after_base_release(self) -> None:
        self.assertEqual(self.version("rpm_nv", "weekly-2026w41-5406c399"),
                         ["0.20.5", "1.1.weekly.2026w41.g5406c399"])

    def test_later_nightly_and_next_stable_order(self) -> None:
        a = self.version("deb_version", "nightly-20261005-5406c399")[0]
        b = self.version("deb_version", "nightly-20261006-5406c399")[0]
        if not shutil.which("dpkg"):
            self.skipTest("Debian comparator unavailable")
        self.assertEqual(shell(f'dpkg --compare-versions "{a}" lt "{b}"').returncode, 0)
        self.assertEqual(shell(f'dpkg --compare-versions "{b}" lt 0.20.6').returncode, 0)


class UpgradeOracle(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "user-units").mkdir()
        (self.root / "system-units").mkdir()
        (self.root / "user-units/yams-daemon.service").write_text("unit\n")
        self.source = CHECKS.read_text().rsplit('case "${PHASE}" in', 1)[0]
        # Relocate fixed in-container scratch paths; never write host /var/tmp or /usr.
        self.source = self.source.replace("/var/tmp/", str(self.root) + "/")
        self.mocks = r'''
TOKEN_FILE="$TEST_ROOT/tokens"
USER_UNIT_DIR="$TEST_ROOT/user-units"
SYSTEM_UNIT_DIR="$TEST_ROOT/system-units"
LEGACY_DATA_DIR="$TEST_ROOT/legacy"
UPGRADE_NOTE="$TEST_ROOT/upgrade-note.txt"
LEGACY_MANIFEST="$TEST_ROOT/legacy.sha256"
ensure_user() { :; }
wait_user_manager() { return 0; }
wait_user_active() { return 0; }
user_journal() { :; }
sleep() { :; }
token() { printf 'fixedseedtoken'; }
check_no_dangling_links() { pass "$1" clean; }
uctl() { case "$*" in *is-enabled*) printf enabled;; esac; }
systemctl() {
    case "$*" in
    is-active*)
        if [ -e "$SYSTEM_UNIT_DIR/yams-daemon.service" ] && [ "${SYSTEM_STOPPED:-0}" != 1 ]; then
            local polls
            polls="$(cat "$TEST_ROOT/system-polls" 2>/dev/null || printf 0)"
            polls=$((polls + 1))
            printf '%s' "$polls" > "$TEST_ROOT/system-polls"
            if [ "$polls" -ge "${SYSTEM_READY_AFTER:-1}" ]; then printf active; return 0; fi
            printf activating
            return 3
        fi
        printf inactive
        return 3
        ;;
    *is-enabled*) printf enabled;;
    stop*) SYSTEM_STOPPED=1; return 0;;
    show*) printf 123;;
    esac
}
as_user() {
    shift
    case "$*" in
    *"yams cat"*) if [ "${BODY_CHANGED:-0}" = 1 ]; then printf corrupted; else command cat "$UPGRADE_NOTE"; fi ;;
    *"yams add"*) return "${ADD_RC:-0}";;
    *) return 0;;
    esac
}
search_finds() { return "${SEARCH_RC:-0}"; }
installed_package_identity() { printf '%s' "${ACTUAL_ID:-yams 0.20.5+nightly.20261005+g5406c399 amd64}"; }
package_identity_from_file() { printf '%s' 'yams 0.20.5+nightly.20261005+g5406c399 amd64'; }
rm() { :; }
apt-get() { return "${INSTALL_RC:-0}"; }
sha256sum() { python3 "$TEST_ROOT/hash.py" "$@"; }
# Minimal Arch images have no diffutils; the checks must not need cmp.
cmp() { echo "cmp: command not found" >&2; return 127; }
'''
        (self.root / "hash.py").write_text('''import hashlib, pathlib, sys
args = [a for a in sys.argv[1:] if a != "--quiet"]
if args and args[0] == "-c":
    for line in pathlib.Path(args[1]).read_text().splitlines():
        digest, name = line.split("  ", 1)
        p = pathlib.Path(name)
        if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest() != digest:
            sys.exit(1)
else:
    for name in args:
        print(hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() + "  " + name)
''')

    def run_shell(self, commands: str, **env: str) -> str:
        result = shell('set -- seed deb dummy.deb\n' + self.source + self.mocks + commands,
                       {"TEST_ROOT": str(self.root), **env})
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def seed(self, **env: str) -> str:
        return self.run_shell('phase_seed\nprintf "MODE=%s\\n" "$(recall UPGRADE_MODE)"\n', **env)

    def seeded_state(self, verified: str = "yes") -> str:
        note = b"package upgrade note fixedseedtoken\n"
        (self.root / "upgrade-note.txt").write_bytes(note)
        digest = hashlib.sha256(note).hexdigest()
        return (f'remember UPGRADE_MODE per-user\nremember LEGACY_PRESENT no\nremember UPGRADE_TOKEN fixedseedtoken\n'
                f'remember UPGRADE_HASH {digest}\nremember INSTALL_VERIFIED {verified}\n'
                'remember INSTALL_EXPECTED "yams 0.20.5+nightly.20261005+g5406c399 amd64"\n')

    def test_per_user_seed_records_and_hydrates_pre_transition_note(self) -> None:
        out = self.seed()
        self.assertIn("MODE=per-user", out)
        self.assertIn("PASS upgrade-seed-body", out)
        self.assertNotIn("FAIL ", out)

    def test_unrecognized_service_fixture_fails(self) -> None:
        (self.root / "user-units/yams-daemon.service").unlink()
        self.assertIn("FAIL upgrade-seed-mode", self.seed())

    def test_legacy_seed_snapshots_existing_bytes(self) -> None:
        (self.root / "system-units/yams-daemon.service").write_text("legacy unit\n")
        (self.root / "legacy").mkdir()
        (self.root / "legacy/blob").write_bytes(b"old corpus bytes")
        out = self.seed()
        self.assertIn("MODE=legacy", out)
        self.assertIn("PASS upgrade-seed-legacy", out)
        self.assertIn("PASS upgrade-seed-body", out)
        self.assertNotIn("FAIL ", out)
        self.assertIn(hashlib.sha256(b"old corpus bytes").hexdigest(),
                      (self.root / "legacy.sha256").read_text())

    def test_legacy_seed_waits_for_actual_active_state(self) -> None:
        (self.root / "system-units/yams-daemon.service").write_text("legacy unit\n")
        (self.root / "legacy").mkdir()
        (self.root / "legacy/blob").write_bytes(b"existing legacy bytes")
        out = self.seed(SYSTEM_READY_AFTER="3")
        self.assertIn("MODE=legacy", out)
        self.assertIn("PASS upgrade-seed-body", out)
        self.assertNotIn("FAIL ", out)
        self.assertEqual((self.root / "system-polls").read_text(), "3")

    def test_legacy_seed_does_not_manufacture_absent_corpus(self) -> None:
        (self.root / "system-units/yams-daemon.service").write_text("legacy unit\n")
        out = self.seed()
        self.assertIn("FAIL upgrade-seed-legacy", out)
        self.assertFalse((self.root / "legacy").exists())
        self.assertIn("MODE=\n", out)

    def test_empty_legacy_corpus_not_accepted_as_hydration(self) -> None:
        (self.root / "system-units/yams-daemon.service").write_text("legacy unit\n")
        (self.root / "legacy").mkdir()
        self.assertIn("FAIL upgrade-seed-legacy", self.seed())

    def test_seed_add_failure_does_not_record_valid_mode(self) -> None:
        out = self.seed(ADD_RC="1")
        self.assertIn("FAIL upgrade-seed-add", out)
        self.assertIn("MODE=\n", out)

    def test_seed_body_mismatch_rejected(self) -> None:
        self.assertIn("FAIL upgrade-seed-body", self.seed(BODY_CHANGED="1"))

    def test_unseeded_upgrade_rejected(self) -> None:
        out = self.run_shell(self.seeded_state() + ': > "$TOKEN_FILE"\nphase_upgraded\n')
        self.assertIn("FAIL upgrade-prerequisites", out)
        self.assertNotIn("PASS upgrade-cli", out)

    def test_per_user_upgrade_has_no_unseeded_legacy_directory_requirement(self) -> None:
        out = self.run_shell(self.seeded_state() + 'phase_upgraded\n')
        self.assertIn("PASS upgrade-seeded-body", out)
        self.assertNotIn("FAIL ", out)
        self.assertIn("INFO upgrade-old-corpus-kept", out)

    def test_missing_legacy_precondition_rejected(self) -> None:
        for value in ("", "unknown"):
            with self.subTest(value=value):
                out = self.run_shell(self.seeded_state() + f'remember LEGACY_PRESENT "{value}"\nphase_upgraded\n')
                self.assertIn("FAIL upgrade-prerequisites", out)
                self.assertNotIn("PASS upgrade-cli", out)

    def test_changed_seeded_body_rejected(self) -> None:
        out = self.run_shell(self.seeded_state() + 'phase_upgraded\n', BODY_CHANGED="1")
        self.assertIn("FAIL upgrade-seeded-body", out)

    def test_failed_install_blocks_upgrade(self) -> None:
        out = self.run_shell(self.seeded_state("no") + 'phase_upgraded\n')
        self.assertIn("FAIL upgrade-package-identity", out)
        self.assertNotIn("PASS upgrade-cli", out)

    def test_wrong_installed_candidate_blocks_upgrade(self) -> None:
        out = self.run_shell(self.seeded_state() + 'phase_upgraded\n', ACTUAL_ID="yams 0.20.5 amd64")
        self.assertIn("FAIL upgrade-package-identity", out)
        self.assertNotIn("PASS upgrade-cli", out)

    def test_failed_install_invalidates_previous_success_record(self) -> None:
        out = self.run_shell(self.seeded_state() + 'ARG=dummy.deb\nphase_install\nphase_upgraded\n', INSTALL_RC="100")
        self.assertIn("FAIL install", out)
        self.assertIn("FAIL upgrade-package-identity", out)

    def test_real_debian_metadata_helpers_check_configured_status(self) -> None:
        functions = self.source[self.source.index("package_identity_from_file() {"):
                                self.source.index("phase_install() {")]
        commands = functions + r'''
dpkg-deb() { case "$3" in Package) printf yams;; Version) printf candidate;; Architecture) printf arm64;; esac; }
dpkg-query() { case "$*" in *Status*) printf '%s' "$PACKAGE_STATUS";; *) printf 'yams candidate arm64';; esac; }
package_identity_from_file dummy.deb
if installed_package_identity; then printf '\nVERIFIED\n'; else printf '\nREJECTED\n'; fi
'''
        out = self.run_shell(commands, PACKAGE_STATUS="install ok installed")
        self.assertIn("yams candidate arm64", out)
        self.assertIn("VERIFIED", out)
        out = self.run_shell(commands, PACKAGE_STATUS="install ok unpacked")
        self.assertIn("REJECTED", out)

    def test_real_metadata_helper_rejects_foreign_package(self) -> None:
        functions = self.source[self.source.index("package_identity_from_file() {"):
                                self.source.index("phase_install() {")]
        out = self.run_shell(functions + r'''
dpkg-deb() { case "$3" in Package) printf other;; Version) printf candidate;; Architecture) printf arm64;; esac; }
if package_identity_from_file dummy.deb; then printf ACCEPTED; else printf REJECTED; fi
''')
        self.assertIn("REJECTED", out)

    def test_install_checks_candidate_identity_even_on_success_exit(self) -> None:
        out = self.run_shell('ARG=dummy.deb\nphase_install\n', ACTUAL_ID="yams 0.20.5 amd64")
        self.assertIn("FAIL install-identity", out)
        self.assertNotIn("PASS install ", out)

    def test_legacy_manifest_rejects_changed_bytes(self) -> None:
        (self.root / "legacy").mkdir()
        data = self.root / "legacy/blob"
        data.write_bytes(b"original legacy content")
        manifest = self.root / "legacy.sha256"
        manifest.write_text(hashlib.sha256(data.read_bytes()).hexdigest() + "  " + str(data) + "\n")
        commands = self.seeded_state() + 'remember UPGRADE_MODE legacy\nremember LEGACY_PRESENT yes\n'
        out = self.run_shell(commands + 'phase_upgraded\n')
        self.assertIn("PASS upgrade-old-corpus-kept", out)
        data.write_bytes(b"changed legacy content")
        out = self.run_shell(commands + 'phase_upgraded\n')
        self.assertIn("FAIL upgrade-old-corpus-kept", out)

    def test_missing_legacy_manifest_rejected(self) -> None:
        out = self.run_shell(self.seeded_state() + 'remember UPGRADE_MODE legacy\nremember LEGACY_PRESENT yes\nphase_upgraded\n')
        self.assertIn("FAIL upgrade-old-corpus-kept", out)


class HarnessPhaseGates(unittest.TestCase):
    def test_transport_failure_is_recorded_without_child_fail_lines(self) -> None:
        text = (ROOT / "scripts/local-ci/package-validate.sh").read_text()
        function = text[text.index("run_phase() {"):text.index("note_fail() {")]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            mocks = r'''
WORK="$TEST_ROOT"
log() { :; }
lane_fmt() { printf deb; }
docker() { return "$TRANSPORT_RC"; }
note_fail() { printf '%s FAIL %s %s\n' "$1" "$2" "$3" >> "$WORK/$1.results"; }
'''
            for code in (0, 42):
                with self.subTest(code=code):
                    result_file = root / "debian.results"
                    result_file.write_text("")
                    result = shell('set -o pipefail\n' + function + mocks +
                                   '\nrun_phase debian container install candidate.deb\n'
                                   'printf "RESULT=%s\\n" "$?"\n',
                                   {"TEST_ROOT": temp, "TRANSPORT_RC": str(code)})
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(f"RESULT={code}", result.stdout)
                    if code:
                        self.assertIn("FAIL install", result_file.read_text())
                    else:
                        self.assertNotIn("FAIL ", result_file.read_text())

    def test_failed_prerequisites_do_not_reach_upgraded_phase(self) -> None:
        text = (ROOT / "scripts/local-ci/package-validate.sh").read_text()
        functions = text[text.index("validate_lane() {"):text.index('for lane in "${LANES[@]}"; do')]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "old.deb").touch()
            (root / "candidate.deb").touch()
            mocks = r'''
WORK="$TEST_ROOT"
RUN_TAG=unit
abs() { printf '%s' "$1"; }
lane_pkg() { printf '%s/candidate.deb' "$TEST_ROOT"; }
lane_upgrade_from() { printf '%s/old.deb' "$TEST_ROOT"; }
build_substrate() { printf fake-image; }
boot() { :; }
stage_files() { :; }
log() { :; }
note_fail() { printf 'HARNESS FAILURE'; }
reboot() { :; }
docker() { :; }
lane_base_image() { printf fake-base; }
lane_platform() { :; }
CONTAINERS=()
run_phase() {
    printf '%s %s\\n' "$3" "${4:-}" >> "$TEST_ROOT/phases"
    if [ "$3" = "$FAIL_PHASE" ] && [ "${4:-}" = "$FAIL_PACKAGE" ]; then return 1; fi
    return 0
}
'''
            for phase, package in [("seed", ""), ("install", str(root / "old.deb")),
                                   ("install", str(root / "candidate.deb"))]:
                with self.subTest(phase=phase, package=package):
                    (root / "phases").write_text("")
                    result = shell(functions + mocks + '\nvalidate_lane debian\n',
                                   {"TEST_ROOT": temp, "FAIL_PHASE": phase, "FAIL_PACKAGE": package})
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertNotIn("upgraded", (root / "phases").read_text())


if __name__ == "__main__":
    unittest.main()

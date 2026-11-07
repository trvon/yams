#!/usr/bin/env python3
import argparse
import importlib.util
import json
import math
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPEC = importlib.util.spec_from_file_location("mesh", Path(__file__).with_name("mesh.py"))
assert SPEC is not None and SPEC.loader is not None
mesh = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mesh)


class FakeClock:
    def __init__(self):
        self.mono = 0.0

    def monotonic(self):
        return self.mono

    def time(self):
        return 1000.0 - self.mono

    def sleep(self, seconds):
        self.mono += seconds

    @staticmethod
    def strftime(_fmt):
        return "stamp"


class MemoryMeasurementTests(unittest.TestCase):
    def test_memory_parser_supports_iec_and_si_units(self):
        cases = {
            "512kB": 512_000 / 2**20,
            "512KB": 512_000 / 2**20,
            "512KiB": 0.5,
            "1MB": 1_000_000 / 2**20,
            "1MiB": 1.0,
            "1.5GB": 1.5e9 / 2**20,
            "1.5GiB": 1536.0,
            "1024B": 1024 / 2**20,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertTrue(math.isclose(mesh.Mesh.mem_mib(text), expected, rel_tol=1e-9))

    def test_memory_parser_rejects_malformed_or_partial_values(self):
        for text in ("", "garbage", "12", "12MiB trailing", "NaNMiB", "-1MiB", "1XB"):
            with self.subTest(text=text):
                self.assertIsNone(mesh.Mesh.mem_mib(text))

    def test_slope_requires_comparable_samples(self):
        self.assertIsNone(mesh.Mesh.slope([], []))
        self.assertIsNone(mesh.Mesh.slope([0, 1], [1, 2]))
        self.assertIsNone(mesh.Mesh.slope([0, 1, 2], [1, 2]))
        self.assertAlmostEqual(mesh.Mesh.slope([0, 1, 2], [1, 3, 5]), 2.0)

    def idle_args(self, out, **overrides):
        values = {"seconds": 3, "interval": 1, "out": str(out), "threads": False,
                  "max_cpu": None}
        values.update(overrides)
        return argparse.Namespace(**values)

    def make_mesh(self, work, node_names):
        instance = mesh.Mesh(work, project="test")
        instance.state = {"nodes": [{"name": name} for name in node_names],
                          "cpus": 0, "memory": ""}
        return instance

    def test_idle_memory_slope_uses_monotonic_time_when_wall_clock_moves_backwards(self):
        with tempfile.TemporaryDirectory() as td:
            instance = self.make_mesh(Path(td) / "work", ["n1"])
            clock = FakeClock()
            samples = iter(("1MiB", "2MiB", "3MiB"))

            def fake_run(_cmd, **_kwargs):
                mem = next(samples)
                return subprocess.CompletedProcess([], 0, f"test-n1\t1.0%\t{mem} / 4MiB\n", "")

            with mock.patch.object(mesh, "time", clock), mock.patch.object(mesh, "run", fake_run):
                self.assertEqual(instance.cmd_idle(self.idle_args(Path(td) / "out")), 0)
            summary = json.loads((Path(td) / "out" / "idle.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["n1"]["container_mem_slope_mib_per_h"], 3600.0)
            header = (Path(td) / "out" / "idle.csv").read_text(encoding="utf-8").splitlines()[0]
            self.assertIn("monotonic_s", header)

    def test_invalid_cpu_sample_is_an_explicit_measurement_failure(self):
        with tempfile.TemporaryDirectory() as td:
            instance = self.make_mesh(Path(td) / "work", ["n1"])
            clock = FakeClock()

            def fake_run(_cmd, **_kwargs):
                return subprocess.CompletedProcess(
                    [], 0, "test-n1\tNaN%\t1MiB / 4MiB\n", "")

            with mock.patch.object(mesh, "time", clock), mock.patch.object(mesh, "run", fake_run):
                rc = instance.cmd_idle(self.idle_args(Path(td) / "out", seconds=1))
            self.assertEqual(rc, 6)
            summary = json.loads((Path(td) / "out" / "idle.json").read_text(encoding="utf-8"))
            self.assertIn("invalid CPU value", summary["n1"]["measurement_error"])

    def test_cpu_failure_precedes_missing_node_in_both_node_orders(self):
        for names in (["hot", "missing"], ["missing", "hot"]):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as td:
                instance = self.make_mesh(Path(td) / "work", names)
                clock = FakeClock()

                def fake_run(_cmd, **_kwargs):
                    return subprocess.CompletedProcess(
                        [], 0, "test-hot\t20.0%\t1MiB / 4MiB\n", "")

                with mock.patch.object(mesh, "time", clock), mock.patch.object(mesh, "run", fake_run):
                    rc = instance.cmd_idle(self.idle_args(
                        Path(td) / "out", seconds=1, max_cpu=10.0))
                self.assertEqual(rc, 5)

    def test_mixed_cpu_samples_preserve_threshold_failure_and_measurement_error(self):
        for names in (["hot", "missing"], ["missing", "hot"]):
            for values in (("20%", "NaN%"), ("NaN%", "20%"), ("1%", "NaN%")):
                with self.subTest(names=names, values=values), tempfile.TemporaryDirectory() as td:
                    instance = self.make_mesh(Path(td) / "work", names)
                    clock = FakeClock()
                    samples = iter(values)

                    def fake_run(_cmd, _samples=samples, **_kwargs):
                        return subprocess.CompletedProcess(
                            [], 0, f"test-hot\t{next(_samples)}\t1MiB / 4MiB\n", "")

                    with mock.patch.object(mesh, "time", clock), mock.patch.object(mesh, "run", fake_run):
                        rc = instance.cmd_idle(self.idle_args(
                            Path(td) / "out", seconds=2, max_cpu=10.0))
                    self.assertEqual(rc, 5 if "20%" in values else 6)
                    summary = json.loads((Path(td) / "out" / "idle.json").read_text(encoding="utf-8"))
                    self.assertIn("invalid CPU value", summary["hot"]["measurement_error"])
                    self.assertEqual(summary["hot"]["cpu_mean"], 20.0 if "20%" in values else 1.0)

    def test_idle_help_names_container_memory_not_rss(self):
        help_text = mesh.build_parser()._subparsers._group_actions[0].choices["idle"].description or ""
        command_help = mesh.build_parser()._subparsers._group_actions[0].choices["idle"].format_help()
        self.assertNotIn("RSS", help_text + command_help)
        self.assertIn("container memory", help_text + command_help)


if __name__ == "__main__":
    unittest.main()

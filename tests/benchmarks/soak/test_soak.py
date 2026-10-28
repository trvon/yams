#!/usr/bin/env python3
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("soak", HERE / "soak.py")
assert SPEC is not None and SPEC.loader is not None
soak = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(soak)


def task(start, cpu, vcs, *, comm="worker"):
    return {"comm": comm, "starttime": start, "utime": cpu, "stime": 0,
            "vcs": vcs, "nvcs": 0, "slices": cpu, "wchan": "futex"}


def sample(mono, phase, cpu, vcs, *, tid_start=10):
    return {"t": 1000 + mono, "mono": mono, "phase": phase, "rss_kb": 1024,
            "pss_kb": 900, "anon_kb": 800, "threads": 1, "fds": 2,
            "proc_vcs": vcs, "proc_nvcs": 0, "utime": cpu, "stime": 0,
            "process_starttime": 1, "files": {"__total__": 0},
            "tasks": {"7": task(tid_start, cpu, vcs)}, "status_calls": 0}


def run_args(work, **overrides):
    values = {
        "work": str(work), "build_dir": None, "yams": "yams", "daemon": "yams-daemon",
        "env": [], "fresh": False, "config_append": None, "no_plugins": True,
        "log_level": "info", "ready_timeout": 1, "corpus": None, "docs": 2,
        "add_batch": 100, "settle_max": 1, "settle_poll": 1, "settle_cpu": 5.0,
        "phases": "idle:0", "interval": 1.0, "status_every": 0.0, "load_period": 0.0,
        "warmup": 0, "stacks_per_phase": 0, "stack_depth": 1, "keep_daemon": True,
        "stacks_at_end": False, "daemon_prefix": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def fake_run_harness(work, **overrides):
    harness = soak.Soak(run_args(work, **overrides))
    stopped = []
    harness.init_config = lambda: None

    def start():
        harness.daemon = object()
        harness.pid = os.getpid()

    harness.start_daemon = start
    harness.stop_daemon = lambda: stopped.append(True)
    harness.cli = lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", "")
    harness.sampler = lambda: harness.stop.wait(0.1)
    return harness, stopped


class SoakSummaryTests(unittest.TestCase):
    def write_run(self, out, meta, samples, ops=()):
        out.mkdir(parents=True)
        (out / "meta.json").write_text(json.dumps(meta))
        (out / "samples.jsonl").write_text("".join(json.dumps(x) + "\n" for x in samples))
        (out / "ops.jsonl").write_text("".join(json.dumps(x) + "\n" for x in ops))

    def test_legacy_artifact_is_unknown_not_valid(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            self.write_run(out, {"phases": [["idle", 10]]},
                           [sample(0, "0-idle", 0, 0), sample(10, "0-idle", 10, 2)])
            summary = soak.summarize(out)
            self.assertEqual(summary["validation"]["status"], "unknown")
            self.assertIsNone(summary["validation"]["valid"])

    def test_completed_run_requires_settle_and_planned_phase_coverage(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            meta = {"schema_version": 2, "phases": [["idle", 10]], "interval_s": 5,
                    "settle": {"required": True, "completed": False},
                    "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
            self.write_run(out, meta,
                           [sample(0, "0-idle", 0, 0), sample(5, "0-idle", 5, 1),
                            sample(10, "0-idle", 10, 2)])
            summary = soak.summarize(out)
            self.assertFalse(summary["validation"]["valid"])
            self.assertIn("seed did not settle", " ".join(summary["validation"]["errors"]))

    def test_operation_failure_invalidates_run(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            meta = {"schema_version": 2, "phases": [["load", 10]], "interval_s": 5,
                    "settle": {"required": False, "completed": None},
                    "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
            rows = [sample(0, "0-load", 0, 0), sample(5, "0-load", 5, 1),
                    sample(10, "0-load", 10, 2)]
            self.write_run(out, meta, rows, [{"phase": "0-load", "op": "search", "ms": 1, "rc": 1}])
            summary = soak.summarize(out)
            self.assertFalse(summary["validation"]["valid"])
            self.assertIn("operation failures: 1", summary["validation"]["errors"])

    def test_tid_reuse_preserves_valid_intervals_for_both_lifetimes(self):
        rows = [sample(0, "0-idle", 0, 0, tid_start=10),
                sample(5, "0-idle", 5, 2, tid_start=10),
                sample(10, "0-idle", 0, 0, tid_start=20),
                sample(15, "0-idle", 7, 3, tid_start=20)]
        acc, _wall, rejected = soak.thread_deltas(rows)
        self.assertEqual(len(acc), 2)
        self.assertEqual(sum(d["cpu"] for d in acc.values()), 12)
        self.assertEqual(sum(d["vcs"] for d in acc.values()), 5)
        self.assertEqual(rejected, 1)

    def test_reused_tid_labels_are_scoped_to_one_lifetime(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            meta = {"schema_version": 2, "phases": [["idle", 15]], "interval_s": 5,
                    "settle": {"required": False, "completed": None},
                    "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
            rows = [sample(0, "0-idle", 0, 0, tid_start=10),
                    sample(5, "0-idle", 5, 2, tid_start=10),
                    sample(10, "0-idle", 0, 0, tid_start=20),
                    sample(15, "0-idle", 7, 3, tid_start=20)]
            self.write_run(out, meta, rows)
            later = soak.lifetime_key(1, "7", 20)
            (out / "stack_labels.json").write_text(json.dumps({later: "later lifetime"}))
            summary = soak.summarize(out)
            labels = summary["phases"]["0-idle"]["by_label"]
            self.assertEqual(labels["later lifetime"]["observed_lifetimes"], 1)
            self.assertEqual(labels["(unknown)"]["observed_lifetimes"], 1)

    def test_corrupt_jsonl_invalidates_otherwise_valid_run(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            meta = {"schema_version": 2, "phases": [["idle", 10]], "interval_s": 5,
                    "settle": {"required": False, "completed": None},
                    "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
            rows = [sample(0, "0-idle", 0, 0), sample(5, "0-idle", 5, 1),
                    sample(10, "0-idle", 10, 2)]
            self.write_run(out, meta, rows)
            with (out / "samples.jsonl").open("a") as f:
                f.write("{not-json}\n")
            summary = soak.summarize(out)
            self.assertFalse(summary["validation"]["valid"])
            self.assertIn("samples.jsonl line 4", " ".join(summary["validation"]["errors"]))

    def test_invalid_numeric_sample_invalidates_run_without_crashing_summary(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out"
            meta = {"schema_version": 2, "phases": [["idle", 10]], "interval_s": 5,
                    "settle": {"required": False, "completed": None},
                    "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
            rows = [sample(0, "0-idle", 0, 0), sample(5, "0-idle", 5, 1),
                    sample(10, "0-idle", 10, 2)]
            rows[-1]["mono"] = "invalid"
            self.write_run(out, meta, rows)
            summary = soak.summarize(out)
            self.assertFalse(summary["validation"]["valid"])
            self.assertIn("invalid numeric mono", " ".join(summary["validation"]["errors"]))

    def test_summarize_cli_exit_status_matches_validity(self):
        for status, expected_rc in (("valid", 0), ("invalid", 3), ("unknown", 3)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as td:
                work = Path(td)
                meta = {"schema_version": 2, "phases": [["idle", 10]], "interval_s": 5,
                        "settle": {"required": False, "completed": None},
                        "run": {"completed": True, "sampler_error": None, "workload_errors": []}}
                if status == "unknown":
                    meta.pop("schema_version")
                rows = [sample(0, "0-idle", 0, 0), sample(5, "0-idle", 5, 1),
                        sample(10, "0-idle", 10, 2)]
                self.write_run(work / "out", meta, rows)
                if status == "invalid":
                    with (work / "out" / "samples.jsonl").open("a") as stream:
                        stream.write("{invalid-json}\n")
                result = subprocess.run([sys.executable, str(HERE / "soak.py"),
                                         "summarize", "--work", str(work)],
                                        capture_output=True, text=True, timeout=10)
                summary = json.loads((work / "out" / "summary.json").read_text())
                self.assertEqual(summary["validation"]["status"], status)
                self.assertEqual(result.returncode, expected_rc, result.stderr)

    def test_redacts_sensitive_config_values(self):
        text = 'api_key = "secret"\nmode = "safe"\npassword="p" # comment\n'
        redacted = soak.redact_config(text)
        self.assertNotIn("secret", redacted)
        self.assertNotIn('password="p"', redacted)
        self.assertIn('mode = "safe"', redacted)


class DaemonPlacementTests(unittest.TestCase):
    def test_daemon_command_prepends_shell_split_prefix(self):
        base = soak.daemon_command("yams-daemon", "c.toml", "s.sock", "data", "p.pid", "d.log",
                                   "info", True, None)
        self.assertEqual(base[0], "yams-daemon")
        self.assertEqual(base[-1], "--no-plugins")
        wrapped = soak.daemon_command("yams-daemon", "c.toml", "s.sock", "data", "p.pid", "d.log",
                                      "info", False,
                                      "systemd-run --user --scope -p 'CPUQuota=400%' taskset -c 2-5")
        self.assertEqual(wrapped[:8], ["systemd-run", "--user", "--scope", "-p", "CPUQuota=400%",
                                       "taskset", "-c", "2-5"])
        self.assertEqual(wrapped[8:], base[:-1])

    def test_placement_reads_affinity_and_cgroup_limits(self):
        files = {"/sys/fs/cgroup/user.slice/x.scope/cpu.max": "400000 100000\n",
                 "/sys/fs/cgroup/user.slice/x.scope/memory.max": "8589934592\n"}
        got = soak.placement_from("Name:\tyams-daemon\nCpus_allowed_list:\t2-5,26-29\n",
                                  "0::/user.slice/x.scope\n", lambda path: files.get(path, ""))
        self.assertEqual(got, {"cpus_allowed_list": "2-5,26-29", "cgroup": "/user.slice/x.scope",
                               "cpu_max": "400000 100000", "memory_max": "8589934592"})
        # Missing cgroup files (cgroup v1, or no delegation) are recorded as unknown, not errors.
        bare = soak.placement_from("Cpus_allowed_list:\t0-47\n", "0::/\n", lambda path: "")
        self.assertEqual(bare["cpu_max"], None)
        self.assertEqual(bare["cpus_allowed_list"], "0-47")

    def test_stacks_at_end_snapshots_once_after_timed_phases(self):
        with tempfile.TemporaryDirectory() as td:
            harness, _stopped = fake_run_harness(Path(td) / "work", stacks_at_end=True,
                                                 phases="idle:0")
            harness.seed = lambda: None
            snapshots = []
            harness.snapshot_stacks = lambda tag, force=False: snapshots.append((tag, force))
            with mock.patch.object(soak, "placement", return_value={"cpus_allowed_list": "0"}):
                harness.run()
            self.assertEqual(snapshots, [("end", True)])
            meta = json.loads((harness.out / "meta.json").read_text())
            self.assertTrue(meta["observer_probes"]["gdb_snapshot_at_end"])
            self.assertEqual(meta["placement"], {"cpus_allowed_list": "0"})


class SoakCommandPathTests(unittest.TestCase):
    def setUp(self):
        # The real run command is Linux-only; these mocked paths also run in Windows CI.
        uname = mock.patch.object(soak.os, "uname", create=True,
                                  return_value=SimpleNamespace(nodename="test-host"))
        uname.start()
        self.addCleanup(uname.stop)

    def test_settle_timeout_aborts_before_warmup_and_timed_phases(self):
        with tempfile.TemporaryDirectory() as td:
            corpus = Path(td) / "corpus.jsonl"
            corpus.write_text(json.dumps({"_id": "one", "text": "body"}) + "\n")
            harness, stopped = fake_run_harness(Path(td) / "work", corpus=str(corpus),
                                                docs=1, settle_max=0, warmup=1)
            with mock.patch.object(harness, "set_phase", wraps=harness.set_phase) as phases, \
                    mock.patch.object(harness.stop, "wait", return_value=False):
                rc = harness.run()
            self.assertEqual(rc, 3)
            self.assertEqual([call.args[0] for call in phases.call_args_list], ["seed"])
            self.assertEqual(stopped, [True])
            meta = json.loads((harness.out / "meta.json").read_text())
            self.assertIs(meta["settle"]["completed"], False)
            self.assertFalse(meta["run"]["completed"])
            self.assertIn("settle", " ".join(meta["run"]["workload_errors"]))

    def test_seed_add_failure_is_recorded_and_aborts_before_timed_phase(self):
        with tempfile.TemporaryDirectory() as td:
            corpus = Path(td) / "corpus.jsonl"
            corpus.write_text(json.dumps({"_id": "one", "text": "body"}) + "\n")
            harness, stopped = fake_run_harness(Path(td) / "work", corpus=str(corpus), docs=1)
            phases = []
            original_set_phase = harness.set_phase
            harness.set_phase = lambda phase: (phases.append(phase), original_set_phase(phase))[1]
            harness.cli = lambda *args, **kwargs: subprocess.CompletedProcess(args, 9, "", "add failed")
            harness.settle = lambda: self.fail("failed seed must not settle")

            self.assertEqual(harness.run(), 3)
            self.assertNotIn("0-idle", phases)
            meta = json.loads((harness.out / "meta.json").read_text())
            self.assertEqual(meta["seed"]["requested_docs"], 1)
            self.assertEqual(meta["seed"]["successful_docs"], 0)
            self.assertEqual(len(meta["seed"]["failed_batches"]), 1)
            self.assertEqual(stopped, [True])

    def test_partial_requested_corpus_is_recorded_and_aborts_before_timed_phase(self):
        with tempfile.TemporaryDirectory() as td:
            corpus = Path(td) / "corpus.jsonl"
            corpus.write_text(json.dumps({"_id": "one", "text": "body"}) + "\n")
            harness, stopped = fake_run_harness(Path(td) / "work", corpus=str(corpus), docs=2)
            phases = []
            original_set_phase = harness.set_phase
            harness.set_phase = lambda phase: (phases.append(phase), original_set_phase(phase))[1]
            harness.settle = lambda: self.fail("partial seed must not settle")

            self.assertEqual(harness.run(), 3)
            self.assertNotIn("0-idle", phases)
            meta = json.loads((harness.out / "meta.json").read_text())
            self.assertEqual(meta["seed"]["requested_docs"], 2)
            self.assertEqual(meta["seed"]["loaded_docs"], 1)
            self.assertEqual(meta["seed"]["successful_docs"], 0)
            self.assertEqual(stopped, [True])

    def test_empty_requested_corpus_is_recorded_and_aborts_before_timed_phase(self):
        with tempfile.TemporaryDirectory() as td:
            corpus = Path(td) / "corpus.jsonl"
            corpus.write_text("")
            harness, stopped = fake_run_harness(Path(td) / "work", corpus=str(corpus), docs=2)
            phases = []
            original_set_phase = harness.set_phase
            harness.set_phase = lambda phase: (phases.append(phase), original_set_phase(phase))[1]

            self.assertEqual(harness.run(), 3)
            self.assertNotIn("0-idle", phases)
            meta = json.loads((harness.out / "meta.json").read_text())
            self.assertEqual(meta["seed"]["requested_docs"], 2)
            self.assertEqual(meta["seed"]["loaded_docs"], 0)
            self.assertEqual(meta["seed"]["successful_docs"], 0)
            self.assertEqual(stopped, [True])

    def test_invalid_operation_stops_owned_daemon_even_with_keep_daemon(self):
        with tempfile.TemporaryDirectory() as td:
            harness, stopped = fake_run_harness(Path(td) / "work")

            def seed_with_failed_operation():
                for row in (sample(0, "0-idle", 0, 0), sample(0, "0-idle", 0, 0)):
                    harness.samples_f.write(json.dumps(row) + "\n")
                harness.samples_f.flush()
                harness.ops_f.write(json.dumps(
                    {"phase": "seed", "op": "add", "ms": 1, "rc": 1}) + "\n")
                harness.ops_f.flush()

            harness.seed = seed_with_failed_operation
            self.assertEqual(harness.run(), 3)
            self.assertEqual(stopped, [True])

    def test_startup_sampler_workload_and_summary_failures_stop_owned_daemon(self):
        cases = ("startup", "sampler", "workload", "summary")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as td:
                phases = "load:0" if case == "workload" else "idle:0"
                harness, stopped = fake_run_harness(Path(td) / "work", phases=phases)
                if case == "startup":
                    def fail_start(current=harness):
                        current.daemon = object()
                        raise RuntimeError("startup failed")
                    harness.start_daemon = fail_start
                elif case == "sampler":
                    harness.sampler = lambda: (_ for _ in ()).throw(RuntimeError("sampler failed"))
                elif case == "workload":
                    harness.run_load = lambda _seconds: (_ for _ in ()).throw(
                        RuntimeError("workload failed"))

                patch = (mock.patch.object(soak, "summarize", side_effect=RuntimeError("summary failed"))
                         if case == "summary" else mock.patch.object(soak, "summarize", wraps=soak.summarize))
                with patch:
                    if case == "summary":
                        with self.assertRaisesRegex(RuntimeError, "summary failed"):
                            harness.run()
                    else:
                        self.assertEqual(harness.run(), 3)
                self.assertEqual(stopped, [True])


if __name__ == "__main__":
    unittest.main()

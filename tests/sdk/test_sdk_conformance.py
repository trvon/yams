#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile

def run(args):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def main():
    repo = os.environ.get("REPO_ROOT", ".")
    runner = os.path.join(repo, "external", "yams-sdk", "python", "yams_sdk", "conformance", "runner.py")
    if not os.path.exists(runner):
        print("runner not found", runner)
        return 77

    with tempfile.TemporaryDirectory() as td:
        man_ok = {
            "name": "dr_python",
            "version": "0.0.1",
            "interfaces": ["dr_provider_v1"],
        }
        mp = os.path.join(td, "plugin.manifest.json")
        with open(mp, "w", encoding="utf-8") as f:
            json.dump(man_ok, f)

        # Positive
        r1 = run([sys.executable, runner, "--manifest", mp, "--require", "dr_provider_v1.is_replication_ready"])
        assert r1.returncode == 0, f"unexpected rc {r1.returncode}: {r1.stdout} {r1.stderr}"
        assert "OK" in r1.stdout

        # Negative: missing required interface
        r2 = run([sys.executable, runner, "--manifest", mp, "--require", "object_storage_v1.head_object"])
        assert r2.returncode != 0, "expected non-zero for missing interface"
        assert "required interface" in r2.stderr

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

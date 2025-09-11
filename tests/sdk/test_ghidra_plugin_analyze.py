#!/usr/bin/env python3
import json
import os
import subprocess
import sys

def have_ghidra_env() -> bool:
    return bool(os.environ.get("GHIDRA_INSTALL_DIR"))

def main():
    # Require GHIDRA_INSTALL_DIR and TEST_GHIDRA_BIN for this ad-hoc test
    plugin = os.path.join(os.environ.get("REPO_ROOT", "."), "plugins", "yams-ghidra-plugin", "plugin.py")
    if not os.path.exists(plugin):
        print("plugin not found", plugin)
        return 77
    test_bin = os.environ.get("TEST_GHIDRA_BIN")
    if not have_ghidra_env() or not test_bin or not os.path.exists(test_bin):
        print("Skipping: GHIDRA_INSTALL_DIR or TEST_GHIDRA_BIN not set")
        return 77

    env = os.environ.copy()
    # Ensure SDK is importable
    repo = os.environ.get("REPO_ROOT", ".")
    sdk = os.path.join(repo, "external", "yams-sdk", "python")
    env["PYTHONPATH"] = sdk + os.pathsep + env.get("PYTHONPATH", "")

    p = subprocess.Popen([sys.executable, plugin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
    try:
        # handshake
        p.stdin.write(json.dumps({"id": 1, "method": "handshake.manifest"}) + "\n"); p.stdin.flush()
        resp = json.loads(p.stdout.readline())
        assert resp.get("result", {}).get("name") == "yams_ghidra"

        # init
        init = {"id": 2, "method": "plugin.init", "params": {"project_dir": os.path.join(repo, ".crush", "ghidra-test")}}
        p.stdin.write(json.dumps(init) + "\n"); p.stdin.flush()
        _ = json.loads(p.stdout.readline())

        # analyze
        req = {"id": 3, "method": "ghidra.analyze", "params": {"source": {"type": "path", "path": test_bin}, "opts": {"max_functions": 5}}}
        p.stdin.write(json.dumps(req) + "\n"); p.stdin.flush()
        ar = json.loads(p.stdout.readline())
        res = ar.get("result", {})
        assert "arch" in res and "functions" in res
    finally:
        try:
            p.terminate()
        except Exception:
            pass
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import json
import os
import subprocess
import sys

def main():
    repo = os.environ.get("REPO_ROOT", ".")
    plugin = os.path.join(repo, "external", "yams-sdk", "python", "templates", "external-dr", "plugin.py")
    if not os.path.exists(plugin):
        print("plugin not found", plugin)
        return 77

    # Spawn plugin
    p = subprocess.Popen([sys.executable, plugin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

    try:
        # Handshake
        req = {"id": 1, "method": "handshake.manifest"}
        p.stdin.write(json.dumps(req) + "\n")
        p.stdin.flush()
        line = p.stdout.readline().strip()
        assert line, "no response from plugin"
        resp = json.loads(line)
        assert "result" in resp, f"no result: {resp}"
        assert resp["result"].get("name") == "dr_python"

        # Call method
        req2 = {"id": 2, "method": "dr_provider_v1.is_replication_ready", "params": {"manifest_id": "vec0"}}
        p.stdin.write(json.dumps(req2) + "\n")
        p.stdin.flush()
        line2 = p.stdout.readline().strip()
        assert line2, "no method response"
        resp2 = json.loads(line2)
        assert resp2.get("result", {}).get("ok") is True
    finally:
        try:
            p.terminate()
        except Exception:
            pass

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

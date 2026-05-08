#!/usr/bin/env python3
import json
import sys


def handle_request(req):
    method = req.get("method", "")
    if method == "handshake.manifest":
        return {
            "name": "dispatcher_external_plugin",
            "version": "1.0.0",
            "interfaces": ["content_extractor_v1"]
        }
    if method == "plugin.init":
        return {"status": "initialized"}
    if method == "plugin.health":
        return {"status": "ok"}
    if method == "plugin.shutdown":
        return {"status": "ok"}
    raise ValueError(f"Method not found: {method}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req_id = None
    try:
        req = json.loads(line)
        req_id = req.get("id")
        response = {"jsonrpc": "2.0", "id": req_id, "result": handle_request(req)}
    except Exception as exc:
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32603, "message": str(exc)}
        }
    print(json.dumps(response), flush=True)

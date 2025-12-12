#!/usr/bin/env python3
"""Mock plugin that simulates stale JSON-RPC responses for testing."""

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        req_id = None
        try:
            req = json.loads(line)
            req_id = req.get("id")
            method = req.get("method", "")

            if method == "test.with_stale":
                # Send a stale response first (wrong ID), then the correct one
                stale = {"jsonrpc": "2.0", "id": req_id - 1, "result": {"status": "stale"}}
                print(json.dumps(stale), flush=True)
                response = {"jsonrpc": "2.0", "id": req_id, "result": {"status": "ok"}}

            elif method == "test.with_multiple_stale":
                # Send 3 stale responses then the correct one
                for i in range(3):
                    stale = {"jsonrpc": "2.0", "id": req_id - 3 + i, "result": {"stale": i}}
                    print(json.dumps(stale), flush=True)
                response = {"jsonrpc": "2.0", "id": req_id, "result": {"count": 3}}

            else:
                response = {"jsonrpc": "2.0", "id": req_id, "result": {"status": "ok"}}

        except Exception as e:
            response = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32603, "message": str(e)}}

        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()

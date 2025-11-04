#!/usr/bin/env python3
"""Mock plugin for testing ExternalPluginExtractor."""

import json
import sys


def handle_request(req):
    """Handle JSON-RPC request."""
    method = req.get("method", "")
    params = req.get("params", {})

    if method == "handshake.manifest":
        return {
            "name": "mock_plugin",
            "version": "1.0.0",
            "interfaces": ["content_extractor_v1"],
            "capabilities": {
                "content_extraction": {
                    "formats": ["text/plain"],
                    "extensions": [".txt"]
                }
            }
        }

    elif method == "plugin.init":
        return {"status": "ok"}

    elif method == "plugin.health":
        return {"status": "ok"}

    elif method == "plugin.shutdown":
        return {"status": "ok"}

    elif method == "extractor.supports":
        mime_type = params.get("mime_type", "")
        extension = params.get("extension", "")
        supported = mime_type == "text/plain" or extension == ".txt"
        return {"supported": supported}

    elif method == "extractor.extract":
        # Mock extraction - just return dummy text
        return {
            "text": "Mock extracted text from file",
            "metadata": {
                "extractor": "mock_plugin",
                "version": "1.0.0"
            },
            "error": None
        }

    else:
        # Method not found
        raise ValueError(f"Method not found: {method}")


def main():
    """Main loop: read NDJSON requests, send NDJSON responses."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        req_id = None
        try:
            req = json.loads(line)
            req_id = req.get("id")
            result = handle_request(req)
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": result
            }
        except Exception as e:
            response = {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }

        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()

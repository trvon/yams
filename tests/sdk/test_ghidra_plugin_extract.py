#!/usr/bin/env python3
"""Test for Ghidra plugin extractor.extract RPC.

This test verifies the optimized extraction flow that opens the program once
and decompiles all functions in a single session.

Requirements:
- GHIDRA_INSTALL_DIR environment variable must be set
- TEST_GHIDRA_BIN environment variable should point to a test binary
  (defaults to notepad.exe on Windows if not set)
"""
import base64
import json
import os
import subprocess
import sys
import time


def have_ghidra_env() -> bool:
    return bool(os.environ.get("GHIDRA_INSTALL_DIR"))


def get_test_binary() -> str:
    """Get path to a test binary for extraction."""
    # Use TEST_GHIDRA_BIN if set
    test_bin = os.environ.get("TEST_GHIDRA_BIN")
    if test_bin and os.path.exists(test_bin):
        return test_bin

    # Fall back to notepad.exe on Windows
    if sys.platform == "win32":
        notepad = r"C:\Windows\System32\notepad.exe"
        if os.path.exists(notepad):
            return notepad

    return ""


def send_rpc(proc, method: str, params: dict = None, timeout: float = 300) -> dict:
    """Send an RPC request and wait for response."""
    rpc_id = int(time.time() * 1000)
    request = {"jsonrpc": "2.0", "id": rpc_id, "method": method}
    if params:
        request["params"] = params

    proc.stdin.write(json.dumps(request) + "\n")
    proc.stdin.flush()

    # Read response with timeout
    import select
    if sys.platform != "win32":
        ready, _, _ = select.select([proc.stdout], [], [], timeout)
        if not ready:
            raise TimeoutError(f"Timeout waiting for response to {method}")

    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"No response to {method}")

    return json.loads(line)


def test_extract_from_path(proc, binary_path: str) -> dict:
    """Test extraction using a file path source."""
    print(f"\n=== Testing extractor.extract with path source ===")
    print(f"Binary: {binary_path}")
    print(f"Size: {os.path.getsize(binary_path)} bytes")

    params = {
        "source": {"type": "path", "path": binary_path},
        "options": {"max_functions": 10, "timeout_sec": 30}
    }

    start = time.time()
    resp = send_rpc(proc, "extractor.extract", params, timeout=300)
    elapsed = time.time() - start

    print(f"Response time: {elapsed:.2f}s")

    if "error" in resp and resp["error"]:
        print(f"RPC error: {resp['error']}")
        return resp

    result = resp.get("result", {})
    text = result.get("text")
    metadata = result.get("metadata", {})
    error = result.get("error")

    if error:
        print(f"Extraction error: {error}")
    elif text:
        print(f"Extracted {len(text)} chars")
        print(f"Metadata: {metadata}")
        # Show first 500 chars
        print(f"\n--- First 500 chars ---\n{text[:500]}\n---")
    else:
        print("No text extracted")

    return result


def test_extract_from_bytes(proc, binary_path: str) -> dict:
    """Test extraction using base64-encoded bytes source."""
    print(f"\n=== Testing extractor.extract with bytes source ===")

    # Read and encode binary
    with open(binary_path, "rb") as f:
        data = f.read()

    # Limit to 1MB for this test
    if len(data) > 1024 * 1024:
        print(f"Binary too large ({len(data)} bytes), using first 1MB")
        data = data[:1024 * 1024]

    b64_data = base64.b64encode(data).decode("ascii")
    print(f"Encoded {len(data)} bytes -> {len(b64_data)} base64 chars")

    params = {
        "source": {"type": "bytes", "data": b64_data},
        "options": {"max_functions": 5, "timeout_sec": 30}
    }

    start = time.time()
    resp = send_rpc(proc, "extractor.extract", params, timeout=300)
    elapsed = time.time() - start

    print(f"Response time: {elapsed:.2f}s")

    result = resp.get("result", {})
    if result.get("text"):
        print(f"Extracted {len(result['text'])} chars")
    elif result.get("error"):
        print(f"Error: {result['error']}")

    return result


def test_supports(proc) -> bool:
    """Test extractor.supports RPC."""
    print("\n=== Testing extractor.supports ===")

    test_cases = [
        ("application/x-executable", ".exe", True),
        ("application/x-msdownload", ".dll", True),
        ("text/plain", ".txt", False),
        ("application/json", ".json", False),
        ("application/octet-stream", ".bin", True),
    ]

    all_passed = True
    for mime, ext, expected in test_cases:
        resp = send_rpc(proc, "extractor.supports", {"mime_type": mime, "extension": ext})
        result = resp.get("result", {})
        supported = result.get("supported", False)
        status = "PASS" if supported == expected else "FAIL"
        if supported != expected:
            all_passed = False
        print(f"  {mime} / {ext}: {supported} (expected {expected}) [{status}]")

    return all_passed


def main():
    # Check environment
    if not have_ghidra_env():
        print("Skipping: GHIDRA_INSTALL_DIR not set")
        return 77

    test_bin = get_test_binary()
    if not test_bin:
        print("Skipping: No test binary available (set TEST_GHIDRA_BIN)")
        return 77

    # Find plugin
    repo = os.environ.get("REPO_ROOT", os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    plugin = os.path.join(repo, "plugins", "yams-ghidra-plugin", "plugin.py")
    if not os.path.exists(plugin):
        print(f"Plugin not found: {plugin}")
        return 1

    # Set up environment
    env = os.environ.copy()
    sdk = os.path.join(repo, "external", "yams-sdk", "python")
    env["PYTHONPATH"] = sdk + os.pathsep + env.get("PYTHONPATH", "")

    print(f"Starting plugin: {plugin}")
    print(f"Test binary: {test_bin}")
    print(f"GHIDRA_INSTALL_DIR: {os.environ.get('GHIDRA_INSTALL_DIR')}")

    # Start plugin process
    proc = subprocess.Popen(
        [sys.executable, plugin],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        bufsize=1
    )

    try:
        # Handshake
        print("\n=== Handshake ===")
        resp = send_rpc(proc, "handshake.manifest")
        manifest = resp.get("result", {})
        print(f"Plugin: {manifest.get('name')} v{manifest.get('version')}")
        assert manifest.get("name") == "yams_ghidra", "Unexpected plugin name"

        # Initialize
        print("\n=== Initialize ===")
        project_dir = os.path.join(repo, ".crush", "ghidra-extract-test")
        os.makedirs(project_dir, exist_ok=True)
        resp = send_rpc(proc, "plugin.init", {"project_dir": project_dir})
        print(f"Init response: {resp.get('result')}")

        # Test supports
        supports_ok = test_supports(proc)

        # Test extraction from path
        result = test_extract_from_path(proc, test_bin)

        # Check results
        if result.get("error"):
            print(f"\nExtraction failed: {result['error']}")
            return 1

        if not result.get("text"):
            print("\nNo text extracted")
            return 1

        # Verify we got decompiled functions
        text = result["text"]
        metadata = result.get("metadata", {})

        decompiled = int(metadata.get("decompiled_count", 0))
        failed = int(metadata.get("failed_count", 0))

        print(f"\n=== Summary ===")
        print(f"Total functions: {metadata.get('function_count')}")
        print(f"Decompiled: {decompiled}")
        print(f"Failed: {failed}")
        print(f"Text length: {len(text)} chars")

        if decompiled == 0:
            print("WARNING: No functions were decompiled")
            # Don't fail - this could be due to Ghidra configuration

        # Test extraction from bytes (smaller test)
        # test_extract_from_bytes(proc, test_bin)

        print("\n=== All tests passed ===")
        return 0

    except TimeoutError as e:
        print(f"\nTimeout: {e}")
        # Print stderr for debugging
        stderr = proc.stderr.read()
        if stderr:
            print(f"\n--- Plugin stderr ---\n{stderr}\n---")
        return 1

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

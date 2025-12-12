
import sys
import os
import time
import subprocess
import json
import socket
import pytest

# Add tests/sdk to path to use yams_client
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'sdk'))

def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def run_test():
    # Explicit paths found in build/release
    daemon_bin = os.path.abspath(r"build\release\src\daemon\yams-daemon.exe")
    cli_bin = os.path.abspath(r"build\release\tools\yams-cli\yams-cli.exe")
    
    if not os.path.exists(daemon_bin):
        print(f"Error: Daemon not found at {daemon_bin}")
        # Try finding via env or relative
        daemon_bin = os.getenv("YAMS_DAEMON_BIN")
        if not daemon_bin or not os.path.exists(daemon_bin):
             print("Cannot find daemon binary.")
             sys.exit(1)

    if not os.path.exists(cli_bin):
        print(f"Error: CLI not found at {cli_bin}")
        cli_bin = daemon_bin.replace("yamsd", "yams").replace("daemon", "cli")
        # Just use what we have or try to locate
        
    print(f"Daemon: {daemon_bin}")
    print(f"CLI: {cli_bin}")

    #Create a dummy plugin
    plugin_dir = os.path.abspath("debug_plugin_dir")
    os.makedirs(plugin_dir, exist_ok=True)
    
    plugin_py = os.path.join(plugin_dir, "debug_plugin.py")
    with open(plugin_py, "w") as f:
        f.write('''
import sys
import json
import time

def log(msg):
    sys.stderr.write(f"[debug_plugin] {msg}\\n")
    sys.stderr.flush()

log("Starting up")

while True:
    try:
        line = sys.stdin.readline()
        if not line:
            break
        req = json.loads(line)
        log(f"Received: {req.get('method')}")
        
        if req.get("method") == "handshake.manifest":
            resp = {
                "jsonrpc": "2.0",
                "id": req.get("id"),
                "result": {
                    "name": "debug-plugin",
                    "version": "0.0.1",
                    "capabilities": {"content_extraction": {}}
                }
            }
            sys.stdout.write(json.dumps(resp) + "\\n")
            sys.stdout.flush()
        elif req.get("method") == "plugin.init":
             resp = {
                "jsonrpc": "2.0",
                "id": req.get("id"),
                "result": {"status": "ok"}
            }
             sys.stdout.write(json.dumps(resp) + "\\n")
             sys.stdout.flush()
        elif req.get("method") == "plugin.shutdown":
            log("Shutting down")
            sys.exit(0)
    except Exception as e:
        log(f"Error: {e}")
        break
''')

    port = get_free_port()
    # Start daemon
    cmd = [daemon_bin, "serve", "--port", str(port), "--no-security"] 
    print(f"Starting daemon on port {port}...")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Wait for startup
    time.sleep(2)
    
    try:
        print(f"Using CLI: {cli_bin}")
        
        # Trust
        subprocess.run([cli_bin, "plugin", "trust", "add", plugin_py], check=False)
        
        # Load
        print("Sending load command...")
        res = subprocess.run([cli_bin, "plugin", "load", plugin_py], capture_output=True, text=True)
        print("Load output:", res.stdout)
        print("Load stderr:", res.stderr)
        
        # Give it a moment to spawn
        time.sleep(3)
        
        # Stop daemon
        proc.terminate()
        stdout, stderr = proc.communicate()
        
        print("Daemon Log Output:")
        print(stderr) 
        
        if "Spawning process" in stderr:
            count = stderr.count("Spawning process")
            print(f"Found {count} spawn events.")
            if count >= 2:
                print("SUCCESS: 2 spawns detected")
            else:
                print("FAILURE: Less than 2 spawns detected")
        else:
             print("FAILURE: No spawn events detected")
        
    finally:
        if proc.poll() is None:
            proc.kill()

            
if __name__ == "__main__":
    run_test()

#!/usr/bin/env python3
import os
import sys
import subprocess
import time
import json
import signal
import tempfile
import psutil

# Check if yamsd is in path or build dir
DAEMON_BIN = os.environ.get("YAMS_DAEMON_BIN")
if not DAEMON_BIN:
    # Try finding it in build dir
    possible_paths = [
        "build/release/src/daemon/yamsd",
        "build/src/daemon/yamsd",
        "build/release/yamsd",
        "build/yamsd"
    ]
    for p in possible_paths:
        if os.path.exists(p):
            DAEMON_BIN = os.path.abspath(p)
            break

def get_child_pids(parent_pid):
    try:
        parent = psutil.Process(parent_pid)
        return [c.pid for c in parent.children(recursive=True)]
    except psutil.NoSuchProcess:
        return []

def main():
    if not DAEMON_BIN or not os.path.exists(DAEMON_BIN):
        print(f"yamsd binary not found. Set YAMS_DAEMON_BIN or build project.")
        print(f"Checked paths: {DAEMON_BIN}")
        return 77 # Skip

    print(f"Using daemon: {DAEMON_BIN}")

    # Create a dummy plugin
    plugin_script = """
import sys
import json
import time

def main():
    # Loop forever
    while True:
        line = sys.stdin.readline()
        if not line: break
        try:
            req = json.loads(line)
            if req.get('method') == 'handshake.manifest':
                resp = {"jsonrpc": "2.0", "id": req.get("id"), "result": {"name": "cleanup_live", "version": "1.0", "interfaces": []}}
                print(json.dumps(resp))
                sys.stdout.flush()
        except:
            pass
        time.sleep(0.1)

if __name__ == "__main__":
    main()
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(plugin_script)
        plugin_path = f.name
        # Make executable
        os.chmod(plugin_path, 0o755)

    try:
        # Start Daemon
        # We need to start it with a config that allows plugins
        # Create temp config
        # ... simplifying for now, assuming defaults work or we can pass args
        
        daemon_proc = subprocess.Popen([DAEMON_BIN, "--config", "dev_config.toml"], 
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        print(f"Daemon started with PID {daemon_proc.pid}")
        time.sleep(2) # Wait for start

        if daemon_proc.poll() is not None:
            print("Daemon failed to start")
            print(daemon_proc.stderr.read().decode())
            return 1

        # NOTE: We need the CLI tool 'yams' to load plugins.
        # Assuming 'yams' is in build/release/src/cli/yams
        CLI_BIN = DAEMON_BIN.replace("daemon/yamsd", "cli/yams").replace("yamsd", "yams")
        if not os.path.exists(CLI_BIN):
             # Try side-by-side
             CLI_BIN = os.path.join(os.path.dirname(DAEMON_BIN), "yams")

        if not os.path.exists(CLI_BIN):
            print(f"CLI binary not found at {CLI_BIN}")
            daemon_proc.terminate()
            return 77

        print(f"Using CLI: {CLI_BIN}")

        # 1. Trust Plugin
        subprocess.run([CLI_BIN, "plugin", "trust", "add", plugin_path], check=True)

        # 2. Load Plugin
        print("Loading plugin...")
        subprocess.run([CLI_BIN, "plugin", "load", plugin_path], check=True)
        time.sleep(1)

        # 3. Verify Plugin Process Exists
        # The daemon should have spawned 'python plugin.py'
        # We can check children of daemon
        children = get_child_pids(daemon_proc.pid)
        print(f"Daemon children: {children}")
        
        plugin_pid = None
        for pid in children:
            try:
                p = psutil.Process(pid)
                cmd = p.cmdline()
                if plugin_path in cmd or (len(cmd) > 1 and plugin_path in cmd[1]):
                    plugin_pid = pid
                    break
            except:
                pass
        
        if not plugin_pid:
            print("Could not find plugin process spawned by daemon")
            # Dump daemon output
            # print(daemon_proc.stdout.read().decode())
            daemon_proc.terminate()
            return 1
            
        print(f"Found plugin process: {plugin_pid}")

        # 4. Unload Plugin
        print("Unloading plugin...")
        subprocess.run([CLI_BIN, "plugin", "unload", "cleanup_live"], check=True)
        time.sleep(2)

        # 5. Verify Plugin Process Gone
        if psutil.pid_exists(plugin_pid):
            print(f"FAILURE: Plugin process {plugin_pid} still exists after unload!")
            daemon_proc.terminate()
            return 1
            
        print("SUCCESS: Plugin process terminated.")

    finally:
        if 'daemon_proc' in locals() and daemon_proc.poll() is None:
            daemon_proc.terminate()
            daemon_proc.wait()
        
        if os.path.exists(plugin_path):
            os.unlink(plugin_path)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

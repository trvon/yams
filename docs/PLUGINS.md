# YAMS Plugin System

YAMS plugins extend the system with new capabilities. Choose your plugin type:

| Type | Use Case | Language | Deployment |
|------|----------|----------|------------|
| **External** | Prototyping, integration, scripting | Python, Node.js, any | Process-based |
| **Native** | Production, performance-critical | C/C++ | Shared library |

---

## External Plugins (Recommended for New Development)

External plugins run as separate processes communicating via JSON-RPC 2.0 over stdio.

### Minimal Python Plugin

```python
#!/usr/bin/env python3
"""Minimal YAMS external plugin."""
import json
import sys

def handle(req):
    method = req.get("method", "")

    if method == "handshake.manifest":
        return {"name": "my_plugin", "version": "1.0.0", "interfaces": []}
    elif method == "plugin.init":
        return {"status": "ok"}
    elif method == "plugin.health":
        return {"status": "ok"}
    elif method == "plugin.shutdown":
        return {"status": "ok"}
    else:
        raise ValueError(f"Unknown method: {method}")

if __name__ == "__main__":
    for line in sys.stdin:
        req = json.loads(line.strip())
        try:
            result = handle(req)
            resp = {"jsonrpc": "2.0", "id": req.get("id"), "result": result}
        except Exception as e:
            resp = {"jsonrpc": "2.0", "id": req.get("id"), "error": {"code": -32603, "message": str(e)}}
        print(json.dumps(resp), flush=True)
```

### Using the Python SDK

```python
#!/usr/bin/env python3
from yams_sdk import BasePlugin, rpc

class MyPlugin(BasePlugin):
    def manifest(self):
        return {
            "name": "my_plugin",
            "version": "1.0.0",
            "interfaces": ["content_extractor_v1"],
            "capabilities": {
                "content_extraction": {"formats": ["text/plain"]}
            }
        }

    def init(self, config):
        self.config = config

    def health(self):
        return {"status": "ok"}

    @rpc("extractor.supports")
    def supports(self, mime_type=None, extension=None):
        return {"supported": extension == ".txt" or mime_type == "text/plain"}

    @rpc("extractor.extract")
    def extract(self, source, options=None):
        with open(source["path"]) as f:
            text = f.read()
        return {"success": True, "content": text, "metadata": {}}

if __name__ == "__main__":
    MyPlugin().run()
```

### Required RPC Methods

Every external plugin MUST implement these JSON-RPC methods:

| Method | Purpose | Response |
|--------|---------|----------|
| `handshake.manifest` | Return plugin metadata | `{name, version, interfaces, capabilities}` |
| `plugin.init` | Initialize with config | `{status: "ok"}` |
| `plugin.health` | Report health status | `{status: "ok"\|"degraded"\|"error"}` |
| `plugin.shutdown` | Graceful cleanup | `null` or `{status: "ok"}` |

### Content Extractor Interface

Plugins implementing `content_extractor_v1` must also provide:

| Method | Purpose | Response |
|--------|---------|----------|
| `extractor.supports` | Check file type support | `{supported: bool}` |
| `extractor.extract` | Extract text content | `{success: bool, content: str, metadata: {}}` |

### Plugin Directory Structure

For production deployment, organize plugins as directories:

```
my_plugin/
├── yams-plugin.json    # Manifest (required)
├── plugin              # Compiled binary (Linux/macOS)
├── plugin.exe          # Compiled binary (Windows)
└── plugin.py           # Python fallback
```

### Plugin Manifest (`yams-plugin.json`)

```json
{
  "name": "my_plugin",
  "version": "1.0.0",
  "entry": {
    "binary": {
      "linux": "${plugin_dir}/plugin",
      "darwin": "${plugin_dir}/plugin",
      "windows": "${plugin_dir}/plugin.exe"
    },
    "fallback_cmd": ["python3", "-u", "${plugin_dir}/plugin.py"],
    "env": {"LOG_LEVEL": "INFO"}
  },
  "capabilities": {
    "content_extraction": {"formats": ["application/pdf"]}
  }
}
```

**Variable substitution:** `${plugin_dir}` expands to the plugin directory path.

### Entry Point Resolution

The host loads plugins in this priority order:

1. `entry.binary.<platform>` - Platform-specific compiled binary (most secure)
2. `plugin` or `plugin.exe` - Default compiled binary in plugin directory
3. `entry.fallback_cmd` or `entry.cmd` - Interpreter command
4. Direct execution based on file extension (`.py` → Python, `.js` → Node.js)

---

## Native Plugins (C ABI)

Native plugins are shared libraries loaded directly into the daemon process.

### Required Entry Points

```c
#include "yams/plugins/abi.h"

int yams_plugin_get_abi_version(void)          { return 1; }
const char* yams_plugin_get_name(void)         { return "my_plugin"; }
const char* yams_plugin_get_version(void)      { return "1.0.0"; }
const char* yams_plugin_get_manifest_json(void);
int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out);
int yams_plugin_init(const char* config_json, const void* host_context);
void yams_plugin_shutdown(void);
int yams_plugin_get_health_json(char** out_json);  // Optional
```

### Interface Pattern

Plugins expose vtables for specific capabilities:

```c
typedef struct content_extractor_v1 {
    void* handle;
    int (*extract_content)(void* handle, const char* path, char** out_text, char** out_metadata);
    void (*free_extracted_content)(void* handle, char* text, char* metadata);
    int (*get_supported_extensions)(void* handle, char*** out_exts, uint32_t* out_count);
    void (*free_extension_list)(void* handle, char** exts, uint32_t count);
} content_extractor_v1_t;
```

### Standard Interfaces

| Interface | Purpose |
|-----------|---------|
| `content_extractor_v1` | Document text extraction |
| `symbol_extractor_v1` | Binary symbol extraction |
| `model_provider_v1` | Embedding generation |
| `graph_adapter_v1` | Graph storage backend |
| `search_provider_v1` | Search backend |

### Error Codes

```c
#define YAMS_PLUGIN_OK            0
#define YAMS_PLUGIN_ERR_INVALID   1
#define YAMS_PLUGIN_ERR_NOT_FOUND 3
#define YAMS_PLUGIN_ERR_IO        4
```

### Memory Management

- Plugin allocates with `malloc()`, host frees with `free()`
- Vtables have static lifetime (valid until plugin unload)
- Thread-safe unless documented otherwise

---

## Plugin Loading & Trust

### Trust Policy

**Default deny:** Only explicitly trusted paths are loaded.

```bash
# Manage trust
yams plugin trust add /path/to/plugin
yams plugin trust list
yams plugin trust remove /path/to/plugin

# Development override
export YAMS_PLUGIN_TRUST_ALL=1
```

Canonical trust file: `<data_dir>/plugins.trust` (default `~/.local/share/yams/plugins.trust`).
Legacy path `~/.config/yams/plugins_trust.txt` is imported for compatibility.

### Search Paths

1. Persisted trusted roots from `<data_dir>/plugins.trust`
2. Optional `[daemon].plugin_dir` and `[daemon|plugins].trusted_paths`
3. Built-in defaults (`~/.local/lib/yams/plugins`, `/opt/homebrew/lib/yams/plugins` on macOS,
   `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`) unless strict mode is enabled

Strict mode controls:
- Config: `[daemon].plugin_dir_strict = true`
- Env override: `YAMS_PLUGIN_DIR_STRICT=1`

### CLI Commands

```bash
yams plugin scan                  # Discover plugins
yams plugin list                  # Show loaded plugins
yams plugin load <path|name>      # Load a plugin
yams plugin unload <name>         # Unload a plugin
yams plugin info <name>           # Show plugin details
```

---

## Process Lifecycle (External Plugins)

### States

```
Unstarted → Starting → Ready ⇄ Busy → ShuttingDown → Terminated
                ↓                           ↓
             Failed ←───────────────────────┘
```

### Platform Implementation

| Platform | Spawn | Cleanup | Termination |
|----------|-------|---------|-------------|
| Windows | `CreateProcess` | Job Objects | `TerminateProcess` |
| Unix | `fork`/`exec` | Process groups | `SIGTERM` → `SIGKILL` |

### Automatic Recovery

Crashed plugins are restarted with exponential backoff (up to 3 retries by default).

---

## JSON-RPC Protocol Reference

### Request Format

```json
{"jsonrpc": "2.0", "id": 1, "method": "method_name", "params": {...}}
```

### Success Response

```json
{"jsonrpc": "2.0", "id": 1, "result": {...}}
```

### Error Response

```json
{"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": "...", "data": {...}}}
```

### Standard Error Codes

| Code | Name | Meaning |
|------|------|---------|
| -32700 | Parse error | Invalid JSON |
| -32600 | Invalid Request | Missing required fields |
| -32601 | Method not found | Unknown method |
| -32602 | Invalid params | Bad parameters |
| -32603 | Internal error | Plugin error |
| -32000 | Extraction failed | Content extraction failed |
| -32001 | Unsupported format | Cannot handle file type |

---

## Examples

### Complete External Plugin Example

See: `tests/fixtures/mock_plugin.py`

### Native Plugin Examples

- `plugins/onnx/` - Model provider (ONNX Runtime)
- `plugins/pdf_extractor/` - Content extractor (MuPDF)
- `plugins/symbol_extractor_treesitter/` - Symbol extractor

### External Plugin Examples

- `plugins/yams-ghidra-plugin/` - Binary analysis (PyGhidra)

---

## Reference

| Resource | Path |
|----------|------|
| C ABI Specification | `docs/spec/plugin_spec.md` |
| JSON-RPC Protocol | `docs/spec/external_plugin_jsonrpc_protocol.md` |
| Interface Headers | `include/yams/plugins/*.h` |
| Python SDK | `external/yams-sdk/` |
| Interface Registry | `docs/spec/interface_versions.json` |

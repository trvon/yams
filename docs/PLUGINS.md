# YAMS Plugin System

YAMS loads plugins to extend extraction, embeddings, storage, search, and graph backends.

| Type         | Use case                              | Language    | Deployment       |
|--------------|---------------------------------------|-------------|------------------|
| **External** | Prototyping, integration, scripting   | Python, JS, any | Subprocess (JSON-RPC over stdio) |
| **Native**   | Performance-critical, in-process      | C/C++       | Shared library (C ABI) |

## Trust and discovery

**Default deny.** Only paths in the trust file load.

```bash
yams plugin trust add ~/.local/lib/yams/plugins   # trust a directory
yams plugin trust list
yams plugin trust remove <path>

yams plugin scan                                  # discover
yams plugin list                                  # loaded
yams plugin load <path|name>
yams plugin unload <name>
yams plugin info <name>
yams plugin health
yams doctor plugin <name>                         # diagnose one
```

Trust file: `<data_dir>/plugins.trust` (default `~/.local/share/yams/plugins.trust`). Legacy `~/.config/yams/plugins_trust.txt` is imported.

**Search order:**

1. Persisted trusted roots from `plugins.trust`
2. `[daemon].plugin_dir` and `[daemon|plugins].trusted_paths` in config
3. Built-in defaults: `~/.local/lib/yams/plugins`, `/opt/homebrew/lib/yams/plugins` (macOS), `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins` — unless strict mode is on

Strict mode: `[daemon].plugin_dir_strict = true` or `YAMS_PLUGIN_DIR_STRICT=1`. Dev override: `YAMS_PLUGIN_TRUST_ALL=1`.

## Bundled plugins

| Plugin                         | Purpose                                      | Details |
|--------------------------------|----------------------------------------------|---------|
| `onnx`                         | ONNX Runtime embeddings (CPU + GPU)          | [plugins/onnx/README.md](../plugins/onnx/README.md) |
| `object_storage_s3`            | S3 / R2-compatible object storage backend    | [plugins/object_storage_s3/README.md](../plugins/object_storage_s3/README.md) |
| `zyp`                          | PDF text + metadata extraction               | `plugins/zyp/` |
| `symbol_extractor_treesitter`  | Tree-sitter symbols for 18 languages         | `plugins/symbol_extractor_treesitter/` |
| `glint`                        | GLiNER natural-language entity extraction    | [plugins/glint/README.md](../plugins/glint/README.md) |
| `hound`                        | Graph adapter mapping Hound → YAMS KG        | `plugins/hound/` |
| `yams-ghidra-plugin`           | Ghidra binary analysis (external, PyGhidra)  | `plugins/yams-ghidra-plugin/` |

## Standard interfaces

| Interface              | Header                                              | Purpose                      |
|------------------------|-----------------------------------------------------|------------------------------|
| `content_extractor_v1` | `include/yams/plugins/content_extractor_v1.h`       | Document text extraction     |
| `symbol_extractor_v1`  | `include/yams/plugins/symbol_extractor_v1.h`        | Source / binary symbols      |
| `entity_extractor_v2`  | `include/yams/plugins/entity_extractor_v2.h`        | Named entity extraction      |
| `search_provider_v1`   | `include/yams/plugins/search_provider_v1.h`         | Search backend               |
| `graph_adapter_v1`     | See [docs/api/graph_adapter_v1.md](api/graph_adapter_v1.md) | KG adapter           |
| `model_provider_v1`    | `include/yams/ml/`                                  | Embedding model provider     |
| `object_storage_v1`    | See [docs/api/storage_plugin_v1.md](api/storage_plugin_v1.md) | Object storage       |

Interface versions are tracked in [docs/spec/interface_versions.json](spec/interface_versions.json).

---

## External plugins (JSON-RPC)

External plugins run as subprocesses communicating JSON-RPC 2.0 over stdio. Every external plugin must implement:

| Method               | Purpose                | Response                                              |
|----------------------|------------------------|-------------------------------------------------------|
| `handshake.manifest` | Plugin metadata        | `{name, version, interfaces, capabilities}`           |
| `plugin.init`        | Initialize with config | `{status: "ok"}`                                      |
| `plugin.health`      | Health status          | `{status: "ok"\|"degraded"\|"error"}`                 |
| `plugin.shutdown`    | Graceful cleanup       | `{status: "ok"}`                                      |

Per-interface methods are defined in [docs/spec/external_plugin_jsonrpc_protocol.md](spec/external_plugin_jsonrpc_protocol.md).

### Minimal Python example

```python
#!/usr/bin/env python3
import json, sys

def handle(req):
    m = req.get("method", "")
    if m == "handshake.manifest":
        return {"name": "my_plugin", "version": "1.0.0", "interfaces": []}
    if m in ("plugin.init", "plugin.health", "plugin.shutdown"):
        return {"status": "ok"}
    raise ValueError(f"Unknown method: {m}")

for line in sys.stdin:
    req = json.loads(line)
    try:
        resp = {"jsonrpc": "2.0", "id": req.get("id"), "result": handle(req)}
    except Exception as e:
        resp = {"jsonrpc": "2.0", "id": req.get("id"),
                "error": {"code": -32603, "message": str(e)}}
    print(json.dumps(resp), flush=True)
```

The higher-level `yams_sdk.BasePlugin` is in [external/yams-sdk/](../external/yams-sdk/).

### Plugin directory layout

```
my_plugin/
├── yams-plugin.json    # manifest (required)
├── plugin              # compiled binary (Linux/macOS)
├── plugin.exe          # compiled binary (Windows)
└── plugin.py           # interpreter fallback
```

Manifest fields and `${plugin_dir}` substitution: see [docs/spec/plugin_spec.md](spec/plugin_spec.md).

### Process lifecycle

```
Unstarted → Starting → Ready ⇄ Busy → ShuttingDown → Terminated
                ↓                         ↓
             Failed ←───────────────────── ┘
```

Crashed plugins are restarted with exponential backoff (up to 3 retries). Unix uses `SIGTERM → SIGKILL` with process groups; Windows uses Job Objects and `TerminateProcess`.

---

## Native plugins (C ABI)

Required entry points — full signatures in [include/yams/plugins/abi.h](../include/yams/plugins/abi.h):

```c
int         yams_plugin_get_abi_version(void);
const char* yams_plugin_get_name(void);
const char* yams_plugin_get_version(void);
const char* yams_plugin_get_manifest_json(void);
int         yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out);
int         yams_plugin_init(const char* config_json, const void* host_context);
void        yams_plugin_shutdown(void);
int         yams_plugin_get_health_json(char** out_json);  // optional
```

Interfaces are exposed as vtables (see `include/yams/plugins/*.h`). Memory: plugin allocates with `malloc`, host frees with `free`. Vtables have static lifetime. Thread-safe unless documented otherwise.

## Object storage provider compatibility

- **Cloudflare R2** — tested in-repo.
- **AWS S3** — same interface, not validated by automated tests here.

See [plugins/object_storage_s3/README.md](../plugins/object_storage_s3/README.md) for setup and [docs/api/storage_plugin_v1.md](api/storage_plugin_v1.md) for capability expectations.

## References

| Resource             | Path                                                          |
|----------------------|---------------------------------------------------------------|
| C ABI spec           | [docs/spec/plugin_spec.md](spec/plugin_spec.md)               |
| JSON-RPC spec        | [docs/spec/external_plugin_jsonrpc_protocol.md](spec/external_plugin_jsonrpc_protocol.md) |
| Interface versions   | [docs/spec/interface_versions.json](spec/interface_versions.json) |
| Interface headers    | `include/yams/plugins/*.h`                                    |
| Python SDK           | `external/yams-sdk/`                                          |
| Mock plugin          | `tests/fixtures/mock_plugin.py`                               |

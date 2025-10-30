# YAMS Plugin System

YAMS supports two plugin types:
- **Native (C ABI)**: Shared libraries (.so, .dylib, .dll) for production use
- **External (Python)**: JSON-RPC over stdio for prototyping and integration

**Reference**: `docs/spec/plugin_spec.md` for complete C ABI specification

## Quick Start

### Native Plugin (C/C++)
```c
#include "yams/plugins/abi.h"
#include "yams/plugins/content_extractor_v1.h"

int yams_plugin_get_abi_version(void) { return 1; }
const char* yams_plugin_get_name(void) { return "my_plugin"; }
// ... implement other entry points and interfaces
```

### External Plugin (Python)
```python
from yams_sdk import BasePlugin, rpc

class MyPlugin(BasePlugin):
    def manifest(self):
        return {"name": "my_plugin", "version": "1.0.0", "interfaces": ["content_extractor_v1"]}
    
    @rpc("extract.content")
    def extract(self, content: dict) -> dict:
        return {"text": "...", "metadata": {}}

if __name__ == "__main__":
    MyPlugin().run()
```

## Native Plugins (C ABI)

### Entry Points
```c
int yams_plugin_get_abi_version(void);        // Return 1
const char* yams_plugin_get_name(void);       // Static string
const char* yams_plugin_get_version(void);    // Static string
const char* yams_plugin_get_manifest_json(void);
int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface);
int yams_plugin_init(const char* config_json, const void* host_context);
void yams_plugin_shutdown(void);
int yams_plugin_get_health_json(char** out_json);  // Optional
```

### Interface Pattern
Plugins expose vtables for specific capabilities:
```c
typedef struct content_extractor_v1 {
    void* handle;
    bool (*supports)(const char* mime_type, const char* extension);
    int (*extract)(const uint8_t* content, size_t len, extraction_result_t** out);
    void (*free_result)(extraction_result_t* result);
} content_extractor_v1_t;
```

### Standard Interfaces
| Interface | Header | Version | Purpose |
|-----------|--------|---------|---------|
| `model_provider_v1` | `model_provider_v1.h` | 2 | Embedding generation, model management |
| `content_extractor_v1` | `content_extractor_v1.h` | 1 | Document text extraction |
| `symbol_extractor_v1` | `symbol_extractor_v1.h` | 1 | Binary symbol extraction |
| `graph_adapter_v1` | `graph_adapter_v1.h` | 1 | Graph storage backend |
| `object_storage_v1` | `object_storage_v1.h` | 1 | Object storage backend |
| `search_provider_v1` | `search_provider_v1.h` | 1 | Search backend |

### Error Codes
```c
#define YAMS_PLUGIN_OK 0
#define YAMS_PLUGIN_ERR_INVALID -4
#define YAMS_PLUGIN_ERR_NOT_FOUND -2
#define YAMS_PLUGIN_ERR_INCOMPATIBLE -1
```

### Memory Management
- Plugin allocates with `malloc()`, host frees with `free()`
- Host manages callback data
- Vtables have static lifetime
- Thread-safe unless documented otherwise

## External Plugins (Python)

### SDK Installation
```bash
pip install yams-sdk
# Or from source:
pip install -e external/yams_sdk
```

### Plugin Structure
```python
from yams_sdk import BasePlugin, rpc

class MyPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        # Auto-register @rpc methods
        for name in dir(self):
            fn = getattr(self, name)
            rpc_name = getattr(fn, "__rpc_name__", None)
            if rpc_name:
                self.register(rpc_name, fn)
    
    def manifest(self):
        return {
            "name": "my_plugin",
            "version": "1.0.0",
            "interfaces": ["my_interface_v1"]
        }
    
    def init(self, config):
        # Initialize from daemon config
        pass
    
    def health(self):
        return {"status": "ok"}
    
    @rpc("my.method")
    def my_method(self, param: dict) -> dict:
        return {"result": "..."}

if __name__ == "__main__":
    MyPlugin().run()
```

### SDK Modules
- **`yams_sdk.base`** - BasePlugin, JSON-RPC transport
- **`yams_sdk.decorators`** - @rpc decorator
- **`yams_sdk.abi`** - Error codes, interface constants, manifest helpers
- **`yams_sdk.interfaces`** - Type-safe protocols for standard interfaces
- **`yams_sdk.testing`** - PluginTestHarness, validation utilities

### Testing
```python
from yams_sdk.testing import PluginTestHarness

with PluginTestHarness(["python3", "my_plugin.py"]) as h:
    manifest = h.handshake()
    h.init({"config_key": "value"})
    result = h.call("my.method", {"param": "data"})
    health = h.health()
```

## Plugin Loading

### Search Paths (priority order)
1. Configured directories (`plugin_dirs` in daemon config)
2. `$YAMS_PLUGIN_DIR` environment variable
3. `~/.local/lib/yams/plugins`
4. `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
5. `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`

### Trust Policy
**Default deny**: Only explicitly trusted paths are loaded.

```bash
# Trust management
yams plugin trust add /path/to/plugin.so
yams plugin trust list
yams plugin trust remove /path/to/plugin.so
```

**Trust file**: `~/.config/yams/plugins_trust.txt` (one path per line)  
**Dev override**: `export YAMS_PLUGIN_TRUST_ALL=1`

### CLI Commands
```bash
# Scan for plugins
yams plugin scan                    # All search paths
yams plugin scan --dir /custom/path
yams plugin scan /path/to/plugin.so

# List loaded plugins
yams plugin list
yams plugin list -v                 # Show skipped plugins

# Load/unload
yams plugin load /path/to/plugin.so
yams plugin load plugin_name
yams plugin unload plugin_name

# Plugin info
yams plugin info plugin_name        # Manifest and health
```

### Configuration
```toml
# ~/.yams/config/daemon.toml
[plugins]
plugin_dirs = ["/usr/local/lib/yams/plugins", "~/.local/lib/yams/plugins"]
name_policy = "relaxed"  # or "spec"

[embeddings]
preferred_model = "hf://sentence-transformers/all-MiniLM-L6-v2"
```

## Development

### Building Native Plugins
```bash
# In plugins/my_plugin/
add_library(yams_my_plugin SHARED plugin.cpp)
target_include_directories(yams_my_plugin PRIVATE ${CMAKE_SOURCE_DIR}/include)
install(TARGETS yams_my_plugin DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins)
```

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build
```

### Building Python Plugins
```bash
# In plugins/my_plugin/ with pyproject.toml
uv build
uv pip install dist/*.whl
```

### Testing Workflow
```bash
# 1. Trust plugin directory
yams plugin trust add /usr/local/lib/yams/plugins

# 2. Scan to verify
yams plugin scan

# 3. Load plugin
yams plugin load /usr/local/lib/yams/plugins/yams_my_plugin.so

# 4. Verify loaded
yams plugin list
yams plugin info my_plugin
```

## Implementation Examples

- **`plugins/onnx/`** - Model provider (C++, ONNX Runtime)
- **`plugins/pdf_extractor/`** - Content extractor (C++, MuPDF)
- **`plugins/symbol_extractor_treesitter/`** - Symbol extractor (C++, Tree-sitter)
- **`plugins/yams-ghidra-plugin/`** - Binary analysis (Python, PyGhidra)
- **`plugins/object_storage_s3/`** - Object storage (C++, AWS SDK)

## Reference

- **Spec**: `docs/spec/plugin_spec.md` - Complete C ABI specification
- **Headers**: `include/yams/plugins/*.h` - Interface definitions
- **Interface registry**: `docs/spec/interface_versions.json`
- **SDK**: `external/yams_sdk/README.md` - Python SDK documentation

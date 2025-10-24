# YAMS Plugin ABI Specification

This document provides an overview of the YAMS plugin system based on the stable C ABI. For detailed specifications, see `docs/spec/plugin_spec.md`.

## Core Components

- **ABI Specification**: `docs/spec/plugin_spec.md` - Complete C ABI reference
- **Core ABI Header**: `include/yams/plugins/abi.h` - Plugin entry points and constants
- **Interface Headers**: 
  - `include/yams/plugins/model_provider_v1.h` - Model provider interface
  - `include/yams/plugins/content_extractor_v1.h` - Content extraction interface
  - `include/yams/plugins/host_services_v1.h` - Host callback services (optional)

## Plugin Architecture

### C-ABI Native Plugins

Plugins are native shared libraries (.so, .dylib, .dll) that implement the YAMS C ABI:

```c
// Required plugin entry points
int yams_plugin_get_abi_version(void);
const char* yams_plugin_get_name(void);
const char* yams_plugin_get_version(void);
const char* yams_plugin_get_manifest_json(void);
int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface);
int yams_plugin_init(const char* config_json, const void* host_context);
void yams_plugin_shutdown(void);
```

### Interface VTables

Plugins expose functionality through versioned interface vtables:

```c
// Model provider interface example
typedef struct model_provider_v1 {
    void* handle;
    int (*load_model)(void* handle, const char* model_id, const char* model_path, const char* options_json);
    int (*generate_embedding)(void* handle, const char* model_id, const uint8_t* input, size_t input_len, float** out_vec, uint32_t* out_dim);
    void (*free_embedding)(void* handle, float* vec, uint32_t dim);
    // ... additional methods
} model_provider_v1_t;
```

## Plugin Discovery and Loading

### Search Order (first match wins)

1. **Configured directories**: `plugin_dir` or `plugin_dirs` from daemon config
2. **Environment override**: `YAMS_PLUGIN_DIR` (deprecated)
3. **User directory**: `$HOME/.local/lib/yams/plugins`
4. **System directories**: `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
5. **Install prefix**: `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`

### Trust Policy

- **Default deny**: Only plugins from trusted directories are loaded
- **Trust file**: `~/.config/yams/plugins_trust.txt` (one absolute path per line)
- **CLI management**:
  ```bash
  yams plugin trust add /path/to/plugin.so
  yams plugin trust list
  yams plugin trust remove /path/to/plugin.so
  ```
- **Development override**: `YAMS_PLUGIN_TRUST_ALL=1` (use with caution)

### Naming and Deduplication

- **Filename preference**: Non-`lib` prefixed files preferred over `lib*` variants
- **Logical name deduplication**: Only one implementation per provider type
- **Priority order**: User dir > system dir > install prefix

### Name Policy

Configure how plugin names are displayed and selected:

- **Relaxed** (default): Derive canonical name from filename when manifest doesn't specify
- **Spec**: Use manifest `name` field verbatim

```bash
export YAMS_PLUGIN_NAME_POLICY=spec
yams plugins list
```

## Standard Interfaces

### Model Provider v1

For embedding generation and model management:
- **Header**: `include/yams/plugins/model_provider_v1.h`
- **Version**: v1.2 (pre-stable)
- **Key features**:
  - Non-blocking model loading with progress callbacks
  - Single and batch embedding generation
  - Runtime info querying (backend, threading, model metadata)
  - Dimension discovery without model loading

### Content Extractor v1

For document content extraction:
- **Header**: `include/yams/plugins/content_extractor_v1.h`
- **Purpose**: Extract text and metadata from various document formats
- **Key methods**: `extract_content()`, `get_supported_extensions()`

### Host Services v1 (Optional)

For plugins that need to call back into YAMS:
- **Header**: `include/yams/plugins/host_services_v1.h`
- **Services**: Document service, search service, future extensions
- **Usage**: Passed via `host_context` parameter in `yams_plugin_init()`

## Memory Management Rules

- **Plugin allocations**: Use `malloc()`/`calloc()`, host calls `free()`
- **Host allocations**: Host manages memory for callback data
- **Interface vtables**: Static lifetime during plugin load
- **String returns**: Plugin allocates with `malloc()`, host frees with `free()`

## Error Handling

Plugins SHOULD use standard error codes:

```c
#define YAMS_PLUGIN_OK 0
#define YAMS_PLUGIN_ERR_INVALID 1
#define YAMS_PLUGIN_ERR_INCOMPATIBLE 2
#define YAMS_PLUGIN_ERR_NOT_FOUND 3
#define YAMS_PLUGIN_ERR_IO 4
#define YAMS_PLUGIN_ERR_INTERNAL 5
#define YAMS_PLUGIN_ERR_UNSUPPORTED 6
```

## Plugin Development

### Basic Plugin Structure

```c
#include "yams/plugins/abi.h"
#include "yams/plugins/model_provider_v1.h"

static model_provider_v1_t g_model_provider = { /* implementation */ };

int yams_plugin_get_abi_version(void) {
    return 1;
}

const char* yams_plugin_get_name(void) {
    return "my_model_provider";
}

const char* yams_plugin_get_version(void) {
    return "1.0.0";
}

const char* yams_plugin_get_manifest_json(void) {
    return "{"
           "\"name\":\"my_model_provider\","
           "\"version\":\"1.0.0\","
           "\"interfaces\":[{\"id\":\"model_provider_v1\",\"version\":2}]"
           "}";
}

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (strcmp(iface_id, "model_provider_v1") == 0 && version == 2) {
        *out_iface = &g_model_provider;
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_UNSUPPORTED;
}

int yams_plugin_init(const char* config_json, const void* host_context) {
    // Initialize plugin resources
    return YAMS_PLUGIN_OK;
}

void yams_plugin_shutdown(void) {
    // Cleanup plugin resources
}
```

### Build Integration

Use CMake integration from the plugin directory:

```cmake
# In plugins/my_plugin/CMakeLists.txt
add_library(yams_my_plugin SHARED
    plugin.cpp
    implementation.cpp
)

target_include_directories(yams_my_plugin PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

# Install to standard plugin location
install(TARGETS yams_my_plugin
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/yams/plugins
)
```

## Runtime Notes

- **Preferred provider selection**: Daemon prefers host-backed `model_provider_v1` plugins
- **Async readiness**: Model loading is non-blocking; readiness indicated via callbacks
- **Thread safety**: Interface methods may be called from multiple threads
- **Health monitoring**: Optional health JSON via `yams_plugin_get_health_json()`

## Examples

### Trust and Load Plugin

```bash
# Build and install plugin
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build

# Trust the plugin directory
yams plugin trust add /usr/local/lib/yams/plugins

# Scan for available plugins
yams plugin scan

# Load specific plugin (daemon running)
yams plugin load /usr/local/lib/yams/plugins/yams_my_plugin.so
```

### Configuration Example

```toml
# daemon config
[plugins]
plugin_dirs = ["/usr/local/lib/yams/plugins", "~/.local/lib/yams/plugins"]
name_policy = "relaxed"  # or "spec"

[embeddings]
preferred_model = "hf://sentence-transformers/all-MiniLM-L6-v2"
```

## SDK and Testing

- **SDK location**: `external/yams-sdk`
- **Python helpers**: JSON-RPC templates and conformance runners
- **Test harness**: `yams/tests/sdk` for ABI validation
- **Example implementations**: See `plugins/onnx` for reference

For complete interface specifications, error codes, and lifecycle details, refer to:
- `docs/spec/plugin_spec.md`
- Individual interface headers in `include/yams/plugins/`

## External Plugin Examples

While this specification focuses on the core C ABI, YAMS also supports external plugin examples for reference:

### Ghidra Analysis Plugin
- **Location**: `plugins/yams-ghidra-plugin/`
- **Transport**: External process via JSON-RPC
- **Purpose**: Demonstrates binary analysis integration using PyGhidra
- **Note**: Example of external process communication, not core ABI

These examples showcase integration patterns but the primary plugin development should target the C ABI specification above for production use.

## Future Plugin Opportunities

Binary/specialized file types currently fail text extraction and would benefit from dedicated extractor plugins.
# YAMS Plugin ABI Specification

## Core C-ABI Plugin Interface

The YAMS plugin system is built around a stable C ABI that enables plugins to expose services to the host and optionally call back into host services.

> **Implementation note (2026-06-26):** The canonical ABI lives in the shipped headers under `include/yams/plugins/`. This document summarizes the current contract, but exact field layouts, optional callbacks, and interface version constants should be taken from those headers when implementing or reviewing a plugin.

### Plugin Entry Points

Every plugin MUST implement the following symbols:

```c
// Return the plugin ABI version (always 1)
int yams_plugin_get_abi_version(void);

// Return the plugin name (static string, lifetime of plugin)
const char* yams_plugin_get_name(void);

// Return the plugin version (static string, lifetime of plugin)  
const char* yams_plugin_get_version(void);

// Return the plugin manifest as JSON (static string, lifetime of plugin)
const char* yams_plugin_get_manifest_json(void);

// Get an interface vtable by ID and version
// Returns 0 on success, non-zero on error/interface not found
int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface);

// Initialize the plugin with config JSON and optional host context
// Returns 0 on success, non-zero error code on failure
int yams_plugin_init(const char* config_json, const void* host_context);

// Shutdown the plugin (cleanup resources)
void yams_plugin_shutdown(void);

// Optional: Get health status as JSON (allocated with malloc, host frees)
// Returns 0 on success, non-zero if not implemented
int yams_plugin_get_health_json(char** out_json);
```

### Memory Management

- **Plugin-allocated memory**: For any function that returns allocated memory (like `get_health_json`), the plugin MUST use `malloc()`/`calloc()`. The host is responsible for calling `free()`.
- **Host-allocated memory**: When the host provides data to plugins via callbacks, the host allocates and frees the memory.
- **Interface vtables**: Interface pointers returned by `get_interface` have static lifetime for the duration of the plugin load.

### Error Codes

Core loader entry points use the constants from `include/yams/plugins/abi.h`:

```c
#define YAMS_PLUGIN_OK 0
#define YAMS_PLUGIN_ERR_INCOMPATIBLE -1
#define YAMS_PLUGIN_ERR_NOT_FOUND -2
#define YAMS_PLUGIN_ERR_INIT_FAILED -3
#define YAMS_PLUGIN_ERR_INVALID -4
```

Individual interfaces may define richer status enums of their own. For example,
`model_provider_v1` uses `yams_status_t` with `YAMS_OK`,
`YAMS_ERR_INVALID_ARG`, `YAMS_ERR_NOT_FOUND`, `YAMS_ERR_IO`,
`YAMS_ERR_INTERNAL`, and `YAMS_ERR_UNSUPPORTED`.

## Interface VTables

### Interface Layout

Interfaces are versioned C structs declared in their own headers. Do **not** assume a single
universal vtable layout across all interfaces. The stable rule is:

1. negotiate an interface by `(id, version)` through `yams_plugin_get_interface(...)`
2. cast the returned pointer to the exact struct from the matching header
3. treat nullable callbacks as optional / unsupported behavior

### Common Interfaces

#### model_provider_v1

Header: `include/yams/plugins/model_provider_v1.h`
Current interface version constant: `YAMS_IFACE_MODEL_PROVIDER_V1_VERSION = 4`

The current vtable includes:

- `abi_version` and opaque `self`
- model lifecycle: `load_model`, `unload_model`, `is_model_loaded`, `get_loaded_models`
- embedding APIs: `generate_embedding`, `generate_embedding_batch`
- progress/config helpers: `set_progress_callback`, `get_embedding_dim`,
  `get_runtime_info_json`, `free_string`, `set_threading`
- optional higher-level extensions: `score_documents`, `free_scores`,
  `evict_under_pressure`

Memory ownership is paired: any plugin-allocated buffer must be released through the matching
`free_*` callback from the same vtable.

#### content_extractor_v1

Header: `include/yams/plugins/content_extractor_v1.h`
Current interface version constant: `YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION = 1`

The current ABI is **buffer-oriented**, not file-path oriented. The vtable includes:

- `abi_version`
- `supports(const char* mime_type, const char* extension)`
- `extract(const uint8_t* content, size_t content_len, yams_extraction_result_t** result)`
- `free_result(yams_extraction_result_t* result)`

`yams_extraction_result_t` carries extracted text, key/value metadata, and an optional error
string.

## Host Services API (Optional)

Plugins that need to call back into YAMS can use the Host Services API.

### Host Context

During `yams_plugin_init`, the host MAY pass a `yams_plugin_host_context_v1` from
`include/yams/plugins/host_services_v1.h`.

The current host context includes:

- `version`
- opaque `impl`
- `document_service`
- `search_service`
- `grep_service`

The same header defines the request/response structs and vtables for:

- `yams_document_service_v1` (`store`, `retrieve`, `list`, `cat`, plus paired `free_*` helpers)
- `yams_search_service_v1`
- `yams_grep_service_v1`

Plugins must null-check optional service pointers. Minimal daemon/test shims may pass a valid host
context with some or all service pointers unset.

## Plugin Manifest

Each plugin MUST provide a JSON manifest via `yams_plugin_get_manifest_json()`:

```json
{
  "name": "plugin_name",
  "version": "1.0.0",
  "description": "Brief description",
  "interfaces": [
    {
      "id": "model_provider_v1",
      "version": 4
    }
  ],
  "abi_version": 1,
  "capabilities": [],
  "dependencies": []
}
```

## Lifecycle

1. **Discovery**: Host scans for plugin files and reads symbols without side effects
2. **Validation**: Host verifies ABI version, manifest format, and required interfaces
3. **Initialization**: Host calls `yams_plugin_init()` with config and optional host context
4. **Interface Registration**: Host retrieves interface vtables via `yams_plugin_get_interface()`
5. **Operation**: Host calls into plugin interfaces as needed
6. **Shutdown**: Host calls `yams_plugin_shutdown()` before unloading

## Security Considerations

- Plugins run in the same process space as the host
- Install / distribution paths should validate checksums or signatures and maintain a trust list
- Plugins should validate all input parameters and handle errors gracefully
- Memory safety is critical - use proper bounds checking and null checks

## Threading

- Plugin initialization and shutdown are single-threaded
- Interface methods MAY be called from multiple threads concurrently
- Plugins MUST implement their own synchronization if needed
- Callbacks MAY be invoked from background threads

## Versioning

- Plugin ABI version is incremented for breaking changes
- Interface versions are incremented independently
- Host should support multiple interface versions in parallel
- Plugins should advertise the highest interface version they support

## Best Practices

- Use static allocation for fixed-size data structures
- Validate all pointers before dereferencing
- Provide meaningful error messages
- Implement graceful degradation for optional features
- Use consistent naming conventions across interfaces
- Document thread safety guarantees for each method

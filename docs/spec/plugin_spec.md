# YAMS Plugin ABI Specification

## Core C-ABI Plugin Interface

The YAMS plugin system is built around a stable C ABI that enables plugins to expose services to the host and optionally call back into host services.

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

## Interface VTables

### Standard Interface Pattern

All interfaces follow this pattern:

```c
typedef struct iface_name_v1 {
    void* handle;  // Plugin-specific context
    // Interface methods...
} iface_name_v1_t;
```

### Required Interfaces

#### model_provider_v1

Header: `include/yams/plugins/model_provider_v1.h`

```c
typedef struct model_provider_v1 {
    void* handle;
    
    // Set progress callback for long operations (optional)
    int (*set_progress_callback)(void* handle, 
                                 void (*callback)(void* user_data, const char* phase, int progress, int total),
                                 void* user_data);
    
    // Load a model (non-blocking)
    int (*load_model)(void* handle, const char* model_id, const char* model_path, const char* options_json);
    
    // Unload a model
    int (*unload_model)(void* handle, const char* model_id);
    
    // Check if model is loaded
    int (*is_model_loaded)(void* handle, const char* model_id, bool* out_loaded);
    
    // Get list of loaded models (plugin allocates arrays, host frees)
    int (*get_loaded_models)(void* handle, char*** out_ids, uint32_t* out_count);
    void (*free_model_list)(void* handle, char** ids, uint32_t count);
    
    // Generate single embedding
    int (*generate_embedding)(void* handle, const char* model_id, 
                             const uint8_t* input, size_t input_len,
                             float** out_vec, uint32_t* out_dim);
    void (*free_embedding)(void* handle, float* vec, uint32_t dim);
    
    // Generate batch embeddings
    int (*generate_embedding_batch)(void* handle, const char* model_id,
                                   const uint8_t** inputs, const size_t* lens, uint32_t batch,
                                   float*** out_vecs, uint32_t* out_batch, uint32_t* out_dim);
    void (*free_embedding_batch)(void* handle, float** vecs, uint32_t batch, uint32_t dim);
    
    // v1.2 extensions (optional, may return UNSUPPORTED)
    int (*get_embedding_dim)(void* handle, const char* model_id, uint32_t* out_dim);
    int (*get_runtime_info_json)(void* handle, const char* model_id, char** out_json);
    void (*free_string)(void* handle, char* str);
    int (*set_threading)(void* handle, const char* model_id, uint32_t intra_threads, uint32_t inter_threads);
} model_provider_v1_t;
```

#### content_extractor_v1

Header: `include/yams/plugins/content_extractor_v1.h`

```c
typedef struct content_extractor_v1 {
    void* handle;
    
    // Extract content from a document
    int (*extract_content)(void* handle, const char* file_path,
                           char** out_text, char** out_metadata_json);
    void (*free_extracted_content)(void* handle, char* text, char* metadata);
    
    // Get supported file extensions (plugin allocates, host frees)
    int (*get_supported_extensions)(void* handle, char*** out_extensions, uint32_t* out_count);
    void (*free_extension_list)(void* handle, char** extensions, uint32_t count);
} content_extractor_v1_t;
```

## Host Services API (Optional)

Plugins that need to call back into YAMS can use the Host Services API.

### Host Context

During `yams_plugin_init`, the host MAY pass a `yams_plugin_host_context_v1` struct:

```c
typedef struct yams_plugin_host_context_v1 {
    uint32_t version;  // YAMS_PLUGIN_HOST_SERVICES_API_VERSION
    const document_service_v1* document_service;
    const search_service_v1* search_service;
    // Additional services may be added in future versions
} yams_plugin_host_context_v1_t;
```

### Document Service v1

```c
typedef struct document_service_v1 {
    void* handle;
    
    int (*list_documents)(void* handle, const list_documents_request_t* req,
                         list_documents_response_t** out_res);
    void (*free_list_documents_response)(void* handle, list_documents_response_t* res);
    
    int (*get_document)(void* handle, const get_document_request_t* req,
                       get_document_response_t** out_res);
    void (*free_get_document_response)(void* handle, get_document_response_t* res);
    
    // Additional methods...
} document_service_v1_t;
```

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
      "version": 2
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
- Host should validate plugin signatures and maintain a trust list
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
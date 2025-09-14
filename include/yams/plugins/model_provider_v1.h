#ifndef YAMS_PLUGINS_MODEL_PROVIDER_V1_H
#define YAMS_PLUGINS_MODEL_PROVIDER_V1_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Interface identity
#define YAMS_IFACE_MODEL_PROVIDER_V1 "model_provider_v1"
#define YAMS_IFACE_MODEL_PROVIDER_V1_VERSION 2u

// Status codes (int ABI)
enum yams_status_e {
    YAMS_OK = 0,
    YAMS_ERR_INVALID_ARG = 1,
    YAMS_ERR_NOT_FOUND = 2,
    YAMS_ERR_IO = 3,
    YAMS_ERR_INTERNAL = 4,
    YAMS_ERR_UNSUPPORTED = 5
};
typedef int yams_status_t;

// Model load phases for progress callback
enum yams_model_load_phase_e {
    YAMS_MODEL_PHASE_UNKNOWN = 0,
    YAMS_MODEL_PHASE_PROBE = 1,
    YAMS_MODEL_PHASE_DOWNLOAD = 2,
    YAMS_MODEL_PHASE_LOAD = 3,
    YAMS_MODEL_PHASE_WARMUP = 4,
    YAMS_MODEL_PHASE_READY = 5
};

// Progress callback:
// - May be invoked from background threads.
// - 'current'/'total' in bytes or step counts when applicable; 0 if unknown.
// - 'message' may be NULL.
typedef void (*yams_model_load_progress_cb)(void* user, const char* model_id, int phase,
                                            uint64_t current, uint64_t total, const char* message);

// Vtable for model provider v1.
// Threading: All functions must be thread-safe unless documented otherwise.
// Memory ownership:
// - Any returned buffers are owned by the plugin and must be released with the corresponding free_*
// functions.
// - Host must not call free() directly on plugin-allocated memory.
typedef struct yams_model_provider_v1 {
    uint32_t abi_version; // must be YAMS_IFACE_MODEL_PROVIDER_V1_VERSION
    void* self;           // opaque plugin context

    // Set/replace progress callback (idempotent). Passing cb=NULL clears it.
    yams_status_t (*set_progress_callback)(void* self, yams_model_load_progress_cb cb, void* user);

    // Model lifecycle
    yams_status_t (*load_model)(
        void* self, const char* model_id,
        const char* model_path,    // local path or NULL if using internal resolution
        const char* options_json); // optional JSON string with provider-specific options (nullable)

    yams_status_t (*unload_model)(void* self, const char* model_id);

    yams_status_t (*is_model_loaded)(void* self, const char* model_id, bool* out_loaded);

    // Query loaded models
    yams_status_t (*get_loaded_models)(void* self,
                                       const char*** out_ids, // array of NUL-terminated strings
                                       size_t* out_count);

    void (*free_model_list)(void* self, const char** ids, size_t count);

    // Embedding generation (single)
    yams_status_t (*generate_embedding)(void* self, const char* model_id, const uint8_t* input,
                                        size_t input_len,
                                        float** out_vec, // length = out_dim
                                        size_t* out_dim);

    void (*free_embedding)(void* self, float* vec, size_t dim);

    // Embedding generation (batch)
    // Returns a single contiguous row-major buffer: [batch, dim]
    yams_status_t (*generate_embedding_batch)(void* self, const char* model_id,
                                              const uint8_t* const* inputs,
                                              const size_t* input_lens, size_t batch_size,
                                              float** out_vecs,  // length = out_batch * out_dim
                                              size_t* out_batch, // echoes batch_size on success
                                              size_t* out_dim);

    void (*free_embedding_batch)(void* self, float* vecs, size_t batch, size_t dim);

    // v1.2 extensions (non-breaking for unstable pre-release):
    // Retrieve embedding dimension for a loaded model (fast path when known)
    yams_status_t (*get_embedding_dim)(void* self, const char* model_id, size_t* out_dim);

    // Optional: return runtime/model info as JSON string. The host will free via free_string.
    // Suggested keys: backend (e.g., "onnxruntime"|"genai"), pipeline ("raw_ort"|"genai"),
    // model, dim, intra_threads, inter_threads, runtime_version.
    yams_status_t (*get_runtime_info_json)(void* self, const char* model_id, char** out_json);

    // Free a string allocated by the plugin (paired with get_runtime_info_json)
    void (*free_string)(void* self, char* s);

    // Optional: set threading parameters for a model or provider (no-op if unsupported)
    yams_status_t (*set_threading)(void* self, const char* model_id, int intra_threads,
                                   int inter_threads);
} yams_model_provider_v1;

#ifdef __cplusplus
} // extern "C"
#endif

#endif // YAMS_PLUGINS_MODEL_PROVIDER_V1_H

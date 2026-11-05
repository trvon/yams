#pragma once

// Host context passed to yams_plugin_init, plus the resource service. Kept
// apart from host_services_v1.h so plugins can include it next to other
// plugin headers (content_extractor_v1.h) without duplicate typedefs.

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Version 2 adds resource_service. New fields go at the end of the context;
// plugins read a field only when `version` is at least the one that added it.
#define YAMS_PLUGIN_HOST_SERVICES_API_VERSION 2

// Forward declarations for C-style service structs
typedef struct yams_document_service_v1 yams_document_service_v1;
typedef struct yams_search_service_v1 yams_search_service_v1;
typedef struct yams_grep_service_v1 yams_grep_service_v1;
typedef struct yams_resource_service_v1 yams_resource_service_v1;

// Main context struct passed to plugins
typedef struct yams_plugin_host_context_v1 {
    uint32_t version;
    void* impl; // Opaque pointer to host-side implementation
    yams_document_service_v1* document_service;
    yams_search_service_v1* search_service;
    yams_grep_service_v1* grep_service;
    // Version 2+. May be null.
    yams_resource_service_v1* resource_service;
} yams_plugin_host_context_v1;

// --- Resource Service C-ABI (host services version 2) ---
// Grants from the host's resource governor. Ask once per unit of work and stay
// within the answer: grants shrink as host memory pressure rises.

typedef struct {
    // Threads one document's extraction may use. At least 1.
    uint32_t max_threads;
    uint32_t reserved;
    // Bytes one document's extraction may hold decompressed at once, summed
    // over its threads. 0 means no byte grant.
    uint64_t max_decompressed_bytes;
} yams_extraction_budget_v1;

struct yams_resource_service_v1 {
    void* handle;
    // Fills *out and returns 0, or returns nonzero when no grant is available
    // (the plugin then uses its own defaults).
    int (*get_extraction_budget)(void* handle, yams_extraction_budget_v1* out);
};

#ifdef __cplusplus
}
#endif

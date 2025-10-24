#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_IFACE_ONNX_REQUEST_V1 "onnx_request_v1"
#define YAMS_IFACE_ONNX_REQUEST_V1_VERSION 1u

// Generic JSON request/response interface for ONNX plugin
// Request JSON example:
// {
//   "model_id": "text-embed-e5-small",
//   "task": "embedding",           // future: sentiment|ner|classify
//   "inputs": ["text1", "text2"], // array of UTF-8 strings
//   "options": {"device":"cpu", "batch":16}
// }
// Response JSON example (embedding):
// {"dim":384, "batch":2, "vectors":[[...],[...]]}

typedef struct yams_onnx_request_v1 {
    uint32_t abi_version; // must be YAMS_IFACE_ONNX_REQUEST_V1_VERSION
    void* self;           // opaque

    // Process a request and return a JSON string allocated by the plugin
    // Caller must free via free_string.
    int (*process_json)(void* self, const char* request_json, char** out_json);

    void (*free_string)(void* self, char* s);
} yams_onnx_request_v1;

#ifdef __cplusplus
}
#endif

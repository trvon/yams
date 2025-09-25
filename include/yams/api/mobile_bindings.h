#pragma once

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
extern "C" {
#else
#include <stddef.h>
#include <stdint.h>
#endif

#ifndef YAMS_MOBILE_API
#if defined(_WIN32) && !defined(YAMS_STATIC)
#ifdef yams_mobile_EXPORTS
#define YAMS_MOBILE_API __declspec(dllexport)
#else
#define YAMS_MOBILE_API __declspec(dllimport)
#endif
#else
#define YAMS_MOBILE_API __attribute__((visibility("default")))
#endif
#endif

/**
 * ABI version for mobile bindings. Increment MAJOR on breaking changes.
 */
typedef struct yams_mobile_version_info {
    uint16_t major;
    uint16_t minor;
    uint16_t patch;
} yams_mobile_version_info;

/**
 * Status codes returned by the mobile bindings.
 */
typedef enum yams_mobile_status {
    YAMS_MOBILE_STATUS_OK = 0,
    YAMS_MOBILE_STATUS_INVALID_ARGUMENT = 1,
    YAMS_MOBILE_STATUS_NOT_INITIALIZED = 2,
    YAMS_MOBILE_STATUS_INTERNAL_ERROR = 3,
    YAMS_MOBILE_STATUS_TIMEOUT = 4,
    YAMS_MOBILE_STATUS_UNAVAILABLE = 5,
    YAMS_MOBILE_STATUS_NOT_FOUND = 6,
    YAMS_MOBILE_STATUS_UNKNOWN = 255
} yams_mobile_status;

/**
 * Opaque context handle representing a configured search/grep environment.
 */
typedef struct yams_mobile_context_t yams_mobile_context_t;

typedef struct yams_mobile_context_config {
    const char* working_directory; /**< Root path for metadata/storage; UTF-8, nullable */
    const char* cache_directory;   /**< Optional cache directory override */
    const char* telemetry_sink;    /**< Optional sink identifier (e.g., "console", "noop") */
    uint32_t max_worker_threads;   /**< 0 => auto */
    uint32_t flags;                /**< Reserved for future feature toggles */
} yams_mobile_context_config;

/**
 * Basic request envelope shared by grep and search flows.
 */
typedef struct yams_mobile_request_header {
    const char* correlation_id; /**< Optional identifier for tracing */
    uint32_t timeout_ms;        /**< 0 => default */
} yams_mobile_request_header;

/**
 * Represents a UTF-8 string slice owned by the callee.
 */
typedef struct yams_mobile_string_view {
    const char* data;
    size_t length;
} yams_mobile_string_view;

/**
 * Grep parameters (subset of CLI options).
 */
typedef struct yams_mobile_grep_request {
    yams_mobile_request_header header;
    const char* pattern;
    uint8_t literal;
    uint8_t ignore_case;
    uint8_t word_boundary;
    uint32_t max_matches;
} yams_mobile_grep_request;

/**
 * Grep result handle; caller must dispose via `yams_mobile_grep_result_destroy`.
 */
typedef struct yams_mobile_grep_result_t yams_mobile_grep_result_t;

/**
 * Search parameters (hybrid/semantic options to be expanded).
 */
typedef struct yams_mobile_search_request {
    yams_mobile_request_header header;
    const char* query;
    uint32_t limit;
    const char** tags;
    size_t tag_count;
    uint8_t paths_only;
    uint8_t semantic;
} yams_mobile_search_request;

typedef struct yams_mobile_search_result_t yams_mobile_search_result_t;

typedef struct yams_mobile_document_store_request {
    yams_mobile_request_header header;
    const char* path;
    const char** tags;
    size_t tag_count;
    uint8_t sync_now;
} yams_mobile_document_store_request;

typedef struct yams_mobile_metadata_request {
    yams_mobile_request_header header;
    const char* document_hash;
    const char* path;
} yams_mobile_metadata_request;

typedef struct yams_mobile_metadata_result_t yams_mobile_metadata_result_t;

typedef struct yams_mobile_vector_status_request {
    yams_mobile_request_header header;
    uint8_t warmup;
} yams_mobile_vector_status_request;

typedef struct yams_mobile_vector_status_result_t yams_mobile_vector_status_result_t;

/**
 * Retrieve the compiled ABI version.
 */
YAMS_MOBILE_API yams_mobile_version_info yams_mobile_get_version(void);

/**
 * Create/destroy the shared mobile context.
 */
YAMS_MOBILE_API yams_mobile_status yams_mobile_context_create(
    const yams_mobile_context_config* config, yams_mobile_context_t** out_context);

YAMS_MOBILE_API void yams_mobile_context_destroy(yams_mobile_context_t* ctx);

/**
 * Execute grep/search operations. Results require explicit destroy calls.
 */
YAMS_MOBILE_API yams_mobile_status yams_mobile_grep_execute(yams_mobile_context_t* ctx,
                                                            const yams_mobile_grep_request* request,
                                                            yams_mobile_grep_result_t** out_result);

YAMS_MOBILE_API void yams_mobile_grep_result_destroy(yams_mobile_grep_result_t* result);

YAMS_MOBILE_API yams_mobile_status
yams_mobile_search_execute(yams_mobile_context_t* ctx, const yams_mobile_search_request* request,
                           yams_mobile_search_result_t** out_result);

YAMS_MOBILE_API void yams_mobile_search_result_destroy(yams_mobile_search_result_t* result);

YAMS_MOBILE_API yams_mobile_status yams_mobile_store_document(
    yams_mobile_context_t* ctx, const yams_mobile_document_store_request* request,
    yams_mobile_string_view* out_hash);

YAMS_MOBILE_API yams_mobile_status yams_mobile_remove_document(yams_mobile_context_t* ctx,
                                                               const char* document_hash);

YAMS_MOBILE_API yams_mobile_status
yams_mobile_get_metadata(yams_mobile_context_t* ctx, const yams_mobile_metadata_request* request,
                         yams_mobile_metadata_result_t** out_result);

YAMS_MOBILE_API void yams_mobile_metadata_result_destroy(yams_mobile_metadata_result_t* result);

YAMS_MOBILE_API yams_mobile_status yams_mobile_get_vector_status(
    yams_mobile_context_t* ctx, const yams_mobile_vector_status_request* request,
    yams_mobile_vector_status_result_t** out_result);

YAMS_MOBILE_API void
yams_mobile_vector_status_result_destroy(yams_mobile_vector_status_result_t* result);

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_metadata_result_json(const yams_mobile_metadata_result_t* result);

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_vector_status_result_json(const yams_mobile_vector_status_result_t* result);

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_grep_result_stats_json(const yams_mobile_grep_result_t* result);

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_search_result_stats_json(const yams_mobile_search_result_t* result);

/**
 * Fetch extended error information for the last API call on the invoking thread.
 * Returns a pointer valid until the next API invocation on the same thread.
 */
YAMS_MOBILE_API const char* yams_mobile_last_error_message(void);

#ifdef __cplusplus
} // extern "C"
#endif

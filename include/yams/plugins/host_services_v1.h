#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_PLUGIN_HOST_SERVICES_API_VERSION 1

// Forward declarations for C-style service structs
typedef struct yams_document_service_v1 yams_document_service_v1;
typedef struct yams_search_service_v1 yams_search_service_v1;
typedef struct yams_grep_service_v1 yams_grep_service_v1;

// Main context struct passed to plugins
typedef struct yams_plugin_host_context_v1 {
    uint32_t version;
    void* impl; // Opaque pointer to host-side implementation
    yams_document_service_v1* document_service;
    yams_search_service_v1* search_service;
    yams_grep_service_v1* grep_service;
    // Add other services here in the future
} yams_plugin_host_context_v1;

// --- Generic Utility Structs ---

typedef struct {
    char** strings;
    size_t count;
} yams_string_list_t;

typedef struct {
    char* key;
    char* value;
} yams_key_value_pair_t;

typedef struct {
    yams_key_value_pair_t* pairs;
    size_t count;
} yams_key_value_map_t;

// --- Document Service C-ABI ---

typedef struct {
    const char* path;
    const char* content;
    size_t content_len;
    const char* name;
    const char* mime_type;
    bool disable_auto_mime;
    yams_string_list_t tags;
    yams_key_value_map_t metadata;
    const char* collection;
    const char* snapshot_id;
    const char* snapshot_label;
    bool no_embeddings;
} yams_store_document_request_t;

typedef struct {
    char* hash;
    uint64_t bytes_stored;
    uint64_t bytes_deduped;
} yams_store_document_response_t;

typedef struct {
    const char* hash;
    const char* name;
    const char* output_path;
    bool include_content;
    bool graph;
    int depth;
} yams_retrieve_document_request_t;

typedef struct {
    char* hash;
    char* path;
    char* name;
    uint64_t size;
    char* mime_type;
    char* content; // Null if not requested
} yams_retrieved_document_t;

typedef struct {
    char* hash;
    char* path;
    int distance;
} yams_related_document_t;

typedef struct {
    yams_retrieved_document_t* document; // Can be null
    yams_related_document_t* related;
    size_t related_count;
} yams_retrieve_document_response_t;

typedef struct {
    int limit;
    int offset;
    const char* pattern;
    yams_string_list_t tags;
    bool match_all_tags;
    const char* sort_by;
    const char* sort_order;
    bool paths_only;
} yams_list_documents_request_t;

typedef struct {
    char* name;
    char* hash;
    char* path;
    uint64_t size;
    char* mime_type;
    int64_t indexed;
    yams_string_list_t tags;
    char* snippet;
} yams_document_entry_t;

typedef struct {
    yams_document_entry_t* documents;
    size_t count;
    size_t total_found;
    char** paths; // if paths_only was true
    size_t paths_count;
} yams_list_documents_response_t;

typedef struct {
    const char* hash;
    const char* name;
} yams_cat_document_request_t;

typedef struct {
    char* content;
    size_t size;
    char* hash;
    char* name;
} yams_cat_document_response_t;

// --- Search Service C-ABI ---

typedef struct yams_search_response_s yams_search_response_t;

typedef void (*yams_search_callback_t)(yams_search_response_t* result, void* user_data);

typedef struct {
    const char* query;
    size_t limit;
    bool fuzzy;
    float similarity;
    const char* hash;
    const char* type;
    bool paths_only;
} yams_search_request_t;

typedef struct {
    char* hash;
    char* title;
    char* path;
    double score;
    char* snippet;
} yams_search_item_t;

typedef struct yams_search_response_s {
    size_t total;
    char* type;
    yams_search_item_t* results;
    size_t results_count;
    char** paths;
    size_t paths_count;
} yams_search_response_t;

// --- Grep Service C-ABI ---

typedef struct {
    const char* pattern;
    yams_string_list_t paths;
    bool ignore_case;
    bool word;
    bool invert;
} yams_grep_request_t;

typedef struct {
    char* line;
    size_t line_number;
} yams_grep_match_t;

typedef struct {
    char* file;
    yams_grep_match_t* matches;
    size_t match_count;
} yams_grep_file_result_t;

typedef struct {
    yams_grep_file_result_t* results;
    size_t count;
} yams_grep_response_t;

// --- Service V-tables ---

struct yams_document_service_v1 {
    void* handle;
    int (*store)(void* handle, const yams_store_document_request_t* req,
                 yams_store_document_response_t** res);
    int (*retrieve)(void* handle, const yams_retrieve_document_request_t* req,
                    yams_retrieve_document_response_t** res);
    int (*list)(void* handle, const yams_list_documents_request_t* req,
                yams_list_documents_response_t** res);
    int (*cat)(void* handle, const yams_cat_document_request_t* req,
               yams_cat_document_response_t** res);
    void (*free_store_document_response)(yams_store_document_response_t* res);
    void (*free_retrieve_document_response)(yams_retrieve_document_response_t* res);
    void (*free_list_documents_response)(yams_list_documents_response_t* res);
    void (*free_cat_document_response)(yams_cat_document_response_t* res);
};

struct yams_search_service_v1 {
    void* handle;
    int (*search)(void* handle, const yams_search_request_t* req, yams_search_response_t** res);
    void (*free_search_response)(yams_search_response_t* res);
};

struct yams_grep_service_v1 {
    void* handle;
    int (*grep)(void* handle, const yams_grep_request_t* req, yams_grep_response_t** res);
    void (*free_grep_response)(yams_grep_response_t* res);
};

#ifdef __cplusplus
}
#endif
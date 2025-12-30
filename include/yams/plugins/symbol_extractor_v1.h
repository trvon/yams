#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Interface identity
#define YAMS_IFACE_SYMBOL_EXTRACTOR_V1 "symbol_extractor_v1"
#define YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION 1u

// Symbol information structure
typedef struct yams_symbol_v1 {
    char* name;           // Symbol name (e.g., "processTask")
    char* qualified_name; // Fully qualified name (e.g., "yams::daemon::processTask")
    char* kind;      // Symbol kind: "function", "class", "method", "variable", "macro", "typedef"
    char* file_path; // Absolute path to source file
    char* scope;     // Enclosing scope (e.g., "yams::daemon::GraphComponent", nullable)
    uint32_t start_line;   // 1-based line number
    uint32_t end_line;     // 1-based line number
    uint32_t start_offset; // Byte offset from file start
    uint32_t end_offset;   // Byte offset from file start
    char* return_type;     // Return type for functions/methods (nullable)
    char** parameters;     // Array of parameter type strings (nullable)
    size_t parameter_count;
    char* documentation; // Extracted documentation/comment (nullable)
} yams_symbol_v1;

// Symbol relationship structure
typedef struct yams_symbol_relation_v1 {
    char* src_symbol; // Source symbol qualified name
    char* dst_symbol; // Destination symbol qualified name
    char* kind;       // Relationship: "calls", "inherits", "includes", "defines", "uses"
    double weight;    // Relationship strength (0.0-1.0)
} yams_symbol_relation_v1;

// Symbol extraction result
typedef struct yams_symbol_extraction_result_v1 {
    yams_symbol_v1* symbols;
    size_t symbol_count;
    yams_symbol_relation_v1* relations;
    size_t relation_count;
    char* error; // Null on success, error message on failure
} yams_symbol_extraction_result_v1;

// Symbol extractor v-table following YAMS plugin pattern
typedef struct yams_symbol_extractor_v1 {
    uint32_t abi_version; // Must be YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION
    void* self;           // Opaque plugin context

    // Check if extractor supports the given language
    bool (*supports_language)(void* self, const char* language);

    // Extract symbols and relationships from file content
    // content: File content as UTF-8 string
    // content_len: Length of content in bytes
    // file_path: Absolute path to source file (for error reporting)
    // language: Programming language ("cpp", "python", "rust", etc.)
    // result: Output extraction result (owned by caller, free with free_result)
    // Returns 0 on success, non-zero on failure
    int (*extract_symbols)(void* self, const char* content, size_t content_len,
                           const char* file_path, const char* language,
                           yams_symbol_extraction_result_v1** result);

    // Free extraction result memory
    void (*free_result)(void* self, yams_symbol_extraction_result_v1* result);

    // Get extractor capabilities as JSON string
    // Returns supported languages, features, etc. (caller frees with free_string)
    int (*get_capabilities_json)(void* self, char** out_json);

    // Free string allocated by plugin
    void (*free_string)(void* self, char* s);
} yams_symbol_extractor_v1;

#ifdef __cplusplus
}
#endif

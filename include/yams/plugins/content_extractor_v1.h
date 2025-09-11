#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_IFACE_CONTENT_EXTRACTOR_V1_ID "content_extractor_v1"
#define YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION 1

// Represents a key-value map for metadata
typedef struct {
    char* key;
    char* value;
} yams_key_value_pair_t;

typedef struct {
    yams_key_value_pair_t* pairs;
    size_t count;
} yams_metadata_map_t;

// The result of an extraction operation
typedef struct {
    char* text;                   // Extracted text content, null on failure
    yams_metadata_map_t metadata; // Extracted metadata
    char* error;                  // Null on success, error message on failure
} yams_extraction_result_t;

// The content extractor interface v-table
typedef struct {
    uint32_t abi_version;

    // Checks if the extractor can handle the given file type.
    // mime_type: The MIME type of the file (e.g., "application/pdf").
    // extension: The file extension (e.g., ".pdf").
    // Returns true if the extractor can handle this file type, false otherwise.
    bool (*supports)(const char* mime_type, const char* extension);

    // Extracts text and metadata from a byte buffer.
    // content: A pointer to the byte buffer.
    // content_len: The length of the buffer.
    // result: A pointer to a pointer that will be populated with the extraction result.
    //         The caller is responsible for freeing the result with free_result().
    // Returns 0 on success, non-zero on failure.
    int (*extract)(const uint8_t* content, size_t content_len, yams_extraction_result_t** result);

    // Frees the memory allocated for an extraction result.
    void (*free_result)(yams_extraction_result_t* result);

} yams_content_extractor_v1;

#ifdef __cplusplus
}
#endif
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_IFACE_ENTITY_EXTRACTOR_V2 "entity_extractor_v2"
#define YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION 2u

typedef struct yams_entity_v2 {
    char* text;
    char* type;
    char* qualified_name;
    char* scope;
    uint32_t start_offset;
    uint32_t end_offset;
    uint32_t start_line;
    uint32_t end_line;
    float confidence;
    char* properties_json;
} yams_entity_v2;

typedef struct yams_entity_relation_v2 {
    char* src_entity;
    char* dst_entity;
    char* relation_type;
    float weight;
} yams_entity_relation_v2;

typedef struct yams_entity_extraction_result_v2 {
    yams_entity_v2* entities;
    size_t entity_count;
    yams_entity_relation_v2* relations;
    size_t relation_count;
    char* error;
} yams_entity_extraction_result_v2;

typedef struct yams_entity_extraction_options_v2 {
    const char* const* entity_types;
    size_t entity_type_count;
    const char* language;
    const char* file_path;
    bool extract_relations;
} yams_entity_extraction_options_v2;

typedef struct yams_entity_extractor_v2 {
    uint32_t abi_version;
    void* self;

    bool (*supports)(void* self, const char* content_type);

    int (*extract)(void* self, const char* content, size_t content_len,
                   const yams_entity_extraction_options_v2* options,
                   yams_entity_extraction_result_v2** result);

    void (*free_result)(void* self, yams_entity_extraction_result_v2* result);

    int (*get_capabilities_json)(void* self, char** out_json);

    void (*free_string)(void* self, char* s);
} yams_entity_extractor_v2;

#ifdef __cplusplus
}
#endif

#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "pdf_extractor.h"
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

// --- Helper Functions ---

static char* c_string_from_cpp(const std::string& s) {
    if (s.empty()) {
        return nullptr;
    }
    char* c_str = (char*)malloc(s.length() + 1);
    if (c_str) {
        memcpy(c_str, s.c_str(), s.length() + 1);
    }
    return c_str;
}

// --- content_extractor_v1 implementation ---

bool supports(const char* mime_type, const char* extension) {
    if (mime_type && (strcmp(mime_type, "application/pdf") == 0)) {
        return true;
    }
    if (extension && (strcmp(extension, ".pdf") == 0)) {
        return true;
    }
    return false;
}

int extract(const uint8_t* content, size_t content_len, yams_extraction_result_t** result) {
    if (!content || content_len == 0 || !result) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    auto extractor = std::make_unique<yams::extraction::PdfExtractor>();
    std::span<const std::byte> buffer(reinterpret_cast<const std::byte*>(content), content_len);

    yams::extraction::ExtractionConfig config;
    auto extraction_result = extractor->extractFromBuffer(buffer, config);

    *result = (yams_extraction_result_t*)malloc(sizeof(yams_extraction_result_t));
    if (!*result) {
        return YAMS_PLUGIN_ERR_INVALID; // Out of memory
    }
    memset(*result, 0, sizeof(yams_extraction_result_t));

    if (!extraction_result) {
        (*result)->error = c_string_from_cpp(extraction_result.error().message);
        return YAMS_PLUGIN_ERR_INVALID;
    }

    (*result)->text = c_string_from_cpp(extraction_result.value().text);

    // Convert metadata
    const auto& metadata = extraction_result.value().metadata;
    (*result)->metadata.count = metadata.size();
    (*result)->metadata.pairs =
        (yams_key_value_pair_t*)malloc(sizeof(yams_key_value_pair_t) * metadata.size());
    if ((*result)->metadata.pairs) {
        int i = 0;
        for (const auto& pair : metadata) {
            (*result)->metadata.pairs[i].key = c_string_from_cpp(pair.first);
            (*result)->metadata.pairs[i].value = c_string_from_cpp(pair.second);
            i++;
        }
    }

    return YAMS_PLUGIN_OK;
}

void free_result(yams_extraction_result_t* result) {
    if (!result)
        return;
    free(result->text);
    free(result->error);
    if (result->metadata.pairs) {
        for (size_t i = 0; i < result->metadata.count; ++i) {
            free(result->metadata.pairs[i].key);
            free(result->metadata.pairs[i].value);
        }
        free(result->metadata.pairs);
    }
    free(result);
}

static yams_content_extractor_v1 content_extractor_iface = {
    .abi_version = 2, .supports = supports, .extract = extract, .free_result = free_result};

// --- Plugin ABI Implementation ---

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

const char* yams_plugin_get_name(void) {
    return "pdf_extractor";
}

const char* yams_plugin_get_version(void) {
    return "1.0.0";
}

const char* yams_plugin_get_manifest_json(void) {
    // This could be read from the file, but for simplicity, we'll hardcode it.
    return R"({
      "name": "pdf_extractor",
      "version": "1.0.0",
      "abi": 1,
      "interfaces": [
        {
          "id": "content_extractor_v1",
          "version": 2
        }
      ]
    })";
}

int yams_plugin_init(const char* config_json, const void* host_context) {
    // This plugin doesn't require any configuration or host services yet.
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}

void yams_plugin_shutdown(void) {
    // No-op
}

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (!iface_id || !out_iface) {
        return YAMS_PLUGIN_ERR_INVALID;
    }
    *out_iface = nullptr;

    if (strcmp(iface_id, YAMS_IFACE_CONTENT_EXTRACTOR_V1_ID) == 0) {
        if (version == 2 || version == YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION) {
            *out_iface = &content_extractor_iface;
            return YAMS_PLUGIN_OK;
        }
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

int yams_plugin_get_health_json(char** out_json) {
    // This plugin doesn't have a health check.
    (void)out_json;
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

} // extern "C"

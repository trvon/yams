/**
 * @file zyp_plugin.cpp
 * @brief YAMS plugin ABI implementation for zyp (Zig YAMS PDF)
 *
 * This implements the content_extractor_v1 interface using zpdf for
 * high-performance PDF text extraction.
 */

#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

#include "pdf_metadata.h"
#include "zpdf_wrapper.h"

#include <cstdlib>
#include <cstring>

namespace {

/**
 * Duplicate a C++ string to a malloc'd C string.
 */
char* dupString(const std::string& s) {
    if (s.empty()) {
        return nullptr;
    }
    char* result = static_cast<char*>(std::malloc(s.size() + 1));
    if (result) {
        std::memcpy(result, s.c_str(), s.size() + 1);
    }
    return result;
}

/**
 * Check if this extractor supports the given MIME type or extension.
 */
bool supports(const char* mime_type, const char* extension) {
    if (mime_type && std::strcmp(mime_type, "application/pdf") == 0) {
        return true;
    }
    if (extension) {
        if (std::strcmp(extension, ".pdf") == 0 || std::strcmp(extension, "pdf") == 0) {
            return true;
        }
    }
    return false;
}

/**
 * Extract text and metadata from a PDF buffer.
 */
int extract(const uint8_t* content, size_t content_len, yams_extraction_result_t** result) {
    if (!content || content_len == 0 || !result) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    *result = nullptr;

    // Open document from memory
    std::span<const uint8_t> buffer(content, content_len);
    auto doc = yams::zyp::Document::openMemory(buffer);
    if (!doc) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    // Extract text using parallel processing for best performance
    auto text = doc->extractAllParallel();

    // Extract metadata
    auto metadata = yams::zyp::extractMetadata(buffer);
    auto metaMap = metadata ? metadata->toMap() : std::unordered_map<std::string, std::string>{};

    // Add page count
    metaMap["page_count"] = std::to_string(doc->pageCount());

    // Allocate result structure
    *result = static_cast<yams_extraction_result_t*>(std::malloc(sizeof(yams_extraction_result_t)));
    if (!*result) {
        return YAMS_PLUGIN_ERR_INVALID;
    }
    std::memset(*result, 0, sizeof(yams_extraction_result_t));

    // Copy text
    if (text && !text.empty()) {
        auto textView = text.text();
        (*result)->text = static_cast<char*>(std::malloc(textView.size() + 1));
        if ((*result)->text) {
            std::memcpy((*result)->text, textView.data(), textView.size());
            (*result)->text[textView.size()] = '\0';
        }
    } else {
        // Empty text
        (*result)->text = static_cast<char*>(std::malloc(1));
        if ((*result)->text) {
            (*result)->text[0] = '\0';
        }
    }

    // Populate metadata
    if (!metaMap.empty()) {
        (*result)->metadata.count = metaMap.size();
        (*result)->metadata.pairs = static_cast<yams_key_value_pair_t*>(
            std::malloc(sizeof(yams_key_value_pair_t) * metaMap.size()));

        if ((*result)->metadata.pairs) {
            size_t i = 0;
            for (const auto& [key, value] : metaMap) {
                (*result)->metadata.pairs[i].key = dupString(key);
                (*result)->metadata.pairs[i].value = dupString(value);
                ++i;
            }
        }
    }

    return YAMS_PLUGIN_OK;
}

/**
 * Free an extraction result.
 */
void free_result(yams_extraction_result_t* result) {
    if (!result) {
        return;
    }

    std::free(result->text);
    std::free(result->error);

    if (result->metadata.pairs) {
        for (size_t i = 0; i < result->metadata.count; ++i) {
            std::free(result->metadata.pairs[i].key);
            std::free(result->metadata.pairs[i].value);
        }
        std::free(result->metadata.pairs);
    }

    std::free(result);
}

} // anonymous namespace

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

const char* yams_plugin_get_name(void) {
    return "zyp";
}

const char* yams_plugin_get_version(void) {
    return "1.0.0";
}

const char* yams_plugin_get_manifest_json(void) {
    return "{"
           "\"name\": \"zyp\","
           "\"version\": \"1.0.0\","
           "\"abi\": 1,"
           "\"interfaces\": [{\"id\": \"content_extractor_v1\", \"version\": 1}],"
           "\"description\": \"High-performance PDF text extraction using zpdf\","
           "\"health\": {\"has_health\": false}"
           "}";
}

int yams_plugin_init(const char* config_json, const void* host_context) {
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}

void yams_plugin_shutdown(void) {
    // No cleanup needed
}

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (!iface_id || !out_iface) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    *out_iface = nullptr;

    if (std::strcmp(iface_id, YAMS_IFACE_CONTENT_EXTRACTOR_V1_ID) == 0 &&
        version == YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION) {
        static yams_content_extractor_v1 iface = {
            .abi_version = YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION,
            .supports = supports,
            .extract = extract,
            .free_result = free_result,
        };
        *out_iface = &iface;
        return YAMS_PLUGIN_OK;
    }

    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

int yams_plugin_get_health_json(char** out_json) {
    (void)out_json;
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

} // extern "C"

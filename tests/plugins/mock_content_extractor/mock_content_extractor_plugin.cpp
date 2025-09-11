// Minimal content_extractor_v1 plugin for tests
#include <cstdlib>
#include <cstring>
#include <string>
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}
const char* yams_plugin_get_name(void) {
    return "mock_content_extractor";
}
const char* yams_plugin_get_version(void) {
    return "0.0.1";
}
static const char* k_manifest = "{\"name\":\"mock_content_extractor\",\"version\":\"0.0.1\","
                                "\"interfaces\":[{\"id\":\"content_extractor_v1\",\"version\":1}]}";
const char* yams_plugin_get_manifest_json(void) {
    return k_manifest;
}
int yams_plugin_init(const char* config_json, const void* host_context) {
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}
void yams_plugin_shutdown(void) {}

bool supports(const char* mime_type, const char* extension) {
    if (mime_type && (strcmp(mime_type, "application/x-mock") == 0)) {
        return true;
    }
    if (extension && (strcmp(extension, ".mock") == 0)) {
        return true;
    }
    return false;
}

int extract(const uint8_t* content, size_t content_len, yams_extraction_result_t** result) {
    if (!content || content_len == 0 || !result) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    *result = (yams_extraction_result_t*)malloc(sizeof(yams_extraction_result_t));
    if (!*result) {
        return YAMS_PLUGIN_ERR_INVALID; // Out of memory
    }
    memset(*result, 0, sizeof(yams_extraction_result_t));

    const char* k = "MOCK_EXTRACTED_TEXT";
    size_t n = std::strlen(k);
    char* buf = static_cast<char*>(std::malloc(n + 1));
    if (!buf) {
        free(*result);
        *result = nullptr;
        return YAMS_PLUGIN_ERR_INVALID;
    }
    std::memcpy(buf, k, n + 1);
    (*result)->text = buf;

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

static yams_content_extractor_v1 g_table = {.abi_version = YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION,
                                            .supports = supports,
                                            .extract = extract,
                                            .free_result = free_result};

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (!iface_id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (version == 1 && std::string(iface_id) == "content_extractor_v1") {
        *out_iface = reinterpret_cast<void*>(&g_table);
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    static const char* k = "{\"status\":\"ok\"}";
    size_t n = std::strlen(k);
    char* buf = static_cast<char*>(std::malloc(n + 1));
    if (!buf)
        return YAMS_PLUGIN_ERR_INVALID;
    std::memcpy(buf, k, n + 1);
    *out_json = buf;
    return YAMS_PLUGIN_OK;
}
}
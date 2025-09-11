#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/model_provider_v1.h>
}

#include "model_provider.h"

static const char* kManifestJson = R"JSON({
  "name": "onnx",
  "version": "0.1.0",
  "interfaces": [
    {"id": "model_provider_v1", "version": 1}
  ]
})JSON";

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

const char* yams_plugin_get_name(void) {
    return "onnx";
}

const char* yams_plugin_get_version(void) {
    return "0.1.0";
}

const char* yams_plugin_get_manifest_json(void) {
    return kManifestJson;
}

int yams_plugin_init(const char* /*config_json*/, const void* /*host_context*/) {
    return YAMS_PLUGIN_OK;
}

void yams_plugin_shutdown(void) {}

int yams_plugin_get_interface(const char* id, uint32_t version, void** out_iface) {
    if (!id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (version == YAMS_IFACE_MODEL_PROVIDER_V1_VERSION &&
        std::strcmp(id, YAMS_IFACE_MODEL_PROVIDER_V1) == 0) {
        *out_iface = static_cast<void*>(yams_onnx_get_model_provider());
        return (*out_iface) ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_NOT_FOUND;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    static const char* kHealth = "{\"status\":\"ok\"}";
    size_t n = std::strlen(kHealth);
    char* buf = static_cast<char*>(std::malloc(n + 1));
    if (!buf)
        return YAMS_PLUGIN_ERR_INVALID;
    std::memcpy(buf, kHealth, n + 1);
    *out_json = buf;
    return YAMS_PLUGIN_OK;
}

} // extern "C"

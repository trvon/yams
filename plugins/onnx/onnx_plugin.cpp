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
    {"id": "model_provider_v1", "version": 2}
  ]
})JSON"; // ABI v1.2 (model_provider_v1 version=2)

// Lightweight runtime flags
static bool g_plugin_disabled = false; // set by env at init

// Forward-declared helper exposed by model_provider.cpp
extern "C" const char* yams_onnx_get_health_json_cstr();

extern "C" {

YAMS_PLUGIN_API int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

YAMS_PLUGIN_API const char* yams_plugin_get_name(void) {
    return "onnx";
}

YAMS_PLUGIN_API const char* yams_plugin_get_version(void) {
    return "0.1.0";
}

YAMS_PLUGIN_API const char* yams_plugin_get_manifest_json(void) {
    return kManifestJson;
}

YAMS_PLUGIN_API int yams_plugin_init(const char* /*config_json*/, const void* /*host_context*/) {
    // Allow disabling the plugin without removing it from disk
    if (const char* d = std::getenv("YAMS_ONNX_PLUGIN_DISABLE"); d && *d) {
        g_plugin_disabled = true;
    }
    return YAMS_PLUGIN_OK;
}

YAMS_PLUGIN_API void yams_plugin_shutdown(void) { /* nothing to do */ }

YAMS_PLUGIN_API int yams_plugin_get_interface(const char* id, uint32_t version, void** out_iface) {
    if (!id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (g_plugin_disabled)
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    // Accept any version >= 1 up to our max (2), to avoid CLI hangs on minor mismatches
    if ((version >= 1 && version <= 2) && std::strcmp(id, YAMS_IFACE_MODEL_PROVIDER_V1) == 0) {
        *out_iface = static_cast<void*>(yams_onnx_get_model_provider());
        return (*out_iface) ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_NOT_FOUND;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    const char* src = yams_onnx_get_health_json_cstr();
    if (!src)
        src = "{\"status\":\"unknown\"}";
    size_t n = std::strlen(src);
    char* buf = static_cast<char*>(std::malloc(n + 1));
    if (!buf)
        return YAMS_PLUGIN_ERR_INVALID;
    std::memcpy(buf, src, n + 1);
    *out_json = buf;
    return YAMS_PLUGIN_OK;
}

} // extern "C"

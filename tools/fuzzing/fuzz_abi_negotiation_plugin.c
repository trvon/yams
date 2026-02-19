#include <stdlib.h>
#include <string.h>

#include <yams/plugins/abi.h>

static const char* kName = "yams_fuzz_abi_negotiation";
static const char* kVersion = "0.0.1";
static const char* kManifest = "{"
                               "\"name\":\"yams_fuzz_abi_negotiation\","
                               "\"version\":\"0.0.1\","
                               "\"abi\":1,"
                               "\"interfaces\":["
                               "{\"id\":\"fuzz_iface_v1\",\"version\":1},"
                               "{\"id\":\"content_extractor_v1\",\"version\":1},"
                               "{\"id\":\"dr_provider_v1\",\"version\":1}"
                               "],"
                               "\"capabilities\":[\"test\",\"negotiation\"]"
                               "}";

struct IfaceV1 {
    int (*ping)(int x);
};

static int ping1(int x) {
    return x + 1;
}

static struct IfaceV1 kIface = {ping1};

YAMS_PLUGIN_API int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

YAMS_PLUGIN_API const char* yams_plugin_get_name(void) {
    return kName;
}

YAMS_PLUGIN_API const char* yams_plugin_get_version(void) {
    return kVersion;
}

YAMS_PLUGIN_API const char* yams_plugin_get_manifest_json(void) {
    return kManifest;
}

YAMS_PLUGIN_API int yams_plugin_init(const char* config_json, const void* host_context) {
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}

YAMS_PLUGIN_API void yams_plugin_shutdown(void) {}

YAMS_PLUGIN_API int yams_plugin_get_interface(const char* iface_id, uint32_t version,
                                              void** out_iface) {
    if (!iface_id || !out_iface) {
        return YAMS_PLUGIN_ERR_INVALID;
    }

    if (version == 1 &&
        (strcmp(iface_id, "fuzz_iface_v1") == 0 || strcmp(iface_id, "content_extractor_v1") == 0 ||
         strcmp(iface_id, "dr_provider_v1") == 0)) {
        *out_iface = &kIface;
        return YAMS_PLUGIN_OK;
    }

    *out_iface = NULL;
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json) {
    if (!out_json) {
        return YAMS_PLUGIN_ERR_INVALID;
    }
    const char* payload = "{\"ok\":true,\"mode\":\"negotiation\"}";
    size_t n = strlen(payload) + 1;
    char* p = (char*)malloc(n);
    if (!p) {
        return YAMS_PLUGIN_ERR_INIT_FAILED;
    }
    memcpy(p, payload, n);
    *out_json = p;
    return YAMS_PLUGIN_OK;
}

#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/model_provider_v1.h>
#include <yams/plugins/onnx_request_v1.h>
}

#include <nlohmann/json.hpp>
#include "model_provider.h"

static const char* kManifestJson = R"JSON({
  "name": "onnx",
  "version": "0.2.0",
  "interfaces": [
    {"id": "model_provider_v1", "version": 4},
    {"id": "onnx_request_v1", "version": 1}
  ]
})JSON"; // model_provider_v1 v4 + onnx_request_v1

// Lightweight runtime flags
static bool g_plugin_disabled = false; // set by env at init

// Forward-declared helpers exposed by model_provider.cpp
extern "C" const char* yams_onnx_get_health_json_cstr();
extern "C" void yams_onnx_set_config_json(const char* json);

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

YAMS_PLUGIN_API int yams_plugin_init(const char* config_json, const void* /*host_context*/) {
    // Store the config JSON for later use by ProviderCtx
    if (config_json && *config_json) {
        yams_onnx_set_config_json(config_json);
    }

    // Allow disabling the plugin without removing it from disk
    // Check multiple env vars that indicate vectors/ONNX should be disabled
    if (const char* d = std::getenv("YAMS_ONNX_PLUGIN_DISABLE"); d && *d) {
        g_plugin_disabled = true;
    }
    if (const char* d = std::getenv("YAMS_DISABLE_VECTORS"); d && *d) {
        g_plugin_disabled = true;
    }
    if (const char* d = std::getenv("YAMS_USE_MOCK_PROVIDER"); d && *d) {
        g_plugin_disabled = true;
    }
    if (const char* d = std::getenv("YAMS_SKIP_MODEL_LOADING"); d && *d) {
        g_plugin_disabled = true;
    }
    return YAMS_PLUGIN_OK;
}

extern "C" void yams_onnx_shutdown_provider();

YAMS_PLUGIN_API void yams_plugin_shutdown(void) {
    yams_onnx_shutdown_provider();
}

static yams_onnx_request_v1 g_req_v1 = {
    /*abi_version*/ YAMS_IFACE_ONNX_REQUEST_V1_VERSION,
    /*self*/ nullptr,
    /*process_json*/
    [](void*, const char* req, char** out_json) -> int {
        if (!out_json)
            return YAMS_PLUGIN_ERR_INVALID;
        nlohmann::json resp;
        try {
            auto j = nlohmann::json::parse(req ? req : "{}", nullptr, false);
            if (j.is_discarded()) {
                resp = {{"error", "invalid_json"}};
            } else {
                std::string task = j.value("task", "embedding");
                std::string model = j.value("model_id", "");
                if (task == "embedding") {
                    auto* prov = yams_onnx_get_model_provider();
                    if (!prov) {
                        resp = {{"error", "provider_unavailable"}};
                    } else {
                        // Pass options through to load_model so provider can configure
                        // device/batching hints
                        if (j.contains("options")) {
                            try {
                                std::string opt = j["options"].dump();
                                (void)prov->load_model(prov->self, model.c_str(), nullptr,
                                                       opt.c_str());
                            } catch (...) {
                            }
                        }
                        std::vector<std::string> inputs;
                        if (j.contains("inputs") && j["inputs"].is_array()) {
                            for (auto& x : j["inputs"])
                                if (x.is_string())
                                    inputs.push_back(x.get<std::string>());
                        }
                        size_t b = inputs.size();
                        if (b == 0) {
                            resp = {{"error", "no_inputs"}};
                        } else {
                            std::vector<const uint8_t*> ptrs(b);
                            std::vector<size_t> lens(b);
                            for (size_t i = 0; i < b; ++i) {
                                ptrs[i] = reinterpret_cast<const uint8_t*>(inputs[i].data());
                                lens[i] = inputs[i].size();
                            }
                            float* out = nullptr;
                            size_t out_b = 0, out_d = 0;
                            auto rc = prov->generate_embedding_batch(prov->self, model.c_str(),
                                                                     ptrs.data(), lens.data(), b,
                                                                     &out, &out_b, &out_d);
                            if (rc == YAMS_OK && out && out_b == b && out_d > 0) {
                                resp["task"] = "embedding";
                                resp["model_id"] = model;
                                resp["batch"] = out_b;
                                resp["dim"] = out_d;
                                resp["vectors"] = nlohmann::json::array();
                                for (size_t i = 0; i < out_b; ++i) {
                                    nlohmann::json row = nlohmann::json::array();
                                    for (size_t d = 0; d < out_d; ++d)
                                        row.push_back(out[i * out_d + d]);
                                    resp["vectors"].push_back(std::move(row));
                                }
                                prov->free_embedding_batch(prov->self, out, out_b, out_d);
                            } else {
                                resp = {{"error", "embedding_failed"}};
                            }
                        }
                    }
                } else if (task == "sentiment") {
                    resp = {{"error", "sentiment_not_implemented"}};
                } else {
                    resp = {{"error", "unsupported_task"}};
                }
            }
        } catch (...) {
            resp = {{"error", "exception"}};
        }
        std::string s = resp.dump();
        char* buf = (char*)std::malloc(s.size() + 1);
        if (!buf)
            return YAMS_PLUGIN_ERR_INVALID;
        std::memcpy(buf, s.c_str(), s.size() + 1);
        *out_json = buf;
        return YAMS_PLUGIN_OK;
    },
    /*free_string*/
    [](void*, char* s) {
        if (s)
            std::free(s);
    }};

YAMS_PLUGIN_API int yams_plugin_get_interface(const char* id, uint32_t version, void** out_iface) {
    if (!id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (g_plugin_disabled)
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    // Accept any version >= 1 up to our max (v4).
    if ((version >= 1 && version <= YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) &&
        std::strcmp(id, YAMS_IFACE_MODEL_PROVIDER_V1) == 0) {
        *out_iface = static_cast<void*>(yams_onnx_get_model_provider());
        return (*out_iface) ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_NOT_FOUND;
    }
    if (version == YAMS_IFACE_ONNX_REQUEST_V1_VERSION &&
        std::strcmp(id, YAMS_IFACE_ONNX_REQUEST_V1) == 0) {
        *out_iface = static_cast<void*>(&g_req_v1);
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    // Don't create singleton if plugin is disabled - just return disabled status
    const char* src;
    if (g_plugin_disabled) {
        src = "{\"status\":\"disabled\",\"reason\":\"disabled_by_env\"}";
    } else {
        src = yams_onnx_get_health_json_cstr();
        if (!src)
            src = "{\"status\":\"unknown\"}";
    }
    size_t n = std::strlen(src);
    char* buf = static_cast<char*>(std::malloc(n + 1));
    if (!buf)
        return YAMS_PLUGIN_ERR_INVALID;
    std::memcpy(buf, src, n + 1);
    *out_json = buf;
    return YAMS_PLUGIN_OK;
}

} // extern "C"

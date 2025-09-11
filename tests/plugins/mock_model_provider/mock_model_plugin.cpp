// New-style mock v1 model provider vtable (C ABI)
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <yams/plugins/abi.h>
#include <yams/plugins/model_provider_v1.h>

namespace {
struct Ctx {
    std::mutex mu;
    yams_model_load_progress_cb cb{};
    void* user{};
    std::set<std::string> models;
};
static Ctx g_ctx;
} // namespace

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}
const char* yams_plugin_get_name(void) {
    return "mock_model";
}
const char* yams_plugin_get_version(void) {
    return "0.0.1";
}
static const char* k_manifest = "{\"name\":\"mock_model\",\"version\":\"0.0.1\",\"interfaces\":[{"
                                "\"id\":\"model_provider_v1\",\"version\":1}]}";
const char* yams_plugin_get_manifest_json(void) {
    return k_manifest;
}
int yams_plugin_init(const char* config_json, const void* host_context) {
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}
void yams_plugin_shutdown(void) {}

static yams_model_provider_v1 g_table = [] {
    yams_model_provider_v1 t{};
    t.abi_version = YAMS_IFACE_MODEL_PROVIDER_V1_VERSION;
    t.self = &g_ctx;
    t.set_progress_callback = [](void* self, yams_model_load_progress_cb cb,
                                 void* user) -> yams_status_t {
        if (!self)
            return YAMS_ERR_INVALID_ARG;
        auto* c = static_cast<Ctx*>(self);
        std::lock_guard<std::mutex> lk(c->mu);
        c->cb = cb;
        c->user = user;
        return YAMS_OK;
    };
    t.load_model = [](void* self, const char* id, const char*, const char*) -> yams_status_t {
        if (!self || !id)
            return YAMS_ERR_INVALID_ARG;
        auto* c = static_cast<Ctx*>(self);
        std::lock_guard<std::mutex> lk(c->mu);
        c->models.insert(id);
        if (c->cb)
            c->cb(c->user, id, YAMS_MODEL_PHASE_READY, 0, 0, "ready");
        return YAMS_OK;
    };
    t.unload_model = [](void* self, const char* id) -> yams_status_t {
        if (!self || !id)
            return YAMS_ERR_INVALID_ARG;
        auto* c = static_cast<Ctx*>(self);
        std::lock_guard<std::mutex> lk(c->mu);
        c->models.erase(id);
        return YAMS_OK;
    };
    t.is_model_loaded = [](void* self, const char* id, bool* out) -> yams_status_t {
        if (!self || !id || !out)
            return YAMS_ERR_INVALID_ARG;
        auto* c = static_cast<Ctx*>(self);
        std::lock_guard<std::mutex> lk(c->mu);
        *out = c->models.count(id) != 0;
        return YAMS_OK;
    };
    t.get_loaded_models = [](void* self, const char*** out_ids,
                             size_t* out_count) -> yams_status_t {
        if (!self || !out_ids || !out_count)
            return YAMS_ERR_INVALID_ARG;
        auto* c = static_cast<Ctx*>(self);
        std::lock_guard<std::mutex> lk(c->mu);
        const size_t n = c->models.size();
        const char** ids = nullptr;
        if (n) {
            ids = static_cast<const char**>(std::malloc(sizeof(char*) * n));
            if (!ids)
                return YAMS_ERR_INTERNAL;
            size_t i = 0;
            for (const auto& s : c->models) {
                char* dup = static_cast<char*>(std::malloc(s.size() + 1));
                if (!dup) {
                    for (size_t j = 0; j < i; ++j)
                        std::free(const_cast<char*>(ids[j]));
                    std::free(ids);
                    return YAMS_ERR_INTERNAL;
                }
                std::memcpy(dup, s.c_str(), s.size() + 1);
                ids[i++] = dup;
            }
        }
        *out_ids = ids;
        *out_count = n;
        return YAMS_OK;
    };
    t.free_model_list = [](void*, const char** ids, size_t count) {
        if (!ids) {
            return;
        }
        for (size_t i = 0; i < count; ++i) {
            std::free(const_cast<char*>(ids[i]));
        }
        std::free(const_cast<char**>(ids));
    };
    t.generate_embedding = [](void*, const char*, const uint8_t*, size_t, float** out,
                              size_t* dim) -> yams_status_t {
        if (!out || !dim)
            return YAMS_ERR_INVALID_ARG;
        *out = nullptr;
        *dim = 0;
        return YAMS_ERR_UNSUPPORTED;
    };
    t.free_embedding = [](void*, float* v, size_t) {
        if (v)
            std::free(v);
    };
    t.generate_embedding_batch = [](void*, const char*, const uint8_t* const*, const size_t*,
                                    size_t, float** out, size_t* b, size_t* d) -> yams_status_t {
        if (!out || !b || !d)
            return YAMS_ERR_INVALID_ARG;
        *out = nullptr;
        *b = 0;
        *d = 0;
        return YAMS_ERR_UNSUPPORTED;
    };
    t.free_embedding_batch = [](void*, float* v, size_t, size_t) {
        if (v)
            std::free(v);
    };
    return t;
}();

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (!iface_id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (version == 1 && std::string(iface_id) == "model_provider_v1") {
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

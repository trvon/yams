// Replace stub with actual ONNX-backed implementation
#include "model_provider.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <thread>
#include <yams/daemon/resource/onnx_model_pool.h>

namespace {

struct ProviderCtx {
    std::mutex mu;
    yams_model_load_progress_cb progress_cb = nullptr;
    void* progress_user = nullptr;
    std::unique_ptr<yams::daemon::OnnxModelPool> pool;
    bool ready = false;
    bool disabled = false;
    std::string last_error;

    ProviderCtx() {
        // Allow disabling via env for diagnostics or platform constraints
        if (const char* d = std::getenv("YAMS_ONNX_PLUGIN_DISABLE"); d && *d) {
            disabled = true;
            last_error = "disabled_by_env";
            return;
        }
        yams::daemon::ModelPoolConfig cfg;
        // Defaults: prefer lazy loading to avoid blocking startup
        cfg.lazyLoading = true;
        cfg.enableGPU = false;
        cfg.numThreads = std::max(1u, std::thread::hardware_concurrency());
        // Read embeddings settings from config.toml (preferred_model, model_path, keep_model_hot)
        try {
            namespace fs = std::filesystem;
            fs::path cfgPath;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
                cfgPath = fs::path(xdg) / "yams" / "config.toml";
            } else if (const char* home = std::getenv("HOME")) {
                cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
            }
            std::string modelsRoot;
            bool keepModelHot = true;
            std::string preferredModel;
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream file(cfgPath);
                std::string line;
                std::string section;
                auto trim = [](std::string& s) {
                    if (s.empty())
                        return;
                    s.erase(0, s.find_first_not_of(" \t"));
                    auto pos = s.find_last_not_of(" \t");
                    if (pos != std::string::npos)
                        s.erase(pos + 1);
                };
                while (std::getline(file, line)) {
                    if (line.empty() || line[0] == '#')
                        continue;
                    if (line[0] == '[') {
                        auto end = line.find(']');
                        section = (end != std::string::npos) ? line.substr(1, end - 1) : "";
                        continue;
                    }
                    auto eq = line.find('=');
                    if (eq == std::string::npos)
                        continue;
                    std::string key = line.substr(0, eq);
                    std::string value = line.substr(eq + 1);
                    trim(key);
                    trim(value);
                    auto hash = value.find('#');
                    if (hash != std::string::npos) {
                        value = value.substr(0, hash);
                        trim(value);
                    }
                    if (value.size() >= 2 && value.front() == '"' && value.back() == '"')
                        value = value.substr(1, value.size() - 2);
                    if (section == "embeddings") {
                        if (key == "preferred_model" && preferredModel.empty())
                            preferredModel = value;
                        else if (key == "model_path" && modelsRoot.empty())
                            modelsRoot = value;
                        else if (key == "keep_model_hot") {
                            std::string v = value;
                            for (auto& c : v)
                                c = static_cast<char>(std::tolower(c));
                            keepModelHot = !(v == "false" || v == "0" || v == "no" || v == "off");
                        }
                    }
                }
            }
            if (!modelsRoot.empty() && modelsRoot[0] == '~') {
                if (const char* home = std::getenv("HOME"))
                    modelsRoot = std::string(home) + modelsRoot.substr(1);
            }
            if (!modelsRoot.empty())
                cfg.modelsRoot = modelsRoot;
            // Map keep_model_hot to lazyLoading inverse
            cfg.lazyLoading = !keepModelHot;
            if (keepModelHot && !preferredModel.empty()) {
                if (!modelsRoot.empty())
                    cfg.preloadModels = {modelsRoot + "/" + preferredModel + "/model.onnx"};
                else
                    cfg.preloadModels = {preferredModel};
            }
        } catch (const std::exception&) {
            // ignore config parse errors
        }
        pool = std::make_unique<yams::daemon::OnnxModelPool>(cfg);
        if (auto r = pool->initialize()) {
            ready = true;
            last_error.clear();
        } else {
            ready = false;
            last_error = r.error().message;
            spdlog::warn("[ONNX-Plugin] Pool initialize failed: {}", last_error);
        }
    }
};

// Helpers for progress emission
static void emit_progress(ProviderCtx* ctx, const char* model, int phase, const char* msg,
                          uint64_t cur = 0, uint64_t tot = 0) {
    if (!ctx || !ctx->progress_cb)
        return;
    try {
        ctx->progress_cb(ctx->progress_user, model, phase, cur, tot, msg);
    } catch (...) {
    }
}

struct ProviderSingleton {
    ProviderCtx ctx;
    yams_model_provider_v1 vtable;
    ProviderSingleton() {
        vtable.abi_version = YAMS_IFACE_MODEL_PROVIDER_V1_VERSION;
        vtable.self = &ctx;

        vtable.set_progress_callback = [](void* self, yams_model_load_progress_cb cb,
                                          void* user) -> yams_status_t {
            if (!self)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            std::lock_guard<std::mutex> lk(c->mu);
            c->progress_cb = cb;
            c->progress_user = user;
            return YAMS_OK;
        };

        vtable.load_model = [](void* self, const char* model_id, const char* /*model_path*/,
                               const char* options_json) -> yams_status_t {
            if (!self || !model_id)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled)
                return YAMS_ERR_UNSUPPORTED;
            if (!c->ready || !c->pool)
                return YAMS_ERR_INTERNAL;
            // Parse per-load options (hf.revision, offline)
            try {
                yams::daemon::OnnxModelPool::ResolutionHints hints;
                if (options_json && std::strlen(options_json) > 0) {
                    auto j = nlohmann::json::parse(options_json, nullptr, false);
                    if (!j.is_discarded()) {
                        if (j.contains("hf") && j["hf"].is_object()) {
                            auto& hf = j["hf"];
                            if (hf.contains("revision") && hf["revision"].is_string())
                                hints.hfRevision = hf["revision"].get<std::string>();
                            if (hf.contains("offline") && hf["offline"].is_boolean())
                                hints.offlineOnly = hf["offline"].get<bool>();
                        }
                        if (j.contains("offline") && j["offline"].is_boolean())
                            hints.offlineOnly = j["offline"].get<bool>();
                    }
                }
                if (!hints.hfRevision.empty() || hints.offlineOnly) {
                    c->pool->setResolutionHints(model_id, hints);
                }
            } catch (...) {
                // ignore malformed options
            }
            emit_progress(c, model_id, YAMS_MODEL_PHASE_PROBE, "probe");
            auto r = c->pool->loadModel(model_id);
            if (!r) {
                emit_progress(c, model_id, YAMS_MODEL_PHASE_UNKNOWN, "error");
                return YAMS_ERR_NOT_FOUND;
            }
            emit_progress(c, model_id, YAMS_MODEL_PHASE_LOAD, "initializing");
            // Ready (pool creates session on first acquire if lazyLoading)
            emit_progress(c, model_id, YAMS_MODEL_PHASE_READY, "ready");
            return YAMS_OK;
        };

        vtable.unload_model = [](void* self, const char* model_id) -> yams_status_t {
            if (!self || !model_id)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled)
                return YAMS_ERR_UNSUPPORTED;
            if (!c->ready || !c->pool)
                return YAMS_ERR_INTERNAL;
            auto r = c->pool->unloadModel(model_id);
            return r ? YAMS_OK : YAMS_ERR_NOT_FOUND;
        };

        vtable.is_model_loaded = [](void* self, const char* model_id,
                                    bool* out_loaded) -> yams_status_t {
            if (!self || !model_id || !out_loaded)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled) {
                *out_loaded = false;
                return YAMS_ERR_UNSUPPORTED;
            }
            if (!c->ready || !c->pool) {
                *out_loaded = false;
                return YAMS_ERR_INTERNAL;
            }
            *out_loaded = c->pool->isModelLoaded(model_id);
            return YAMS_OK;
        };

        vtable.get_loaded_models = [](void* self, const char*** out_ids,
                                      size_t* out_count) -> yams_status_t {
            if (!self || !out_ids || !out_count)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled)
                return YAMS_ERR_UNSUPPORTED;
            if (!c->ready || !c->pool) {
                *out_ids = nullptr;
                *out_count = 0;
                return YAMS_ERR_INTERNAL;
            }
            auto v = c->pool->getLoadedModels();
            const size_t n = v.size();
            const char** ids = nullptr;
            if (n > 0) {
                ids = static_cast<const char**>(std::malloc(sizeof(char*) * n));
                if (!ids)
                    return YAMS_ERR_INTERNAL;
                size_t i = 0;
                for (const auto& s : v) {
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

        vtable.free_model_list = [](void* /*self*/, const char** ids, size_t count) {
            if (!ids)
                return;
            for (size_t i = 0; i < count; ++i)
                std::free(const_cast<char*>(ids[i]));
            std::free(const_cast<char**>(ids));
        };

        vtable.generate_embedding = [](void* self, const char* model_id, const uint8_t* input,
                                       size_t input_len, float** out_vec,
                                       size_t* out_dim) -> yams_status_t {
            if (!self || !model_id || !input || !out_vec || !out_dim)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled)
                return YAMS_ERR_UNSUPPORTED;
            if (!c->ready || !c->pool)
                return YAMS_ERR_INTERNAL;
            std::string text(reinterpret_cast<const char*>(input), input_len);
            auto h = c->pool->acquireModel(model_id, std::chrono::seconds(10));
            if (!h)
                return YAMS_ERR_INTERNAL;
            auto& session = *h.value();
            auto r = const_cast<yams::daemon::OnnxModelSession&>(session).generateEmbedding(text);
            if (!r)
                return YAMS_ERR_INTERNAL;
            auto& vec = r.value();
            const size_t n = vec.size();
            float* buf = static_cast<float*>(std::malloc(sizeof(float) * n));
            if (!buf)
                return YAMS_ERR_INTERNAL;
            std::memcpy(buf, vec.data(), sizeof(float) * n);
            *out_vec = buf;
            *out_dim = n;
            return YAMS_OK;
        };

        vtable.free_embedding = [](void* /*self*/, float* vec, size_t /*dim*/) {
            if (vec)
                std::free(vec);
        };

        vtable.generate_embedding_batch = [](void* self, const char* model_id,
                                             const uint8_t* const* inputs, const size_t* input_lens,
                                             size_t batch_size, float** out_vecs, size_t* out_batch,
                                             size_t* out_dim) -> yams_status_t {
            if (!self || !model_id || !inputs || !input_lens || !out_vecs || !out_batch || !out_dim)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (c->disabled)
                return YAMS_ERR_UNSUPPORTED;
            if (!c->ready || !c->pool) {
                *out_vecs = nullptr;
                *out_batch = 0;
                *out_dim = 0;
                return YAMS_ERR_INTERNAL;
            }
            std::vector<std::string> texts;
            texts.reserve(batch_size);
            for (size_t i = 0; i < batch_size; ++i) {
                texts.emplace_back(reinterpret_cast<const char*>(inputs[i]), input_lens[i]);
            }
            auto h = c->pool->acquireModel(model_id, std::chrono::seconds(10));
            if (!h)
                return YAMS_ERR_INTERNAL;
            auto& session = *h.value();
            auto r =
                const_cast<yams::daemon::OnnxModelSession&>(session).generateBatchEmbeddings(texts);
            if (!r)
                return YAMS_ERR_INTERNAL;
            auto& mat = r.value();
            if (mat.empty()) {
                *out_vecs = nullptr;
                *out_batch = 0;
                *out_dim = 0;
                return YAMS_OK;
            }
            const size_t b = mat.size();
            const size_t d = mat[0].size();
            float* buf = static_cast<float*>(std::malloc(sizeof(float) * b * d));
            if (!buf)
                return YAMS_ERR_INTERNAL;
            for (size_t i = 0; i < b; ++i) {
                std::memcpy(buf + i * d, mat[i].data(), sizeof(float) * d);
            }
            *out_vecs = buf;
            *out_batch = b;
            *out_dim = d;
            return YAMS_OK;
        };

        vtable.free_embedding_batch = [](void* /*self*/, float* vecs, size_t /*batch*/,
                                         size_t /*dim*/) {
            if (vecs)
                std::free(vecs);
        };
    }
};

static ProviderSingleton& singleton() {
    static ProviderSingleton s;
    return s;
}

} // namespace

extern "C" yams_model_provider_v1* yams_onnx_get_model_provider() {
    return &singleton().vtable;
}

// Provide a small JSON health snapshot for the ABI host
extern "C" const char* yams_onnx_get_health_json_cstr() {
    static thread_local std::string json;
    const auto& c = singleton().ctx;
    // Build a minimal JSON reflecting readiness/disabled and last error
    nlohmann::json j;
    if (c.disabled) {
        j["status"] = "disabled";
        j["reason"] = c.last_error.empty() ? "disabled_by_env" : c.last_error;
    } else if (!c.pool) {
        j["status"] = "unavailable";
        j["reason"] = "no_pool";
    } else if (!c.ready) {
        j["status"] = "degraded";
        j["reason"] = c.last_error.empty() ? "init_failed" : c.last_error;
    } else {
        j["status"] = "ok";
    }
    try {
        json = j.dump();
    } catch (...) {
        json = "{\"status\":\"unknown\"}";
    }
    return json.c_str();
}

// Replace stub with actual ONNX-backed implementation
#include <future>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>
#include "model_provider.h"
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/resource/onnx_model_pool.h>

namespace {

// Provide a C-callable function for threading control (not supported yet)
static yams_status_t onnx_set_threading(void* /*self*/, const char* /*model_id*/, int /*intra*/,
                                        int /*inter*/) {
    return YAMS_ERR_UNSUPPORTED;
}

struct ProviderCtx {
    enum class State : uint8_t { Unloaded, Loading, Ready, Failed };
    std::mutex mu;
    yams_model_load_progress_cb progress_cb = nullptr;
    void* progress_user = nullptr;
    std::unique_ptr<yams::daemon::OnnxModelPool> pool;
    std::unordered_map<std::string, State> model_states; // per-model FSM
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
        // Models will be loaded on first use, not during plugin initialization
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
            std::string dataDir;
            bool keepModelHot = true;
            std::string preferredModel;
            std::vector<std::string> preloadList;

            // First, check for YAMS_STORAGE environment variable
            if (const char* storage = std::getenv("YAMS_STORAGE")) {
                dataDir = storage;
            }

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

                    // Read data directory from [storage].base_path if not set by env
                    if (section == "storage" && key == "base_path" && dataDir.empty()) {
                        dataDir = value;
                    }

                    if (section == "embeddings") {
                        if (key == "preferred_model" && preferredModel.empty())
                            preferredModel = value;
                        else if (key == "keep_model_hot") {
                            std::string v = value;
                            for (auto& c : v)
                                c = static_cast<char>(std::tolower(c));
                            keepModelHot = !(v == "false" || v == "0" || v == "no" || v == "off");
                        }
                    }
                    // New: preload list under [plugins.onnx]
                    if (section == "plugins.onnx" && key == "preload") {
                        // parse CSV style list: preload = "modelA,modelB"
                        std::string s = value;
                        for (char& ch : s) {
                            if (ch == '[' || ch == ']')
                                ch = ' ';
                        }
                        trim(s);
                        // allow ["a","b"] or a,b
                        if (!s.empty() && s.front() == '"' && s.back() == '"')
                            s = s.substr(1, s.size() - 2);
                        size_t start = 0;
                        while (start < s.size()) {
                            auto comma = s.find(',', start);
                            std::string item =
                                s.substr(start, comma == std::string::npos ? std::string::npos
                                                                           : (comma - start));
                            trim(item);
                            if (!item.empty() && item.front() == '"' && item.back() == '"')
                                item = item.substr(1, item.size() - 2);
                            if (!item.empty())
                                preloadList.push_back(item);
                            if (comma == std::string::npos)
                                break;
                            start = comma + 1;
                        }
                    }
                    // New: explicit models table entries [plugins.onnx.models.NAME]
                    if (section.rfind("plugins.onnx.models.", 0) == 0 && key == "task") {
                        // Section name encodes model_id; mark for preload as hot
                        std::string model_id =
                            section.substr(std::string("plugins.onnx.models.").size());
                        if (!model_id.empty())
                            preloadList.push_back(model_id);
                    }
                }
            }

            // Set modelsRoot to $dataDir/models if dataDir is available
            if (!dataDir.empty()) {
                // Expand ~ if present
                if (dataDir[0] == '~') {
                    if (const char* home = std::getenv("HOME"))
                        dataDir = std::string(home) + dataDir.substr(1);
                }
                cfg.modelsRoot = dataDir + "/models";
                spdlog::info("[ONNX-Plugin] Using models directory: {}", cfg.modelsRoot);
            }
            // Map keep_model_hot to lazyLoading inverse - but always use lazy loading
            // during plugin initialization to avoid blocking daemon startup.
            // Background preloading (if configured) will happen after init completes.
            cfg.lazyLoading = true; // Force lazy loading during plugin init
            // Build preload list if any (will be deferred until after initialization)
            if (!preloadList.empty()) {
                cfg.preloadModels = preloadList;
            } else if (keepModelHot && !preferredModel.empty()) {
                cfg.preloadModels = {preferredModel};
            }
            // Force no-preload in test/mock environments to avoid background threads
            if (std::getenv("YAMS_SKIP_MODEL_LOADING") || std::getenv("YAMS_TEST_MODE") ||
                std::getenv("YAMS_USE_MOCK_PROVIDER")) {
                cfg.preloadModels.clear();
                cfg.lazyLoading = true;
            }
        } catch (const std::exception&) {
            // ignore config parse errors
        }
        pool = std::make_unique<yams::daemon::OnnxModelPool>(cfg);
        // Initialize pool asynchronously (non-blocking)
        // Note: With lazyLoading=true, initialize() just sets a flag and returns immediately.
        // Actual model loading happens on-demand via loadModel() when first requested.
        auto fut = std::async(std::launch::async, [this]() { return pool->initialize(); });
        if (fut.wait_for(std::chrono::seconds(10)) == std::future_status::ready) {
            auto res = fut.get();
            if (res) {
                ready = true;
                last_error.clear();
            } else {
                // Initialization failed (rare - only happens if mutex deadlock or similar)
                ready = false;
                last_error = res.error().message;
                spdlog::warn("[ONNX-Plugin] Pool initialize failed: {}", last_error);
            }
        } else {
            // Timeout during init (should be rare with lazyLoading=true)
            // Mark as ready anyway - models can still be loaded on-demand
            spdlog::warn("[ONNX-Plugin] Pool initialize timed out after 10s, marking ready anyway "
                         "(lazy loading enabled)");
            ready = true;
            last_error.clear();
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
        vtable.abi_version = 2; // v1.2
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
            }
            // FSM: if already loading/ready, return OK
            {
                std::lock_guard<std::mutex> lk(c->mu);
                auto& st = c->model_states[model_id];
                if (st == ProviderCtx::State::Ready || st == ProviderCtx::State::Loading)
                    return YAMS_OK;
                st = ProviderCtx::State::Loading;
            }
            emit_progress(c, model_id, YAMS_MODEL_PHASE_PROBE, "probe");
            // Launch async background loader; publish events
            std::thread([c, mid = std::string(model_id)]() {
                auto r = c->pool->loadModel(mid);
                if (r) {
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->model_states[mid] = ProviderCtx::State::Ready;
                    }
                    emit_progress(c, mid.c_str(), YAMS_MODEL_PHASE_READY, "ready");
                    auto q =
                        yams::daemon::InternalEventBus::instance()
                            .get_or_create_channel<yams::daemon::InternalEventBus::ModelReadyEvent>(
                                "model.events", 256);
                    (void)q->try_push({mid});
                } else {
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->model_states[mid] = ProviderCtx::State::Failed;
                    }
                    emit_progress(c, mid.c_str(), YAMS_MODEL_PHASE_UNKNOWN, "error");
                    auto q = yams::daemon::InternalEventBus::instance()
                                 .get_or_create_channel<
                                     yams::daemon::InternalEventBus::ModelLoadFailedEvent>(
                                     "model.events", 256);
                    (void)q->try_push({mid, r.error().message});
                }
            }).detach();
            return YAMS_OK; // non-blocking
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
            bool loaded = c->pool->isModelLoaded(model_id);
            // Consider FSM Ready as loaded (pool may be lazy)
            {
                std::lock_guard<std::mutex> lk(c->mu);
                auto it = c->model_states.find(model_id);
                if (!loaded && it != c->model_states.end() &&
                    it->second == ProviderCtx::State::Ready)
                    loaded = true;
            }
            *out_loaded = loaded;
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
            auto h = c->pool->acquireModel(model_id, std::chrono::seconds(30));
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
            auto h = c->pool->acquireModel(model_id, std::chrono::seconds(30));
            if (!h) {
                spdlog::warn("[ONNX Plugin] acquireModel failed for {} (batch)", model_id);
                return YAMS_ERR_INTERNAL;
            }
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

        // v1.2: get_embedding_dim (non-loading path)
        vtable.get_embedding_dim = [](void* self, const char* model_id,
                                      size_t* out_dim) -> yams_status_t {
            if (!self || !model_id || !out_dim)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (!c->pool)
                return YAMS_ERR_INTERNAL;

            // 1) If a hot session already exists, read its dim without blocking
            if (c->ready && c->pool->isModelLoaded(model_id)) {
                auto h = c->pool->acquireModel(model_id, std::chrono::milliseconds(0));
                if (h) {
                    *out_dim = h.value()->getEmbeddingDim();
                    if (*out_dim > 0)
                        return YAMS_OK;
                }
            }

            // 2) Probe model directory metadata without loading the model
            namespace fs = std::filesystem;
            size_t dim = 0;
            try {
                auto read_dim = [&](const fs::path& p) -> size_t {
                    try {
                        if (!fs::exists(p))
                            return 0u;
                        std::ifstream in(p);
                        if (!in)
                            return 0u;
                        nlohmann::json j;
                        in >> j;
                        if (j.contains("embedding_dimension"))
                            return j.value("embedding_dimension", 0u);
                        if (j.contains("embedding_dim"))
                            return j.value("embedding_dim", 0u);
                        if (j.contains("hidden_size"))
                            return j.value("hidden_size", 0u);
                    } catch (...) {
                    }
                    return 0u;
                };

                // Candidate roots: XDG_DATA_HOME/yams/models, HOME/.local/share/yams/models
                std::vector<fs::path> roots;
                if (const char* xdg = std::getenv("XDG_DATA_HOME"))
                    roots.emplace_back(fs::path(xdg) / "yams" / "models");
                if (const char* home = std::getenv("HOME"))
                    roots.emplace_back(fs::path(home) / ".local" / "share" / "yams" / "models");

                for (const auto& r : roots) {
                    fs::path base = r / model_id;
                    for (const auto& fn :
                         {"sentence_bert_config.json", "config.json", "model.onnx.yams.meta.json",
                          "config.json.yams.meta.json",
                          "sentence_bert_config.json.yams.meta.json"}) {
                        dim = read_dim(base / fn);
                        if (dim > 0)
                            break;
                    }
                    if (dim > 0)
                        break;
                }
            } catch (...) {
            }
            if (dim > 0) {
                *out_dim = dim;
                return YAMS_OK;
            }

            // 3) Heuristic from common embedding model names (no load)
            std::string lm(model_id);
            for (auto& ch : lm)
                ch = static_cast<char>(std::tolower(ch));
            if (lm.find("minilm") != std::string::npos)
                dim = 384;
            else if (lm.find("mpnet") != std::string::npos || lm.find("nomic") != std::string::npos)
                dim = 768;
            else if (lm.find("bge-m3") != std::string::npos ||
                     (lm.find("bge") != std::string::npos && lm.find("large") != std::string::npos))
                dim = 1024;
            else if (lm.find("bge") != std::string::npos && lm.find("small") != std::string::npos)
                dim = 384;
            else if (lm.find("e5") != std::string::npos && lm.find("large") != std::string::npos)
                dim = 1024;
            else if (lm.find("e5") != std::string::npos && lm.find("small") != std::string::npos)
                dim = 384;
            else if (lm.find("gte") != std::string::npos && lm.find("large") != std::string::npos)
                dim = 1024;
            else if (lm.find("gte") != std::string::npos && lm.find("small") != std::string::npos)
                dim = 384;
            else if (lm.find("text-embedding-3-large") != std::string::npos)
                dim = 3072;
            else if (lm.find("text-embedding-3-small") != std::string::npos)
                dim = 1536;

            if (dim > 0) {
                *out_dim = dim;
                return YAMS_OK;
            }

            *out_dim = 0;
            return YAMS_ERR_UNSUPPORTED;
        };

        // v1.2: get_runtime_info_json
        vtable.get_runtime_info_json = [](void* self, const char* model_id,
                                          char** out_json) -> yams_status_t {
            if (!self || !model_id || !out_json)
                return YAMS_ERR_INVALID_ARG;
            auto* c = static_cast<ProviderCtx*>(self);
            if (!c->pool)
                return YAMS_ERR_INTERNAL;
            nlohmann::json j;
            j["backend"] = "onnxruntime";
            j["pipeline"] = "raw_ort";
            j["model"] = model_id;
            // Best-effort dimension and runtime hints
            j["graph_optimization"] = "enabled";
            j["execution_provider"] = "cpu";
            size_t dim = 0;
            if (c->ready) {
                auto h = c->pool->acquireModel(model_id, std::chrono::seconds(2));
                if (h) {
                    dim = h.value()->getEmbeddingDim();
                    try {
                        const auto& info = h.value()->getInfo();
                        if (!info.path.empty())
                            j["path"] = info.path;
                    } catch (...) {
                    }
                }
            }
            j["dim"] = dim;
            j["intra_threads"] =
                static_cast<int>(std::max(1u, std::thread::hardware_concurrency()));
            j["inter_threads"] = 1;
            // Source hint from path when present
            try {
                if (j.contains("path") && j["path"].is_string()) {
                    std::string p = j["path"].get<std::string>();
                    if (p.find("/.cache/huggingface/") != std::string::npos)
                        j["source"] = "huggingface-cache";
                    else if (p.find(".yams/models/") != std::string::npos)
                        j["source"] = "yams-models";
                    else if (p.rfind("http://", 0) == 0 || p.rfind("https://", 0) == 0)
                        j["source"] = "remote";
                    else
                        j["source"] = "local";
                }
            } catch (...) {
            }
            std::string s;
            try {
                s = j.dump();
            } catch (...) {
                s = "{}";
            }
            char* buf = static_cast<char*>(std::malloc(s.size() + 1));
            if (!buf)
                return YAMS_ERR_INTERNAL;
            std::memcpy(buf, s.c_str(), s.size() + 1);
            *out_json = buf;
            return YAMS_OK;
        };

        vtable.free_string = [](void* /*self*/, char* s) {
            if (s)
                std::free(s);
        };

        // Assign function pointer for set_threading
        vtable.set_threading = &onnx_set_threading;
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

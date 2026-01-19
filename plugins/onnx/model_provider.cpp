// Replace stub with actual ONNX-backed implementation
#include "model_provider.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/resource/onnx_model_pool.h>
#include <yams/daemon/resource/onnx_reranker_session.h>

namespace {

// Global storage for plugin config JSON passed from daemon at init time
static std::string g_plugin_config_json;

// Provide a C-callable function for threading control (not supported yet)
static yams_status_t onnx_set_threading(void* /*self*/, const char* /*model_id*/, int /*intra*/,
                                        int /*inter*/) {
    return YAMS_ERR_UNSUPPORTED;
}

struct ProviderCtx {
    enum class State : uint8_t { Unloaded, Loading, Ready, Failed };

    // Track model load failures for backoff and event signaling
    struct FailureInfo {
        std::chrono::steady_clock::time_point lastAttempt;
        std::chrono::steady_clock::time_point lastFailure;
        uint32_t consecutiveFailures = 0;
        std::string lastError;
        bool eventFired = false; // Whether we've fired ModelLoadFailedEvent
    };

    std::mutex mu;
    yams_model_load_progress_cb progress_cb = nullptr;
    void* progress_user = nullptr;
    std::unique_ptr<yams::daemon::OnnxModelPool> pool;
    std::unique_ptr<yams::daemon::OnnxRerankerSession> reranker; // Cross-encoder reranker
    std::unordered_map<std::string, State> model_states;         // per-model FSM
    std::unordered_map<std::string, FailureInfo> model_failures; // failure tracking
    bool ready = false;
    bool disabled = false;
    std::string last_error;
    std::string rerankerModelPath; // Path to reranker model

    // Check if model is in cooldown period after failures
    bool isInCooldown(const std::string& modelId) const {
        auto it = model_failures.find(modelId);
        if (it == model_failures.end() || it->second.consecutiveFailures == 0) {
            return false;
        }
        // Exponential backoff: 2^min(failures, 6) seconds (max ~64 seconds)
        auto backoffSeconds =
            std::chrono::seconds(1 << std::min(it->second.consecutiveFailures, 6u));
        auto elapsed = std::chrono::steady_clock::now() - it->second.lastFailure;
        return elapsed < backoffSeconds;
    }

    // Record a model load failure
    void recordFailure(const std::string& modelId, const std::string& error) {
        auto& info = model_failures[modelId];
        auto now = std::chrono::steady_clock::now();
        info.lastAttempt = now;
        info.lastFailure = now;
        info.consecutiveFailures++;
        info.lastError = error;
    }

    // Clear failure tracking on successful load
    void clearFailure(const std::string& modelId) {
        auto it = model_failures.find(modelId);
        if (it != model_failures.end()) {
            it->second.consecutiveFailures = 0;
            it->second.lastError.clear();
            it->second.eventFired = false;
        }
    }

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

        // Variables for config (both file-based and JSON-based)
        std::string dataDir;
        bool keepModelHot = true;
        std::string preferredModel;
        std::vector<std::string> preloadList;

        // Read embeddings settings from config.toml (preferred_model, model_path, keep_model_hot)
        try {
            namespace fs = std::filesystem;
            fs::path cfgPath;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
                cfgPath = fs::path(xdg) / "yams" / "config.toml";
            } else if (const char* home = std::getenv("HOME")) {
                cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
            }

            // First, check for YAMS_STORAGE environment variable
            if (const char* storage = std::getenv("YAMS_STORAGE")) {
                dataDir = storage;
            }

            spdlog::debug("[ONNX-Plugin] Config path: {}", cfgPath.string());
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                spdlog::debug("[ONNX-Plugin] Config file exists, parsing...");
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
                        spdlog::debug("[ONNX-Plugin] Entering section: [{}]", section);
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
                    if (section == "storage" && key == "base_path") {
                        spdlog::debug("[ONNX-Plugin] Found [storage].base_path = '{}'", value);
                        if (dataDir.empty()) {
                            dataDir = value;
                        }
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
            spdlog::debug("[ONNX-Plugin] After config parsing: dataDir='{}'", dataDir);
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
            // Disable lazy loading to force startup checks and surface deadlocks early
            cfg.lazyLoading = false;
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

        // Parse JSON config from plugin init (overrides file-based config)
        if (!g_plugin_config_json.empty()) {
            try {
                auto j = nlohmann::json::parse(g_plugin_config_json, nullptr, false);
                if (!j.is_discarded() && j.is_object()) {
                    spdlog::info("[ONNX-Plugin] Applying JSON config: {}", g_plugin_config_json);
                    // preferred_model
                    if (j.contains("preferred_model") && j["preferred_model"].is_string()) {
                        preferredModel = j["preferred_model"].get<std::string>();
                        spdlog::info("[ONNX-Plugin] JSON config: preferred_model='{}'",
                                     preferredModel);
                    }
                    // preload (CSV string or single model)
                    if (j.contains("preload") && j["preload"].is_string()) {
                        std::string preloadStr = j["preload"].get<std::string>();
                        preloadList.clear();
                        // Parse CSV: "modelA,modelB" or single "modelA"
                        size_t start = 0;
                        while (start < preloadStr.size()) {
                            auto comma = preloadStr.find(',', start);
                            std::string item = preloadStr.substr(start, comma == std::string::npos
                                                                            ? std::string::npos
                                                                            : (comma - start));
                            // Trim whitespace
                            auto trim = [](std::string& s) {
                                if (s.empty())
                                    return;
                                s.erase(0, s.find_first_not_of(" \t"));
                                auto pos = s.find_last_not_of(" \t");
                                if (pos != std::string::npos)
                                    s.erase(pos + 1);
                            };
                            trim(item);
                            if (!item.empty())
                                preloadList.push_back(item);
                            if (comma == std::string::npos)
                                break;
                            start = comma + 1;
                        }
                        spdlog::info("[ONNX-Plugin] JSON config: preload list has {} models",
                                     preloadList.size());
                    }
                    // keep_model_hot
                    if (j.contains("keep_model_hot") && j["keep_model_hot"].is_boolean()) {
                        keepModelHot = j["keep_model_hot"].get<bool>();
                        spdlog::info("[ONNX-Plugin] JSON config: keep_model_hot={}", keepModelHot);
                    }
                    // models_root - override the models directory
                    if (j.contains("models_root") && j["models_root"].is_string()) {
                        cfg.modelsRoot = j["models_root"].get<std::string>();
                        spdlog::info("[ONNX-Plugin] JSON config: models_root='{}'", cfg.modelsRoot);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("[ONNX-Plugin] Failed to parse JSON config: {}", e.what());
            }
            // Re-apply preload settings from JSON config
            if (!preloadList.empty()) {
                cfg.preloadModels = preloadList;
            } else if (keepModelHot && !preferredModel.empty()) {
                cfg.preloadModels = {preferredModel};
            }
        }

        spdlog::info("[ONNX-Plugin] Creating OnnxModelPool with modelsRoot={}", cfg.modelsRoot);
        pool = std::make_unique<yams::daemon::OnnxModelPool>(cfg);
        spdlog::info("[ONNX-Plugin] OnnxModelPool created, calling initialize()...");
        try {
            auto res = pool->initialize();
            if (res) {
                ready = true;
                last_error.clear();
                spdlog::info("[ONNX-Plugin] Pool initialized successfully, ready=true");
            } else {
                ready = false;
                last_error = res.error().message;
                spdlog::warn("[ONNX-Plugin] Pool initialize failed: {}", last_error);
            }
        } catch (const std::exception& e) {
            spdlog::error("[ONNX-Plugin] Pool initialize exception: {}", e.what());
            ready = false;
            last_error = e.what();
        }
        spdlog::info("[ONNX-Plugin] ProviderCtx init complete: ready={} pool={}", ready,
                     pool ? "valid" : "null");
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

    std::atomic<bool> shutdownCalled{false};
    std::atomic<bool> explicitShutdownCalled{false}; // Set by yams_onnx_shutdown_provider()

    void shutdown(bool isExplicit = false) noexcept {
        if (shutdownCalled.exchange(true)) {
            return;
        }

        // If this is called from destructor during static destruction (not explicit),
        // skip cleanup to avoid crashes from corrupted global state.
        // At process exit, resources will be freed anyway.
        if (!isExplicit && !explicitShutdownCalled.load()) {
            return; // Skip cleanup during static destruction
        }

        if (ctx.pool) {
            try {
                ctx.pool->shutdown();
            } catch (const std::exception& e) {
                try {
                    spdlog::warn("[ONNX] pool shutdown exception: {}", e.what());
                } catch (...) {
                }
            } catch (...) {
                try {
                    spdlog::warn("[ONNX] pool shutdown unknown exception");
                } catch (...) {
                }
            }
            try {
                ctx.pool.reset();
            } catch (const std::exception& e) {
                try {
                    spdlog::warn("[ONNX] pool reset exception: {}", e.what());
                } catch (...) {
                }
            } catch (...) {
                try {
                    spdlog::warn("[ONNX] pool reset unknown exception");
                } catch (...) {
                }
            }
        }
    }

    ~ProviderSingleton() noexcept {
        shutdown(false); // Called from destructor, not explicit
    }

    ProviderSingleton() {
        vtable.abi_version = 3; // v1.3 (added score_documents for reranking)
        vtable.self = &ctx;

        // Use daemon's default logger - no separate file needed
        spdlog::debug("[ONNX Plugin] Initialized (v1.2) - using daemon logger");

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
            // Wrap entire lambda body in try/catch to prevent exceptions from crossing ABI
            // boundary. On Windows, ONNX Runtime can throw std::system_error("resource deadlock
            // would occur") when thread pool resources are exhausted (PBI-1c1).
            try {
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
                std::string modelIdStr(model_id);

                // FSM: if already loading/ready, return OK
                {
                    std::lock_guard<std::mutex> lk(c->mu);
                    auto& st = c->model_states[modelIdStr];
                    if (st == ProviderCtx::State::Ready) {
                        spdlog::debug("[ONNX Plugin] load_model: '{}' already ready", model_id);
                        return YAMS_OK;
                    }
                    if (st == ProviderCtx::State::Loading) {
                        spdlog::debug("[ONNX Plugin] load_model: '{}' already loading", model_id);
                        return YAMS_OK;
                    }
                    // Check if in cooldown from previous failures
                    if (c->isInCooldown(modelIdStr)) {
                        auto& info = c->model_failures[modelIdStr];
                        spdlog::warn(
                            "[ONNX Plugin] load_model: '{}' in cooldown ({} failures), skipping",
                            model_id, info.consecutiveFailures);
                        return YAMS_ERR_INTERNAL;
                    }
                    st = ProviderCtx::State::Loading;
                }
                spdlog::info("[ONNX Plugin] load_model: starting load for '{}'", model_id);
                emit_progress(c, model_id, YAMS_MODEL_PHASE_PROBE, "probe");

                // Simply call loadModel synchronously - it returns immediately since the pool uses
                // lazy loading
                auto r = c->pool->loadModel(modelIdStr);
                if (r) {
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->model_states[modelIdStr] = ProviderCtx::State::Ready;
                        c->clearFailure(modelIdStr);
                    }
                    spdlog::info("[ONNX Plugin] load_model: '{}' loaded successfully", model_id);
                    emit_progress(c, model_id, YAMS_MODEL_PHASE_READY, "ready");
                    auto q =
                        yams::daemon::InternalEventBus::instance()
                            .get_or_create_channel<yams::daemon::InternalEventBus::ModelReadyEvent>(
                                "model.events", 256);
                    (void)q->try_push({modelIdStr});
                } else {
                    std::string errMsg = r.error().message;
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->model_states[modelIdStr] = ProviderCtx::State::Failed;
                        c->recordFailure(modelIdStr, errMsg);
                    }
                    spdlog::error("[ONNX Plugin] load_model: '{}' failed: {}", model_id, errMsg);
                    emit_progress(c, model_id, YAMS_MODEL_PHASE_UNKNOWN, errMsg.c_str());
                    auto q = yams::daemon::InternalEventBus::instance()
                                 .get_or_create_channel<
                                     yams::daemon::InternalEventBus::ModelLoadFailedEvent>(
                                     "model.events", 256);
                    (void)q->try_push({std::string(model_id), r.error().message});
                }
                return YAMS_OK;
            } catch (const std::system_error& e) {
                fprintf(stderr, "[ONNX Plugin] load_model std::system_error: %s\n", e.what());
                spdlog::error("[ONNX Plugin] load_model std::system_error: {}", e.what());
                return YAMS_ERR_INTERNAL;
            } catch (const std::exception& e) {
                fprintf(stderr, "[ONNX Plugin] load_model exception: %s\n", e.what());
                spdlog::error("[ONNX Plugin] load_model exception: {}", e.what());
                return YAMS_ERR_INTERNAL;
            } catch (...) {
                fprintf(stderr, "[ONNX Plugin] load_model unknown exception\n");
                spdlog::error("[ONNX Plugin] load_model unknown exception");
                return YAMS_ERR_INTERNAL;
            }
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
            // Wrap entire lambda body in try/catch to prevent exceptions from crossing ABI
            // boundary. On Windows, ONNX Runtime can throw std::system_error("resource deadlock
            // would occur") when thread pool resources are exhausted (PBI-1c1).
            try {
                auto* c = static_cast<ProviderCtx*>(self);
                if (c->disabled) {
                    spdlog::warn("[ONNX Plugin] generate_embedding: plugin disabled");
                    return YAMS_ERR_UNSUPPORTED;
                }
                if (!c->ready) {
                    spdlog::warn("[ONNX Plugin] generate_embedding: plugin not ready");
                    return YAMS_ERR_INTERNAL;
                }
                if (!c->pool) {
                    spdlog::warn("[ONNX Plugin] generate_embedding: pool is null");
                    return YAMS_ERR_INTERNAL;
                }
                std::string modelIdStr(model_id);

                // Check if model is in cooldown after previous failures
                {
                    std::lock_guard<std::mutex> lk(c->mu);
                    if (c->isInCooldown(modelIdStr)) {
                        auto& info = c->model_failures[modelIdStr];
                        spdlog::debug("[ONNX Plugin] Model '{}' in cooldown ({} failures)",
                                      model_id, info.consecutiveFailures);
                        *out_vec = nullptr;
                        *out_dim = 0;
                        return YAMS_ERR_INTERNAL;
                    }
                }

                std::string text(reinterpret_cast<const char*>(input), input_len);
                auto h = c->pool->acquireModel(model_id, std::chrono::seconds(30));
                if (!h) {
                    std::string errMsg = h.error().message;
                    spdlog::warn("[ONNX Plugin] acquireModel failed for {}: {}", model_id, errMsg);

                    // Record failure and fire event if this is first failure
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->recordFailure(modelIdStr, errMsg);
                        auto& info = c->model_failures[modelIdStr];
                        c->model_states[modelIdStr] = ProviderCtx::State::Failed;

                        if (!info.eventFired) {
                            info.eventFired = true;
                            spdlog::error("[ONNX Plugin] Model '{}' load failed, firing event: {}",
                                          model_id, errMsg);
                            try {
                                auto q =
                                    yams::daemon::InternalEventBus::instance()
                                        .get_or_create_channel<
                                            yams::daemon::InternalEventBus::ModelLoadFailedEvent>(
                                            "model.events", 256);
                                (void)q->try_push({modelIdStr, errMsg});
                            } catch (...) {
                            }
                        }
                    }
                    *out_vec = nullptr;
                    *out_dim = 0;
                    return YAMS_ERR_INTERNAL;
                }

                // Model acquired successfully - clear failure tracking
                {
                    std::lock_guard<std::mutex> lk(c->mu);
                    c->clearFailure(modelIdStr);
                    c->model_states[modelIdStr] = ProviderCtx::State::Ready;
                }

                auto& session = *h.value();
                auto r =
                    const_cast<yams::daemon::OnnxModelSession&>(session).generateEmbedding(text);
                if (!r) {
                    spdlog::warn("[ONNX Plugin] generateEmbedding failed for {}: {}", model_id,
                                 r.error().message);
                    return YAMS_ERR_INTERNAL;
                }
                auto& vec = r.value();
                const size_t n = vec.size();
                float* buf = static_cast<float*>(std::malloc(sizeof(float) * n));
                if (!buf)
                    return YAMS_ERR_INTERNAL;
                std::memcpy(buf, vec.data(), sizeof(float) * n);
                *out_vec = buf;
                *out_dim = n;
                return YAMS_OK;
            } catch (const std::system_error& e) {
                fprintf(stderr, "[ONNX Plugin] generate_embedding std::system_error: %s\n",
                        e.what());
                spdlog::error("[ONNX Plugin] generate_embedding std::system_error: {}", e.what());
                *out_vec = nullptr;
                *out_dim = 0;
                return YAMS_ERR_INTERNAL;
            } catch (const std::exception& e) {
                fprintf(stderr, "[ONNX Plugin] generate_embedding exception: %s\n", e.what());
                spdlog::error("[ONNX Plugin] generate_embedding exception: {}", e.what());
                *out_vec = nullptr;
                *out_dim = 0;
                return YAMS_ERR_INTERNAL;
            } catch (...) {
                fprintf(stderr, "[ONNX Plugin] generate_embedding unknown exception\n");
                spdlog::error("[ONNX Plugin] generate_embedding unknown exception");
                *out_vec = nullptr;
                *out_dim = 0;
                return YAMS_ERR_INTERNAL;
            }
        };

        vtable.free_embedding = [](void* /*self*/, float* vec, size_t /*dim*/) {
            if (vec)
                std::free(vec);
        };

        vtable.generate_embedding_batch = [](void* self, const char* model_id,
                                             const uint8_t* const* inputs, const size_t* input_lens,
                                             size_t batch_size, float** out_vecs, size_t* out_batch,
                                             size_t* out_dim) -> yams_status_t {
            spdlog::info("[ONNX Plugin] generate_embedding_batch called: model={} batch={}",
                         model_id ? model_id : "(null)", batch_size);

            if (!self || !model_id || !inputs || !input_lens || !out_vecs || !out_batch ||
                !out_dim) {
                spdlog::error("[ONNX Plugin] Invalid arguments");
                return YAMS_ERR_INVALID_ARG;
            }

            int retries = 3;
            while (retries > 0) {
                retries--;
                try {
                    auto* c = static_cast<ProviderCtx*>(self);
                    if (c->disabled) {
                        spdlog::warn("[ONNX Plugin] generate_embedding_batch: plugin disabled");
                        return YAMS_ERR_UNSUPPORTED;
                    }
                    if (!c->ready) {
                        spdlog::warn("[ONNX Plugin] generate_embedding_batch: plugin not ready");
                        *out_vecs = nullptr;
                        *out_batch = 0;
                        *out_dim = 0;
                        return YAMS_ERR_INTERNAL;
                    }
                    if (!c->pool) {
                        spdlog::warn("[ONNX Plugin] generate_embedding_batch: pool is null");
                        *out_vecs = nullptr;
                        *out_batch = 0;
                        *out_dim = 0;
                        return YAMS_ERR_INTERNAL;
                    }
                    std::string modelIdStr(model_id);

                    // Check if model is in cooldown
                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        if (c->isInCooldown(modelIdStr)) {
                            auto& info = c->model_failures[modelIdStr];
                            spdlog::warn("[ONNX Plugin] Model '{}' in cooldown ({} failures)",
                                         model_id, info.consecutiveFailures);
                            *out_vecs = nullptr;
                            *out_batch = 0;
                            *out_dim = 0;
                            return YAMS_ERR_INTERNAL;
                        }
                    }

                    std::vector<std::string> texts;
                    texts.reserve(batch_size);
                    for (size_t i = 0; i < batch_size; ++i) {
                        texts.emplace_back(reinterpret_cast<const char*>(inputs[i]), input_lens[i]);
                    }

                    spdlog::info("[ONNX Plugin] acquiring model '{}'...", model_id);
                    auto h = c->pool->acquireModel(model_id, std::chrono::seconds(30));
                    if (!h) {
                        std::string errMsg = h.error().message;
                        spdlog::error("[ONNX Plugin] acquireModel failed for '{}': {}", model_id,
                                      errMsg);

                        {
                            std::lock_guard<std::mutex> lk(c->mu);
                            c->recordFailure(modelIdStr, errMsg);
                            auto& info = c->model_failures[modelIdStr];
                            c->model_states[modelIdStr] = ProviderCtx::State::Failed;

                            if (!info.eventFired) {
                                info.eventFired = true;
                                try {
                                    auto q =
                                        yams::daemon::InternalEventBus::instance()
                                            .get_or_create_channel<yams::daemon::InternalEventBus::
                                                                       ModelLoadFailedEvent>(
                                                "model.events", 256);
                                    (void)q->try_push({modelIdStr, errMsg});
                                } catch (...) {
                                }
                            }
                        }
                        *out_vecs = nullptr;
                        *out_batch = 0;
                        *out_dim = 0;
                        return YAMS_ERR_INTERNAL;
                    }

                    {
                        std::lock_guard<std::mutex> lk(c->mu);
                        c->clearFailure(modelIdStr);
                        c->model_states[modelIdStr] = ProviderCtx::State::Ready;
                    }

                    auto& session = *h.value();
                    auto r = const_cast<yams::daemon::OnnxModelSession&>(session)
                                 .generateBatchEmbeddings(texts);
                    if (!r) {
                        spdlog::error("[ONNX Plugin] generateBatchEmbeddings failed: {}",
                                      r.error().message);
                        return YAMS_ERR_INTERNAL;
                    }
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

                } catch (const std::system_error& e) {
                    spdlog::error("[ONNX Plugin] system_error: {} (code={}). Retries: {}", e.what(),
                                  e.code().value(), retries);
                    if (retries > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        continue;
                    }
                    *out_vecs = nullptr;
                    *out_batch = 0;
                    *out_dim = 0;
                    return YAMS_ERR_INTERNAL;
                } catch (const std::exception& e) {
                    spdlog::error("[ONNX Plugin] exception: {}", e.what());
                    *out_vecs = nullptr;
                    *out_batch = 0;
                    *out_dim = 0;
                    return YAMS_ERR_INTERNAL;
                } catch (...) {
                    spdlog::error("[ONNX Plugin] unknown exception");
                    *out_vecs = nullptr;
                    *out_batch = 0;
                    *out_dim = 0;
                    return YAMS_ERR_INTERNAL;
                }
            } // end while

            return YAMS_ERR_INTERNAL;
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
                        // Standard models use hidden_size, Nomic models use n_embd
                        if (j.contains("hidden_size"))
                            return j.value("hidden_size", 0u);
                        if (j.contains("n_embd"))
                            return j.value("n_embd", 0u);
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
            else if (lm.find("mpnet") != std::string::npos ||
                     lm.find("nomic") != std::string::npos || lm.find("jina") != std::string::npos)
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
            else if (lm.find("test-model") != std::string::npos &&
                     (std::getenv("YAMS_TEST_MODE") || std::getenv("YAMS_USE_MOCK_PROVIDER")))
                dim = 384;

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

        // v1.3: Cross-encoder reranking
        vtable.score_documents = [](void* self, const char* /*reranker_model_id*/,
                                    const char* query, const char* const* documents,
                                    size_t doc_count, float** out_scores,
                                    size_t* out_count) -> yams_status_t {
            if (!self || !query || !documents || !out_scores || !out_count)
                return YAMS_ERR_INVALID_ARG;
            if (doc_count == 0) {
                *out_scores = nullptr;
                *out_count = 0;
                return YAMS_OK;
            }

            try {
                auto* c = static_cast<ProviderCtx*>(self);
                if (c->disabled)
                    return YAMS_ERR_UNSUPPORTED;

                // Lazy-init reranker session on first use
                if (!c->reranker) {
                    std::lock_guard<std::mutex> lk(c->mu);
                    if (!c->reranker) {
                        // Use the same models root as embedding models (from pool config)
                        namespace fs = std::filesystem;
                        std::string modelsRoot = c->pool ? c->pool->getModelsRoot() : "";

                        if (modelsRoot.empty()) {
                            spdlog::debug(
                                "[ONNX Plugin] No modelsRoot configured, reranker unavailable");
                            return YAMS_ERR_UNSUPPORTED;
                        }

                        // Look for common reranker models in the configured models directory
                        const std::vector<std::string> rerankerModels = {
                            "bge-reranker-v2-m3", "bge-reranker-base", "bge-reranker-large",
                            "ms-marco-MiniLM-L-12-v2"};

                        for (const auto& modelName : rerankerModels) {
                            fs::path modelPath = fs::path(modelsRoot) / modelName;
                            fs::path onnxPath = modelPath / "model.onnx";
                            if (fs::exists(onnxPath)) {
                                spdlog::info("[ONNX Plugin] Found reranker model: {}",
                                             modelPath.string());
                                yams::daemon::RerankerConfig cfg;
                                cfg.model_path = modelPath.string();
                                cfg.model_name = modelName;
                                cfg.num_threads = std::max(1u, std::thread::hardware_concurrency());
                                try {
                                    c->reranker =
                                        std::make_unique<yams::daemon::OnnxRerankerSession>(
                                            modelPath.string(), modelName, cfg);
                                    c->rerankerModelPath = modelPath.string();
                                    spdlog::info("[ONNX Plugin] Reranker session initialized: {}",
                                                 modelName);
                                } catch (const std::exception& e) {
                                    spdlog::warn("[ONNX Plugin] Failed to init reranker: {}",
                                                 e.what());
                                }
                                break;
                            }
                        }

                        if (!c->reranker) {
                            spdlog::debug("[ONNX Plugin] No reranker model found in: {}",
                                          modelsRoot);
                            return YAMS_ERR_UNSUPPORTED;
                        }
                    }
                }

                if (!c->reranker || !c->reranker->isValid()) {
                    return YAMS_ERR_UNSUPPORTED;
                }

                // Build document vector
                std::vector<std::string> docs;
                docs.reserve(doc_count);
                for (size_t i = 0; i < doc_count; ++i) {
                    docs.emplace_back(documents[i] ? documents[i] : "");
                }

                // Score documents
                auto result = c->reranker->scoreBatch(query, docs);
                if (!result) {
                    spdlog::warn("[ONNX Plugin] Reranker scoreBatch failed: {}",
                                 result.error().message);
                    return YAMS_ERR_INTERNAL;
                }

                const auto& scores = result.value();
                float* buf = static_cast<float*>(std::malloc(sizeof(float) * scores.size()));
                if (!buf)
                    return YAMS_ERR_INTERNAL;
                std::memcpy(buf, scores.data(), sizeof(float) * scores.size());
                *out_scores = buf;
                *out_count = scores.size();
                return YAMS_OK;

            } catch (const std::exception& e) {
                spdlog::error("[ONNX Plugin] score_documents exception: {}", e.what());
                return YAMS_ERR_INTERNAL;
            } catch (...) {
                spdlog::error("[ONNX Plugin] score_documents unknown exception");
                return YAMS_ERR_INTERNAL;
            }
        };

        vtable.free_scores = [](void* /*self*/, float* scores, size_t /*count*/) {
            if (scores)
                std::free(scores);
        };
    }
};

static ProviderSingleton& singleton() {
    static ProviderSingleton s;
    return s;
}

} // namespace

// Set the plugin config JSON (called from yams_plugin_init before provider is accessed)
extern "C" void yams_onnx_set_config_json(const char* json) {
    if (json && *json) {
        g_plugin_config_json = json;
        spdlog::info("[ONNX Plugin] Config JSON set: {}", g_plugin_config_json);
    }
}

extern "C" void yams_onnx_shutdown_provider() {
    singleton().explicitShutdownCalled.store(true);
    singleton().shutdown(true);
}

extern "C" yams_model_provider_v1* yams_onnx_get_model_provider() {
    return &singleton().vtable;
}

// Provide a small JSON health snapshot for the ABI host
extern "C" const char* yams_onnx_get_health_json_cstr() {
    static thread_local std::string json;
    auto& c = singleton().ctx;
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

    // Add model states for diagnostics
    {
        std::lock_guard<std::mutex> lk(c.mu);
        if (!c.model_states.empty() || !c.model_failures.empty()) {
            nlohmann::json models = nlohmann::json::object();
            for (const auto& [modelId, state] : c.model_states) {
                nlohmann::json m;
                switch (state) {
                    case ProviderCtx::State::Unloaded:
                        m["state"] = "unloaded";
                        break;
                    case ProviderCtx::State::Loading:
                        m["state"] = "loading";
                        break;
                    case ProviderCtx::State::Ready:
                        m["state"] = "ready";
                        break;
                    case ProviderCtx::State::Failed:
                        m["state"] = "failed";
                        break;
                }
                // Add failure info if present
                auto fit = c.model_failures.find(modelId);
                if (fit != c.model_failures.end() && fit->second.consecutiveFailures > 0) {
                    m["failures"] = fit->second.consecutiveFailures;
                    m["last_error"] = fit->second.lastError;
                    m["in_cooldown"] = c.isInCooldown(modelId);
                }
                models[modelId] = m;
            }
            j["models"] = models;
        }
    }

    try {
        json = j.dump();
    } catch (...) {
        json = "{\"status\":\"unknown\"}";
    }
    return json.c_str();
}

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "daemon_preflight.h"
#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static std::string rand_id(size_t n = 8) {
    static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, 35);
    std::string out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i)
        out.push_back(cs[dist(rng)]);
    return out;
}

static bool model_available(std::string* out_models_root = nullptr) {
    const char* home = std::getenv("HOME");
    if (!home)
        return false;
    fs::path p = fs::path(home) / ".yams" / "models" / "all-MiniLM-L6-v2" / "model.onnx";
    if (out_models_root) {
        *out_models_root = (fs::path(home) / ".yams" / "models").string();
    }
    return fs::exists(p) && fs::is_regular_file(p);
}

TEST(DaemonEmbeddingsIT, LiveLoadModel_AddDocument_GeneratesEmbeddings) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    // Prefer mock provider mode for stability on CI/macOS where ONNX Runtime
    // can crash or be unavailable. This exercises the daemon/provider plumbing
    // without depending on native ONNX binaries. To use a real ONNX runtime,
    // set YAMS_USE_MOCK_PROVIDER=0 (or any falsey value) before running.
    if (const char* v = std::getenv("YAMS_USE_MOCK_PROVIDER"); !(v && *v)) {
        ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 0);
    }
    // Also disable ABI plugin autoload to prevent dlopen of native modules.
    if (const char* d = std::getenv("YAMS_DISABLE_ABI_PLUGINS"); !(d && *d)) {
        ::setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 0);
    }

    std::string models_root;
    if (!model_available(&models_root)) {
        GTEST_SKIP() << "Required model not found under ~/.yams/models/all-MiniLM-L6-v2/model.onnx";
    }

    auto id = rand_id();
    fs::path tmp = fs::temp_directory_path() / ("yams_embed_it_" + id);
    fs::create_directories(tmp);
    fs::path sock = tmp / ("daemon_" + id + ".sock");
    fs::path pid = tmp / ("daemon_" + id + ".pid");
    fs::path log = tmp / ("daemon_" + id + ".log");
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    cfg.enableModelProvider = true;
    // Prefer mock provider in tests; skip plugin autoload to avoid dlopen issues on hosts
    // without a compatible ONNX runtime.
    cfg.autoLoadPlugins = false;
    // Resolve pluginDir for ONNX plugin. We must point at the directory containing the
    // compiled shared library (build/<preset>/plugins/onnx), not the source directory
    // (plugins/onnx) which only has .cpp/.h files. Pointing at the source dir causes the
    // loader to scan and find zero .so/.dylib files, leaving no provider registered and
    // later operations segfault when assuming a provider exists. Prefer build tree path.
    // Strategy:
    //  1. If CMAKE_BINARY_DIR defined, try <CMAKE_BINARY_DIR>/plugins/onnx first.
    //  2. Else/also search for build/*/plugins/onnx containing libyams_onnx_plugin.*
    //  3. Only fall back to source plugins/onnx if no built artifact directory found.
    auto has_plugin_lib = [](const fs::path& dir) {
        if (dir.empty() || !fs::exists(dir) || !fs::is_directory(dir))
            return false;
        for (auto& e : fs::directory_iterator(dir)) {
            if (!e.is_regular_file())
                continue;
            auto fn = e.path().filename().string();
#ifdef __APPLE__
            if (fn.find("libyams_onnx_plugin.dylib") != std::string::npos ||
                fn.find("libyams_onnx_plugin.so") != std::string::npos)
                return true;
#else
            if (fn.find("libyams_onnx_plugin.so") != std::string::npos)
                return true;
#endif
        }
        return false;
    };

#ifdef CMAKE_BINARY_DIR
    do {
        fs::path candidate = fs::path(CMAKE_BINARY_DIR) / "plugins" / "onnx";
        if (has_plugin_lib(candidate)) {
            cfg.pluginDir = candidate;
            break;
        }
    } while (false);
#endif
    if (cfg.pluginDir.empty()) {
        try {
            fs::path cur = fs::current_path();
            for (int depth = 0; depth < 8 && cfg.pluginDir.empty(); ++depth) {
                // Try nested build/*/plugins/onnx with compiled artifact
                fs::path buildDir = cur / "build";
                if (fs::exists(buildDir) && fs::is_directory(buildDir)) {
                    for (auto& entry : fs::directory_iterator(buildDir)) {
                        if (!entry.is_directory())
                            continue;
                        fs::path candidate2 = entry.path() / "plugins" / "onnx";
                        if (has_plugin_lib(candidate2)) {
                            cfg.pluginDir = candidate2;
                            break;
                        }
                    }
                }
                if (!cfg.pluginDir.empty())
                    break;
                // As a last resort (dev scenario) allow source dir only if nothing else found.
                fs::path candidateSrc = cur / "plugins" / "onnx";
                if (cfg.pluginDir.empty() && fs::exists(candidateSrc) &&
                    fs::is_directory(candidateSrc)) {
                    cfg.pluginDir =
                        candidateSrc; // may lack binaries; test will still fail gracefully
                }
                cur = cur.parent_path();
                if (cur.empty() || cur == cur.parent_path())
                    break;
            }
        } catch (...) {
        }
    }
    cfg.modelPoolConfig.modelsRoot = models_root;
    cfg.modelPoolConfig.lazyLoading = false; // mimic keep_model_hot=true
    cfg.modelPoolConfig.preloadModels = {"all-MiniLM-L6-v2"};

    // Debug: print resolved pluginDir and log file for troubleshooting plugin loading.
    std::cerr << "[TEST DEBUG] cfg.pluginDir="
              << (cfg.pluginDir.empty() ? std::string{"<empty>"} : cfg.pluginDir.string())
              << " logFile=" << log.string() << std::endl;

    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << "Failed to start daemon: " << started.error().message;

    struct Stopper {
        yams::daemon::YamsDaemon* d;
        ~Stopper() {
            if (d) {
                (void)d->stop();
            }
        }
    } stopper{&daemon};

    yams::daemon::ClientConfig ccfg;
    ccfg.socketPath = sock;
    ccfg.requestTimeout = 10s;
    ccfg.headerTimeout = 10s;
    ccfg.bodyTimeout = 20s;
    ccfg.autoStart = false;
    yams::daemon::DaemonClient client(ccfg);

    bool ready = false;
    int maxAttempts = 100;
    if (const char* envAttempts = std::getenv("YAMS_TEST_READY_ATTEMPTS")) {
        try {
            maxAttempts = std::max(1, std::stoi(envAttempts));
        } catch (...) {
        }
    }
    // Preflight: wait for daemon then ensure embeddings provider usable
    std::string reason;
    if (!yams::test::wait_for_daemon_ready(client, 20s, nullptr)) {
        GTEST_SKIP() << "Daemon not ready; skipping embeddings tests";
    }
    if (!yams::test::ensure_embeddings_provider(client, 5s, "all-MiniLM-L6-v2", &reason)) {
        GTEST_SKIP() << "No usable embedding provider: " << reason;
    }

    yams::daemon::LoadModelRequest lreq;
    lreq.modelName = "all-MiniLM-L6-v2";
    lreq.preload = true;
    auto lres = yams::cli::run_sync(client.loadModel(lreq), 10s);
    ASSERT_TRUE(lres) << "LoadModel failed: " << lres.error().message;

    yams::daemon::GenerateEmbeddingRequest ereq;
    ereq.text = "hello world";
    ereq.modelName = "all-MiniLM-L6-v2";
    ereq.normalize = true;
    auto eres = yams::cli::run_sync(client.generateEmbedding(ereq), 20s);
    ASSERT_TRUE(eres) << "GenerateEmbedding failed: " << eres.error().message;
    EXPECT_GT(eres.value().embedding.size(), 0u);
    EXPECT_GT(eres.value().dimensions, 0u);

    yams::daemon::GetStatsRequest sreq;
    auto s0 = yams::cli::run_sync(client.getStats(sreq), 5s);
    ASSERT_TRUE(s0);
    size_t base_vec = s0.value().vectorIndexSize;

    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = "This is a small test document. It should produce an embedding.";
    add.name = "it_embed_doc_" + id + ".txt";
    add.mimeType = "text/plain";
    add.noEmbeddings = false;

    auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 30s);
    ASSERT_TRUE(addRes) << "AddDocument failed: " << addRes.error().message;
    ASSERT_FALSE(addRes.value().hash.empty());

    bool grew = false;
    for (int i = 0; i < 200; ++i) {
        auto s1 = yams::cli::run_sync(client.getStats(sreq), 5s);
        if (s1 && s1.value().vectorIndexSize > base_vec) {
            grew = true;
            break;
        }
        // If provider becomes degraded, fail fast to avoid waiting
        if (s1) {
            const auto& st = s1.value();
            auto it = st.additionalStats.find("service_embeddingservice");
            if (it != st.additionalStats.end() && it->second == "unavailable") {
                break;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(grew) << "Vector index size did not increase after adding document";
}

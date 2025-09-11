// Integration: ensure embeddings increase vector index and persist across restart (ONNX plugin)
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static std::optional<fs::path> find_plugin() {
    if (const char* env = std::getenv("TEST_ONNX_PLUGIN_FILE")) {
        fs::path p(env);
        if (fs::exists(p))
            return p;
    }
#ifdef yams_onnx_plugin_BUILT
    {
        fs::path p = fs::path(yams_onnx_plugin_BUILT);
        if (fs::exists(p))
            return p;
    }
#endif
    return std::nullopt;
}

static bool find_model_root(std::string& out_root) {
    const char* home = std::getenv("HOME");
    if (!home)
        return false;
    fs::path root = fs::path(home) / ".yams" / "models";
    if (fs::exists(root / "all-MiniLM-L6-v2" / "model.onnx")) {
        out_root = root.string();
        return true;
    }
    return false;
}

TEST(EmbeddingsPersistIT, VectorIndexPersistsAfterRestart) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }
    auto plugin = find_plugin();
    if (!plugin)
        GTEST_SKIP() << "ONNX plugin not available";
    std::string models_root;
    if (!find_model_root(models_root))
        GTEST_SKIP() << "Model not found";

    fs::path tmp = fs::temp_directory_path() / ("yams_vec_it_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    // Config for plugin
    fs::path xdg = tmp / "cfg";
    fs::create_directories(xdg / "yams");
    std::ofstream(xdg / "yams" / "config.toml")
        << "[embeddings]\npreferred_model=\"all-MiniLM-L6-v2\"\nmodel_path=\"" << models_root
        << "\"\nkeep_model_hot=true\n";
    ::setenv("XDG_CONFIG_HOME", xdg.string().c_str(), 1);

    auto run_once = [&](size_t& out_index_size) {
        fs::path sock = tmp / "d.sock";
        fs::path pid = tmp / "d.pid";
        fs::path log = tmp / "d.log";
        fs::path data = tmp / "data";
        fs::create_directories(data);
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data;
        cfg.socketPath = sock;
        cfg.pidFile = pid;
        cfg.logFile = log;
        cfg.enableModelProvider = true;
        yams::daemon::YamsDaemon daemon(cfg);
        auto started = daemon.start();
        ASSERT_TRUE(started) << started.error().message;
        auto guard = std::unique_ptr<void, void (*)(void*)>{
            &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};
        yams::daemon::ClientConfig cc;
        cc.socketPath = sock;
        cc.requestTimeout = 20s;
        cc.autoStart = false;
        yams::daemon::DaemonClient client(cc);
        // ready
        for (int i = 0; i < 50; ++i) {
            auto st = yams::cli::run_sync(client.status(), 500ms);
            if (st && (st.value().ready || st.value().overallStatus == "Ready")) {
                break;
            }
            std::this_thread::sleep_for(100ms);
        }
        // Trust and load plugin
        yams::daemon::PluginTrustAddRequest tr;
        tr.path = plugin->parent_path().string();
        ASSERT_TRUE(yams::cli::run_sync(client.call(tr), 5s));
        yams::daemon::PluginLoadRequest pl;
        pl.pathOrName = plugin->string();
        pl.dryRun = false;
        ASSERT_TRUE(yams::cli::run_sync(client.call(pl), 10s));
        // Wait for model provider adoption
        bool mp_ready = false;
        for (int i = 0; i < 60; ++i) {
            auto st = yams::cli::run_sync(client.status(), 1s);
            if (st && st.value().readinessStates.count("model_provider") &&
                st.value().readinessStates.at("model_provider")) {
                mp_ready = true;
                break;
            }
            std::this_thread::sleep_for(500ms);
        }
        ASSERT_TRUE(mp_ready);
        // Wait for core services (content_store + metadata_repo)
        for (int i = 0; i < 50; ++i) {
            auto st = yams::cli::run_sync(client.status(), 500ms);
            if (st && st.value().readinessStates.count("content_store") &&
                st.value().readinessStates.at("content_store") &&
                st.value().readinessStates.count("metadata_repo") &&
                st.value().readinessStates.at("metadata_repo"))
                break;
            std::this_thread::sleep_for(100ms);
        }

        // Preload model
        yams::daemon::LoadModelRequest lm;
        lm.modelName = "all-MiniLM-L6-v2";
        lm.preload = true;
        (void)yams::cli::run_sync(client.call(lm), 20s);

        // Baseline index size
        auto s0 = yams::cli::run_sync(client.getStats(yams::daemon::GetStatsRequest{}), 5s);
        ASSERT_TRUE(s0);
        size_t base = s0.value().vectorIndexSize;

        // Add a couple docs to generate embeddings
        std::vector<std::string> hashes;
        for (int i = 0; i < 2; ++i) {
            yams::daemon::AddDocumentRequest add;
            add.path = "";
            add.mimeType = "text/plain";
            add.noEmbeddings = true;
            add.name = std::string("vec_doc_") + std::to_string(i) + ".txt";
            add.content = std::string("embedding test content ") + std::to_string(i);
            auto ar = yams::cli::run_sync(client.streamingAddDocument(add), 30s);
            ASSERT_TRUE(ar);
            hashes.push_back(ar.value().hash);
        }

        // Explicitly embed the documents to ensure vector growth
        yams::daemon::EmbedDocumentsRequest ed;
        ed.modelName = "all-MiniLM-L6-v2";
        ed.documentHashes = hashes;
        ed.batchSize = 8;
        ed.skipExisting = false;
        auto er = yams::cli::run_sync(client.call(ed), 60s);
        ASSERT_TRUE(er) << "EmbedDocuments failed";

        bool grew = false;
        for (int i = 0; i < 60; ++i) {
            auto s1 = yams::cli::run_sync(client.getStats(yams::daemon::GetStatsRequest{}), 5s);
            if (s1 && s1.value().vectorIndexSize > base) {
                out_index_size = s1.value().vectorIndexSize;
                grew = true;
                break;
            }
            std::this_thread::sleep_for(200ms);
        }
        EXPECT_TRUE(grew) << "Vector index did not grow";
        // guard dtor stops daemon
    };

    size_t first_size = 0;
    run_once(first_size);
    ASSERT_GT(first_size, 0u);
    // Restart and ensure index persists (>= previous)
    size_t second_size = 0;
    run_once(second_size);
    EXPECT_GE(second_size, first_size);

    cleanup();
}

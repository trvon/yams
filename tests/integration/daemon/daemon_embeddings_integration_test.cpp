#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <thread>

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
    cfg.modelPoolConfig.modelsRoot = models_root;
    cfg.modelPoolConfig.lazyLoading = false; // mimic keep_model_hot=true
    cfg.modelPoolConfig.preloadModels = {"all-MiniLM-L6-v2"};

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
    for (int i = 0; i < 100; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && (st.value().ready || st.value().overallStatus == "Ready" ||
                   st.value().overallStatus == "Degraded")) {
            ready = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    ASSERT_TRUE(ready) << "Daemon did not become ready in time";

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
    for (int i = 0; i < 50; ++i) {
        auto s1 = yams::cli::run_sync(client.getStats(sreq), 5s);
        if (s1 && s1.value().vectorIndexSize > base_vec) {
            grew = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(grew) << "Vector index size did not increase after adding document";
}

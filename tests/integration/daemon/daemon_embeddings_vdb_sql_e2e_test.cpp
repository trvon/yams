#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static std::string unique_name(const char* prefix) {
    return std::string(prefix) + std::to_string(::getpid()) + "_" + std::to_string(::time(nullptr));
}

static std::optional<std::size_t> read_sentinel_dim(const fs::path& dataDir) {
    try {
        fs::path p = dataDir / "vectors_sentinel.json";
        if (!fs::exists(p))
            return std::nullopt;
        std::ifstream in(p);
        if (!in)
            return std::nullopt;
        nlohmann::json j;
        in >> j;
        if (j.contains("embedding_dim"))
            return j["embedding_dim"].get<std::size_t>();
    } catch (...) {
    }
    return std::nullopt;
}

TEST(DaemonEmbeddingsE2E, PluginEmbeddingsPersistToVectorAndSql) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    // Stabilizers for CI/macOS
    ::setenv("YAMS_USE_MOCK_PROVIDER", "1", 0);   // prefer mock provider by default
    ::setenv("YAMS_DISABLE_ABI_PLUGINS", "1", 0); // avoid dlopen/ONNX on hosts without runtime
    ::setenv("YAMS_DISABLE_STORE_STATS", "1", 0); // avoid ContentStore stats path segfaults

    fs::path tmp = fs::temp_directory_path() / ("yams_e2e_vdb_sql_" + unique_name(""));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

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
    cfg.enableModelProvider = true; // mock provider acceptable
    cfg.autoLoadPlugins = false;    // rely on mock provider; avoid abi autoload

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

    // Wait for core readiness (content store + metadata repo). Vector DB readiness is best-effort.
    for (int i = 0; i < 80; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && st.value().readinessStates.count("content_store") &&
            st.value().readinessStates.at("content_store") &&
            st.value().readinessStates.count("metadata_repo") &&
            st.value().readinessStates.at("metadata_repo"))
            break;
        std::this_thread::sleep_for(100ms);
    }

    // Add a simple document and request embeddings
    std::string content = "e2e plugin embedding document — unicorn battery horse";
    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = content;
    add.name = unique_name("doc_") + ".txt";
    add.mimeType = "text/plain";
    add.noEmbeddings = false; // let daemon generate embeddings immediately
    auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 30s);
    ASSERT_TRUE(addRes) << "AddDocument failed: " << addRes.error().message;
    std::string hash = addRes.value().hash;
    ASSERT_FALSE(hash.empty());

    // Poll for vector index growth or vector DB hasEmbedding
    bool confirmed = false;
    for (int i = 0; i < 100; ++i) {
        // Try via daemon stats first
        auto s = yams::cli::run_sync(client.getStats(yams::daemon::GetStatsRequest{}), 2s);
        if (s && (s.value().vectorIndexSize > 0 || s.value().vectorRowsExact > 0)) {
            confirmed = true;
            break;
        }
        // Also check vector DB directly (in case metrics are gated)
        yams::vector::VectorDatabaseConfig vcfg;
        vcfg.database_path = (data / "vectors.db").string();
        auto vdb = std::make_unique<yams::vector::VectorDatabase>(vcfg);
        if (vdb->initialize() && vdb->hasEmbedding(hash)) {
            confirmed = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(confirmed) << "Embeddings not observed via index size or VDB";

    // Open VectorDatabase and validate stored embeddings for the document
    yams::vector::VectorDatabaseConfig vcfg;
    vcfg.database_path = (data / "vectors.db").string();
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(vcfg);
    ASSERT_TRUE(vdb->initialize());
    EXPECT_TRUE(vdb->hasEmbedding(hash));
    auto records = vdb->getVectorsByDocument(hash);
    ASSERT_FALSE(records.empty());

    // Determine expected dimension from sentinel (or fall back to record dim)
    std::size_t expected_dim = records[0].embedding.size();
    if (auto sdim = read_sentinel_dim(data)) {
        expected_dim = *sdim;
    }
    for (const auto& r : records) {
        EXPECT_EQ(r.embedding.size(), expected_dim);
        EXPECT_FALSE(r.content.empty());
    }

    // Verify SQL metadata DB has the document by hash
    yams::metadata::ConnectionPoolConfig pcfg;
    yams::metadata::ConnectionPool pool((data / "yams.db").string(), pcfg);
    ASSERT_TRUE(pool.initialize());
    yams::metadata::MetadataRepository repo(pool);
    auto docOpt = repo.getDocumentByHash(hash);
    ASSERT_TRUE(docOpt);
    ASSERT_TRUE(docOpt.value().has_value());
    auto doc = *docOpt.value();
    EXPECT_EQ(doc.sha256Hash, hash);
    EXPECT_EQ(doc.mimeType, std::string("text/plain"));
    // Content exists
    auto contentOpt = repo.getContent(doc.id);
    ASSERT_TRUE(contentOpt);
    ASSERT_TRUE(contentOpt.value().has_value());
    EXPECT_FALSE(contentOpt.value()->content.empty());

    // Query daemon search for a token to assert end-to-end retrieval
    yams::daemon::SearchRequest sr;
    sr.query = "unicorn"; // token present in content
    sr.limit = 5;
    auto sres = yams::cli::run_sync(client.search(sr), 10s);
    ASSERT_TRUE(sres) << "Search failed: " << sres.error().message;
    EXPECT_GE(sres.value().totalCount, 1);

    cleanup();
}

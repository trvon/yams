#include <catch2/catch_test_macros.hpp>

#include <exception>
#include <filesystem>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/test_helpers_catch2.h"

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/global_io_context.h>

namespace fs = std::filesystem;

namespace {

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

int run_cli(const std::vector<std::string>& args, std::string* output = nullptr,
            std::optional<std::string> stdinData = std::nullopt) {
    std::vector<std::string> effectiveArgs = args;
    const bool hasDataDirFlag =
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--data-dir") !=
            effectiveArgs.end() ||
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--storage") != effectiveArgs.end();
    if (!hasDataDirFlag) {
        if (const char* dataDir = std::getenv("YAMS_DATA_DIR"); dataDir && *dataDir) {
            effectiveArgs.insert(effectiveArgs.begin() + 1, std::string(dataDir));
            effectiveArgs.insert(effectiveArgs.begin() + 1, "--data-dir");
        }
    }
    int rc = 0;
    std::string captured;
    try {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        argv.reserve(effectiveArgs.size());
        for (const auto& arg : effectiveArgs) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }

        CaptureStdout capture;

        std::istringstream in;
        std::streambuf* oldIn = nullptr;
        if (stdinData.has_value()) {
            in.str(*stdinData);
            oldIn = std::cin.rdbuf(in.rdbuf());
        }

        {
            rc = cli.run(static_cast<int>(argv.size()), argv.data());
        }

        if (oldIn) {
            std::cin.rdbuf(oldIn);
        }
        if (captured.empty()) {
            captured = capture.str();
        }
    } catch (const std::exception& e) {
        rc = -1;
        captured = std::string("EXCEPTION: ") + e.what();
    } catch (...) {
        rc = -1;
        captured = "EXCEPTION: unknown";
    }
    if (output) {
        *output = std::move(captured);
    }
    return rc;
}

yams::metadata::DocumentInfo makeDocumentWithPath(const std::string& path,
                                                  const std::string& hash) {
    yams::metadata::DocumentInfo info;
    info.filePath = path;
    info.fileName = fs::path(path).filename().string();
    info.fileExtension = fs::path(path).extension().string();
    info.fileSize = 123;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    info.contentExtracted = true;
    info.extractionStatus = yams::metadata::ExtractionStatus::Success;
    auto derived = yams::metadata::computePathDerivedValues(path);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    return info;
}

struct StoredTopologyFixture {
    fs::path dataDir;
    std::string snapshotId;
    std::string firstClusterId;
};

StoredTopologyFixture createStoredTopologyFixture(const fs::path& root) {
    using namespace yams::metadata;
    using namespace yams::topology;

    StoredTopologyFixture fixture;
    fixture.dataDir = root / "data";
    fs::create_directories(fixture.dataDir);
    const fs::path dbPath = fixture.dataDir / "yams.db";

    ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;

    auto pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
    REQUIRE(pool->initialize().has_value());

    auto repository = std::make_shared<MetadataRepository>(*pool);
    auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
    REQUIRE(kgResult.has_value());
    auto kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());

    REQUIRE(repository->insertDocument(makeDocumentWithPath((root / "src/a.cpp").string(), "aaa"))
                .has_value());
    REQUIRE(repository->insertDocument(makeDocumentWithPath((root / "src/b.cpp").string(), "bbb"))
                .has_value());
    REQUIRE(
        repository->insertDocument(makeDocumentWithPath((root / "include/c.hpp").string(), "ccc"))
            .has_value());

    ConnectedComponentTopologyEngine engine;
    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = (root / "src/a.cpp").string(),
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = (root / "src/b.cpp").string(),
            .neighbors = {{.documentHash = "aaa", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc", .filePath = (root / "include/c.hpp").string(), .neighbors = {}},
    };
    auto batchResult = engine.buildArtifacts(docs, TopologyBuildConfig{});
    REQUIRE(batchResult.has_value());

    MetadataKgTopologyArtifactStore store(repository, kgStore);
    REQUIRE(store.storeBatch(batchResult.value()).has_value());

    fixture.snapshotId = batchResult.value().snapshotId;
    REQUIRE_FALSE(batchResult.value().clusters.empty());
    fixture.firstClusterId = batchResult.value().clusters.front().clusterId;

    kgStore.reset();
    repository.reset();
    pool->shutdown();
    pool.reset();
    return fixture;
}

} // namespace

TEST_CASE("IntegrationSmoke.GraphCommandFallsBackToInProcessWhenDaemonUnavailable",
          "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_fallback_");
    const fs::path dataDir = root / "data";
    const fs::path blockedSocketDir = root / "blocked-socket";
    fs::create_directories(dataDir);
    fs::create_directories(blockedSocketDir);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar daemonSocket("YAMS_DAEMON_SOCKET",
                                          (blockedSocketDir / "daemon.sock").string());

    std::error_code ec;
    fs::permissions(blockedSocketDir, fs::perms::none, fs::perm_options::replace, ec);

    std::string out;
    const int rc = run_cli({"yams", "graph", "--list-types", "--json"}, &out);

    fs::permissions(blockedSocketDir, fs::perms::owner_all, fs::perm_options::replace, ec);

    INFO(out);
    CHECK(rc == 0);
    INFO(out);
    CHECK(out.find("Connection failed") == std::string::npos);
    INFO(out);
    CHECK(out.find("Operation not permitted") == std::string::npos);
}

TEST_CASE("IntegrationSmoke.GraphCommandRespectsForcedSocketMode", "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_socket_forced_");
    const fs::path dataDir = root / "data";
    const fs::path blockedSocketDir = root / "blocked-socket";
    fs::create_directories(dataDir);
    fs::create_directories(blockedSocketDir);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("0"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar daemonSocket("YAMS_DAEMON_SOCKET",
                                          (blockedSocketDir / "daemon.sock").string());

    std::error_code ec;
    fs::permissions(blockedSocketDir, fs::perms::none, fs::perm_options::replace, ec);

    std::string out;
    const int rc = run_cli({"yams", "graph", "--list-types", "--json"}, &out);

    fs::permissions(blockedSocketDir, fs::perms::owner_all, fs::perm_options::replace, ec);

    INFO(out);
    CHECK(rc != 0);
}

TEST_CASE("IntegrationSmoke.GraphTopologyModesReadStoredSnapshot", "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_topology_");
    const auto fixture = createStoredTopologyFixture(root);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", fixture.dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", fixture.dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    std::string snapshotOut;
    const int snapshotRc =
        run_cli({"yams", "graph", "--topology-snapshots", "--json"}, &snapshotOut);
    INFO(snapshotOut);
    REQUIRE(snapshotRc == 0);
    auto snapshotJson = nlohmann::json::parse(snapshotOut);
    CHECK(snapshotJson["snapshot"]["snapshot_id"] == fixture.snapshotId);
    CHECK(snapshotJson["snapshot"]["cluster_count"].get<std::size_t>() >= 1);

    std::string clustersOut;
    const int clustersRc =
        run_cli({"yams", "graph", "--topology-clusters", "--json"}, &clustersOut);
    INFO(clustersOut);
    REQUIRE(clustersRc == 0);
    auto clustersJson = nlohmann::json::parse(clustersOut);
    CHECK(clustersJson["snapshot_id"] == fixture.snapshotId);
    REQUIRE_FALSE(clustersJson["clusters"].empty());
    CHECK(clustersJson["clusters"][0].contains("role_summary"));
    CHECK(clustersJson["clusters"][0].contains("scoped_member_count"));

    std::string clusterOut;
    const int clusterRc =
        run_cli({"yams", "graph", "--cluster", fixture.firstClusterId, "--json"}, &clusterOut);
    INFO(clusterOut);
    REQUIRE(clusterRc == 0);
    auto clusterJson = nlohmann::json::parse(clusterOut);
    CHECK(clusterJson["snapshot_id"] == fixture.snapshotId);
    CHECK(clusterJson["cluster"]["cluster_id"] == fixture.firstClusterId);
    CHECK(clusterJson["cluster"].contains("role_summary"));
    CHECK(clusterJson["cluster"].contains("role_counts"));
    REQUIRE(clusterJson["members"].is_array());
    REQUIRE_FALSE(clusterJson["members"].empty());
}

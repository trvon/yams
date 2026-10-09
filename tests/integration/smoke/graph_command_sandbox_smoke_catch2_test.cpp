#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <exception>
#include <filesystem>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/test_helpers_catch2.h"

#include <yams/api/content_store_builder.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>

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

class ScopedCurrentPath {
public:
    explicit ScopedCurrentPath(const fs::path& target) : original_(fs::current_path()) {
        fs::current_path(target);
    }

    ~ScopedCurrentPath() {
        std::error_code error;
        fs::current_path(original_, error);
    }

    ScopedCurrentPath(const ScopedCurrentPath&) = delete;
    ScopedCurrentPath& operator=(const ScopedCurrentPath&) = delete;

private:
    fs::path original_;
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

// Source-location fields a graph node carries in its properties for explore hydration.
struct TestSymbol {
    std::string documentHash;
    std::string filePath;
    std::string symbolName;
    std::string qualifiedName;
    std::string kind;
    std::optional<std::int32_t> startLine;
    std::optional<std::int32_t> endLine;
};

std::string symbolNodeProperties(const TestSymbol& sym) {
    nlohmann::json props{{"file_path", sym.filePath}, {"qualified_name", sym.qualifiedName}};
    if (sym.startLine) {
        props["start_line"] = *sym.startLine;
    }
    if (sym.endLine) {
        props["end_line"] = *sym.endLine;
    }
    return props.dump();
}

std::string symbolNodeKey(const TestSymbol& sym) {
    return sym.kind + ":" + sym.qualifiedName + "@" + sym.filePath;
}

TestSymbol makeSymbol(const fs::path& path, const std::string& hash, const std::string& name,
                      const std::string& qualifiedName, std::int32_t startLine,
                      std::int32_t endLine) {
    TestSymbol sym;
    sym.documentHash = hash;
    sym.filePath = path.string();
    sym.symbolName = name;
    sym.qualifiedName = qualifiedName;
    sym.kind = "function";
    sym.startLine = startLine;
    sym.endLine = endLine;
    return sym;
}

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

void createGraphExploreFixture(const fs::path& root) {
    using namespace yams::metadata;

    const fs::path dataDir = root / "data";
    const fs::path sourceDir = root / "repo" / "src";
    const fs::path foreignSourceDir = root / "foreign" / "src";
    fs::create_directories(dataDir);
    fs::create_directories(sourceDir);
    fs::create_directories(foreignSourceDir);
    const fs::path sourcePath = sourceDir / "explore.cpp";
    const fs::path foreignSourcePath = foreignSourceDir / "explore.cpp";
    yams::test::write_file(sourcePath, "int exploreTarget() {\n"
                                       "    return 7;\n"
                                       "}\n"
                                       "int exploreEntry() {\n"
                                       "    return exploreTarget();\n"
                                       "}\n");
    yams::test::write_file(foreignSourcePath, "int exploreTarget() {\n"
                                              "    return 99;\n"
                                              "}\n"
                                              "int exploreEntry() {\n"
                                              "    return exploreTarget();\n"
                                              "}\n");

    const fs::path dbPath = dataDir / "yams.db";
    ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
    REQUIRE(pool->initialize().has_value());

    auto repository = std::make_shared<MetadataRepository>(*pool);
    auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
    REQUIRE(kgResult.has_value());
    auto kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());
    repository->setKnowledgeGraphStore(kgStore);

    REQUIRE(repository->insertDocument(makeDocumentWithPath(sourcePath.string(), "explore-hash"))
                .has_value());
    REQUIRE(repository
                ->insertDocument(
                    makeDocumentWithPath(foreignSourcePath.string(), "foreign-explore-hash"))
                .has_value());

    auto entry = makeSymbol(sourcePath, "explore-hash", "exploreEntry", "demo::exploreEntry", 4, 6);
    auto target =
        makeSymbol(sourcePath, "explore-hash", "exploreTarget", "demo::exploreTarget", 1, 3);
    auto foreignTarget = makeSymbol(foreignSourcePath, "foreign-explore-hash", "exploreTarget",
                                    "foreign::exploreTarget", 1, 3);
    auto foreignEntry = makeSymbol(foreignSourcePath, "foreign-explore-hash", "exploreEntry",
                                   "foreign::exploreEntry", 4, 6);

    KGNode entryNode;
    entryNode.nodeKey = symbolNodeKey(entry);
    entryNode.label = entry.symbolName;
    entryNode.type = entry.kind;
    entryNode.properties = symbolNodeProperties(entry);
    const auto entryId = kgStore->upsertNode(entryNode);
    REQUIRE(entryId.has_value());

    KGNode targetNode;
    targetNode.nodeKey = symbolNodeKey(target);
    targetNode.label = target.symbolName;
    targetNode.type = target.kind;
    targetNode.properties = symbolNodeProperties(target);
    const auto targetId = kgStore->upsertNode(targetNode);
    REQUIRE(targetId.has_value());

    KGEdge edge;
    edge.srcNodeId = entryId.value();
    edge.dstNodeId = targetId.value();
    edge.relation = "call";
    edge.weight = 1.0F;
    REQUIRE(kgStore->addEdge(edge).has_value());

    KGNode foreignEntryNode;
    foreignEntryNode.nodeKey = symbolNodeKey(foreignEntry);
    foreignEntryNode.label = foreignEntry.symbolName;
    foreignEntryNode.type = foreignEntry.kind;
    foreignEntryNode.properties = symbolNodeProperties(foreignEntry);
    const auto foreignEntryId = kgStore->upsertNode(foreignEntryNode);
    REQUIRE(foreignEntryId.has_value());

    KGNode foreignTargetNode;
    foreignTargetNode.nodeKey = symbolNodeKey(foreignTarget);
    foreignTargetNode.label = foreignTarget.symbolName;
    foreignTargetNode.type = foreignTarget.kind;
    foreignTargetNode.properties = symbolNodeProperties(foreignTarget);
    const auto foreignTargetId = kgStore->upsertNode(foreignTargetNode);
    REQUIRE(foreignTargetId.has_value());

    KGEdge foreignEdge;
    foreignEdge.srcNodeId = foreignEntryId.value();
    foreignEdge.dstNodeId = foreignTargetId.value();
    foreignEdge.relation = "call";
    foreignEdge.weight = 1.0F;
    REQUIRE(kgStore->addEdge(foreignEdge).has_value());

    kgStore.reset();
    repository.reset();
    pool->shutdown();
    pool.reset();
}

// Content hashes of the document graph fixture's files.
struct DocumentGraphFixture {
    std::string xa, ya, n1, n2, xb, yb, nb;
};

// Two checkouts that share relative paths, with semantic-neighbour edges between document
// nodes as v0.20 ingestion writes them. root/a/include/x.hpp also has the path:file: node that
// `yams doctor repair --graph` adds.
DocumentGraphFixture createDocumentGraphFixture(const fs::path& root) {
    using namespace yams::metadata;

    const fs::path dataDir = root / "data";
    fs::create_directories(dataDir);
    ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_unique<ConnectionPool>((dataDir / "yams.db").string(), poolConfig);
    REQUIRE(pool->initialize().has_value());

    auto repository = std::make_shared<MetadataRepository>(*pool);
    auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
    REQUIRE(kgResult.has_value());
    auto kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());

    // `get` checks the blob exists, so store the content where the daemon looks for it.
    yams::api::ContentStoreConfig storeConfig;
    storeConfig.storagePath = dataDir / "storage";
    auto contentStore = yams::api::ContentStoreBuilder().withConfig(storeConfig).build();
    REQUIRE(contentStore.has_value());

    DocumentGraphFixture hashes;
    auto addDoc = [&](const std::string& rel, std::string& hash, bool newer) {
        const auto path = root / rel;
        yams::test::write_file(path, "// " + rel + "\n");
        auto stored = contentStore.value()->store(path, yams::api::ContentMetadata{});
        REQUIRE(stored.has_value());
        hash = stored.value().contentHash;
        auto info = makeDocumentWithPath(path.string(), hash);
        if (newer) {
            info.indexedTime += std::chrono::hours(1);
        }
        REQUIRE(repository->insertDocument(info).has_value());
        KGNode node;
        node.nodeKey = "doc:" + hash;
        node.label = info.filePath;
        node.type = "document";
        auto id = kgStore->upsertNode(node);
        REQUIRE(id.has_value());
        return std::pair{id.value(), info.filePath};
    };
    auto neighbour = [&](std::int64_t src, std::int64_t dst) {
        KGEdge edge;
        edge.srcNodeId = src;
        edge.dstNodeId = dst;
        edge.relation = "semantic_neighbor";
        edge.weight = 0.9F;
        REQUIRE(kgStore->addEdge(edge).has_value());
    };

    // The other checkout goes in first and is newer, so a suffix match would prefer it.
    const auto xb = addDoc("b/include/x.hpp", hashes.xb, true).first;
    const auto yb = addDoc("b/include/y.hpp", hashes.yb, true).first;
    const auto nb = addDoc("b/src/nb.cpp", hashes.nb, true).first;
    const auto [xa, xaPath] = addDoc("a/include/x.hpp", hashes.xa, false);
    const auto ya = addDoc("a/include/y.hpp", hashes.ya, false).first;
    const auto n1 = addDoc("a/src/n1.cpp", hashes.n1, false).first;
    const auto n2 = addDoc("a/src/n2.cpp", hashes.n2, false).first;
    neighbour(xa, n1);
    neighbour(xa, n2);
    neighbour(ya, n1);
    neighbour(xb, nb);
    neighbour(yb, nb);

    KGNode blob;
    blob.nodeKey = "blob:" + hashes.xa;
    blob.label = hashes.xa.substr(0, 8);
    blob.type = "blob";
    auto blobId = kgStore->upsertNode(blob);
    REQUIRE(blobId.has_value());
    KGNode file;
    file.nodeKey = "path:file:" + xaPath;
    file.label = xaPath;
    file.type = "file";
    auto fileId = kgStore->upsertNode(file);
    REQUIRE(fileId.has_value());
    KGEdge version;
    version.srcNodeId = fileId.value();
    version.dstNodeId = blobId.value();
    version.relation = "has_version";
    REQUIRE(kgStore->addEdge(version).has_value());

    kgStore.reset();
    repository.reset();
    pool->shutdown();
    pool.reset();
    return hashes;
}

std::set<std::string> relatedHashes(const nlohmann::json& payload) {
    std::set<std::string> hashes;
    for (const auto& rel : payload.at("related")) {
        hashes.insert(rel.at("hash").get<std::string>());
    }
    return hashes;
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
    CHECK((rc == 0));
    INFO(out);
    CHECK((out.find("Connection failed") == std::string::npos));
    INFO(out);
    CHECK((out.find("Operation not permitted") == std::string::npos));
}

TEST_CASE("IntegrationSmoke.GraphCommandRespectsForcedSocketMode", "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_socket_forced_");
    const fs::path dataDir = root / "data";
    const fs::path pinnedSocket = root / "missing-socket" / "daemon.sock";
    fs::create_directories(dataDir);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("0"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar daemonSocket("YAMS_DAEMON_SOCKET", pinnedSocket.string());
    yams::test::ScopedEnvVar daemonSocketPath("YAMS_DAEMON_SOCKET_PATH", pinnedSocket.string());
    yams::test::ScopedEnvVar disableAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART",
                                              std::string("1"));

    std::string out;
    const int rc = run_cli({"yams", "graph", "--list-types", "--json"}, &out);

    INFO(out);
    CHECK((rc != 0));
}

TEST_CASE("IntegrationSmoke.GraphExploreRendersAgentContext", "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_explore_");
    createGraphExploreFixture(root);
    ScopedCurrentPath cwdGuard(root / "repo");

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", (root / "data").string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", (root / "data").string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    std::string jsonOut;
    const int jsonRc = run_cli(
        {"yams", "graph", "--explore", "exploreEntry", "--max-files", "1", "--json"}, &jsonOut);
    INFO(jsonOut);
    REQUIRE((jsonRc == 0));
    auto parsed = nlohmann::json::parse(jsonOut);
    CHECK((parsed["query"] == "exploreEntry"));
    REQUIRE_FALSE(parsed["entrySymbols"].empty());
    CHECK((parsed["entrySymbols"][0]["label"] == "exploreEntry"));
    CHECK((parsed["entrySymbols"].size() == 1));
    CHECK((parsed["entrySymbols"][0]["qualifiedName"] == "demo::exploreEntry"));
    REQUIRE_FALSE(parsed["files"].empty());
    CHECK((parsed["files"][0]["content"].get<std::string>().find("4\tint exploreEntry()") !=
           std::string::npos));
    REQUIRE_FALSE(parsed["relationships"].empty());
    CHECK((parsed["relationships"][0]["relation"] == "calls"));

    std::string humanOut;
    const int humanRc =
        run_cli({"yams", "graph", "--explore", "exploreEntry", "--max-files", "1"}, &humanOut);
    INFO(humanOut);
    REQUIRE((humanRc == 0));
    CHECK((humanOut.find("Graph Explore") != std::string::npos));
    CHECK((humanOut.find("exploreEntry --calls--> exploreTarget") != std::string::npos));
    CHECK((humanOut.find("4\tint exploreEntry()") != std::string::npos));

    std::string globalOut;
    const int globalRc =
        run_cli({"yams", "graph", "--explore", "exploreEntry", "--global", "--json"}, &globalOut);
    INFO(globalOut);
    REQUIRE((globalRc == 0));
    const auto global = nlohmann::json::parse(globalOut);
    CHECK((global["entrySymbols"].size() == 2));

    // --impact walked code-symbol edges that v0.20 removed; the flag is gone, not a no-op.
    std::string impactOut;
    const int impactRc =
        run_cli({"yams", "graph", "--impact", "exploreTarget", "--json"}, &impactOut);
    INFO(impactOut);
    CHECK((impactRc != 0));
}

TEST_CASE("IntegrationSmoke.GraphTopologyModesReadStoredSnapshot", "[smoke][integrationsmoke]") {
    const fs::path root = yams::test::make_temp_dir("yams_graph_topology_");
    const auto fixture = createStoredTopologyFixture(root);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
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
    REQUIRE((snapshotRc == 0));
    auto snapshotJson = nlohmann::json::parse(snapshotOut);
    CHECK((snapshotJson["snapshot"]["snapshot_id"] == fixture.snapshotId));
    CHECK((snapshotJson["snapshot"]["cluster_count"].get<std::size_t>() >= 1));

    std::string clustersOut;
    const int clustersRc =
        run_cli({"yams", "graph", "--topology-clusters", "--json"}, &clustersOut);
    INFO(clustersOut);
    REQUIRE((clustersRc == 0));
    auto clustersJson = nlohmann::json::parse(clustersOut);
    CHECK((clustersJson["snapshot_id"] == fixture.snapshotId));
    REQUIRE_FALSE(clustersJson["clusters"].empty());
    CHECK(clustersJson["clusters"][0].contains("role_summary"));
    CHECK(clustersJson["clusters"][0].contains("scoped_member_count"));

    std::string clusterOut;
    const int clusterRc =
        run_cli({"yams", "graph", "--cluster", fixture.firstClusterId, "--json"}, &clusterOut);
    INFO(clusterOut);
    REQUIRE((clusterRc == 0));
    auto clusterJson = nlohmann::json::parse(clusterOut);
    CHECK((clusterJson["snapshot_id"] == fixture.snapshotId));
    CHECK((clusterJson["cluster"]["cluster_id"] == fixture.firstClusterId));
    CHECK(clusterJson["cluster"].contains("role_summary"));
    CHECK(clusterJson["cluster"].contains("role_counts"));
    REQUIRE(clusterJson["members"].is_array());
    REQUIRE_FALSE(clusterJson["members"].empty());
}

TEST_CASE("IntegrationSmoke.GraphNameResolvesRelativePathAgainstCwd", "[smoke][integrationsmoke]") {
    // #280 follow-up: --name probed path:file:<path> keys first and, when one existed, printed a
    // raw path-node traversal instead of the document's related documents. The in-process
    // daemon shares the client cwd, so the relative-path case guards the cwd-relative contract
    // here; a separate daemon resolved it by suffix to the newer copy in the other checkout.
    const fs::path root = yams::test::make_temp_dir("yams_graph_name_");
    const auto hashes = createDocumentGraphFixture(root);
    ScopedCurrentPath cwdGuard(root / "a");

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", (root / "data").string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", (root / "data").string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    auto lookup = [](const std::vector<std::string>& extra) {
        std::vector<std::string> args{"yams", "graph"};
        args.insert(args.end(), extra.begin(), extra.end());
        args.push_back("--json");
        std::string out;
        const int rc = run_cli(args, &out);
        INFO(out);
        REQUIRE((rc == 0));
        return nlohmann::json::parse(out);
    };

    SECTION("a path with a path:file: node still returns its related documents") {
        const auto payload = lookup({"--name", "include/x.hpp", "--depth", "1"});
        CHECK((payload.value("hash", "") == hashes.xa));
        CHECK((relatedHashes(payload) == std::set<std::string>{hashes.n1, hashes.n2}));
    }

    SECTION("a relative path resolves under the cwd, not to a newer copy elsewhere") {
        const auto payload = lookup({"--name", "include/y.hpp"});
        CHECK((payload.value("hash", "") == hashes.ya));
        CHECK((relatedHashes(payload) == std::set<std::string>{hashes.n1}));
    }

    SECTION("a bare file name still resolves by name") {
        const auto payload = lookup({"--name", "n2.cpp"});
        CHECK((payload.value("hash", "") == hashes.n2));
    }
}

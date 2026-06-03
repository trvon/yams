// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/graph_context_service.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

using namespace yams::app::services;
using namespace yams::metadata;

namespace {

struct GraphContextServiceFixture {
    GraphContextServiceFixture() {
        const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
        testDir = std::filesystem::temp_directory_path() /
                  ("yams_graph_context_service_test_" + std::to_string(nonce));
        std::filesystem::create_directories(testDir);
        dbPath = testDir / "test.db";

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        auto poolInitRes = pool->initialize();
        REQUIRE(poolInitRes.has_value());

        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        KnowledgeGraphStoreConfig kgConfig;
        kgConfig.enable_alias_fts = true;
        kgConfig.enable_wal = false;
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, kgConfig);
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(std::move(kgResult).value());
        metadataRepo->setKnowledgeGraphStore(kgStore);
    }

    ~GraphContextServiceFixture() {
        kgStore.reset();
        metadataRepo.reset();
        pool.reset();
        std::filesystem::remove_all(testDir);
    }

    std::filesystem::path writeSource(const std::string& relativePath,
                                      const std::vector<std::string>& lines) {
        auto path = testDir / relativePath;
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path);
        REQUIRE(out.good());
        for (const auto& line : lines) {
            out << line << '\n';
        }
        return path;
    }

    SymbolMetadata symbol(const std::filesystem::path& path, std::string name,
                          std::string qualifiedName, std::int32_t startLine, std::int32_t endLine,
                          std::string kind = "function") {
        SymbolMetadata sym;
        sym.documentHash = "hash-" + qualifiedName;
        sym.filePath = path.string();
        sym.symbolName = std::move(name);
        sym.qualifiedName = std::move(qualifiedName);
        sym.kind = std::move(kind);
        sym.startLine = startLine;
        sym.endLine = endLine;
        return sym;
    }

    void insertDocumentFor(const SymbolMetadata& sym) {
        DocumentInfo doc;
        doc.filePath = sym.filePath;
        const auto derived = computePathDerivedValues(doc.filePath);
        doc.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
        doc.fileExtension = std::filesystem::path(derived.normalizedPath).extension().string();
        doc.fileSize = std::filesystem::exists(sym.filePath)
                           ? static_cast<std::int64_t>(std::filesystem::file_size(sym.filePath))
                           : 0;
        doc.sha256Hash = sym.documentHash;
        doc.mimeType = "text/plain";
        doc.pathPrefix = derived.pathPrefix;
        doc.reversePath = derived.reversePath;
        doc.pathHash = derived.pathHash;
        doc.parentHash = derived.parentHash;
        doc.pathDepth = derived.pathDepth;
        const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.createdTime = now;
        doc.modifiedTime = now;
        doc.indexedTime = now;
        auto inserted = metadataRepo->insertDocument(doc);
        REQUIRE(inserted.has_value());
    }

    void upsertSymbols(const std::vector<SymbolMetadata>& symbols) {
        for (const auto& sym : symbols) {
            insertDocumentFor(sym);
        }
        REQUIRE(kgStore->upsertSymbolMetadata(symbols).has_value());
    }

    std::int64_t upsertNodeFor(const SymbolMetadata& sym) {
        KGNode node;
        node.nodeKey = sym.kind + ":" + sym.qualifiedName + "@" + sym.filePath;
        node.label = sym.symbolName;
        node.type = sym.kind;
        auto nodeId = kgStore->upsertNode(node);
        REQUIRE(nodeId.has_value());
        return nodeId.value();
    }

    std::filesystem::path testDir;
    std::filesystem::path dbPath;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

} // namespace

TEST_CASE("GraphContextService explore ranks source symbols before tests by default",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath = fixture.writeSource("src/process.cpp", {
                                                                 "namespace demo {",
                                                                 "int helper() { return 1; }",
                                                                 "int processTask() {",
                                                                 "    return helper();",
                                                                 "}",
                                                                 "}",
                                                             });
    auto testPath = fixture.writeSource("tests/process_task_test.cpp",
                                        {
                                            "#include <catch2/catch_test_macros.hpp>",
                                            "TEST_CASE(\"processTask\") {",
                                            "    REQUIRE(processTask() == 1);",
                                            "}",
                                        });

    auto sourceSym = fixture.symbol(sourcePath, "processTask", "demo::processTask", 3, 5);
    auto testSym = fixture.symbol(testPath, "processTask", "demo::processTaskTest", 2, 4);
    fixture.upsertSymbols({sourceSym, testSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = "processTask";
    req.budget.maxFiles = 4;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE(result.has_value());
    const auto& response = result.value();

    REQUIRE(response.entrySymbols.size() == 1);
    CHECK(response.entrySymbols.front().filePath == sourcePath.string());
    REQUIRE(response.files.size() == 1);
    CHECK(response.files.front().filePath == sourcePath.string());
    CHECK(response.files.front().content.find("3\tint processTask()") != std::string::npos);
    CHECK(response.files.front().content.find("5\t}") != std::string::npos);
}

TEST_CASE("GraphContextService explore can include tests when requested",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/process.cpp", {"int processTask() {", "return 1;", "}"});
    auto testPath = fixture.writeSource("tests/process_task_test.cpp",
                                        {"TEST_CASE(\"processTask\") {", "processTask();", "}"});

    auto sourceSym = fixture.symbol(sourcePath, "processTask", "demo::processTask", 1, 3);
    auto testSym = fixture.symbol(testPath, "processTask", "demo::processTaskTest", 1, 3);
    fixture.upsertSymbols({sourceSym, testSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = "processTask";
    req.includeTests = true;
    req.budget.maxFiles = 4;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE(result.has_value());
    const auto& response = result.value();

    REQUIRE(response.entrySymbols.size() == 2);
    CHECK(response.totalFilesConsidered == 2);
    REQUIRE(response.files.size() == 2);
    CHECK(response.files[0].filePath == sourcePath.string());
    CHECK(response.files[1].filePath == testPath.string());
}

TEST_CASE("GraphContextService explore applies snippet budgets and omits code when requested",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath = fixture.writeSource("src/budget.cpp", {
                                                                "int budgetedSymbol() {",
                                                                "    int value = 1;",
                                                                "    value += 2;",
                                                                "    value += 3;",
                                                                "    return value;",
                                                                "}",
                                                            });
    auto sourceSym = fixture.symbol(sourcePath, "budgetedSymbol", "demo::budgetedSymbol", 1, 6);
    fixture.upsertSymbols({sourceSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest limitedReq;
    limitedReq.query = "budgetedSymbol";
    limitedReq.budget.maxSnippetLines = 2;
    limitedReq.budget.maxCharsPerFile = 10;
    limitedReq.budget.maxTotalChars = 10;

    auto limited = service->explore(limitedReq);
    REQUIRE(limited.has_value());
    REQUIRE(limited.value().files.size() == 1);
    CHECK(limited.value().truncated);
    CHECK(limited.value().files.front().truncated);
    CHECK(limited.value().files.front().content.size() <= 10);

    GraphExploreRequest omittedReq;
    omittedReq.query = "budgetedSymbol";
    omittedReq.includeCode = false;

    auto omitted = service->explore(omittedReq);
    REQUIRE(omitted.has_value());
    REQUIRE(omitted.value().files.size() == 1);
    CHECK(omitted.value().files.front().mode == GraphContextSnippetMode::Omitted);
    CHECK(omitted.value().files.front().content.empty());
}

TEST_CASE("GraphContextService explore clamps zero-based symbol lines",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/zero_lines.cpp", {"int zeroBased() {", "    return 42;", "}"});
    auto zeroLineSym = fixture.symbol(sourcePath, "zeroBased", "demo::zeroBased", 0, 0);
    fixture.upsertSymbols({zeroLineSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = "zeroBased";

    auto result = service->explore(req);
    REQUIRE(result.has_value());
    REQUIRE(result.value().files.size() == 1);
    CHECK(result.value().files.front().startLine == 1);
    CHECK(result.value().files.front().endLine == 3);
    CHECK(result.value().files.front().content.find("1\tint zeroBased() {") != std::string::npos);
}

TEST_CASE("GraphContextService explore returns canonical relationship context",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto callerPath =
        fixture.writeSource("src/caller.cpp", {"int caller() {", "return callee();", "}"});
    auto calleePath = fixture.writeSource("src/callee.cpp", {"int callee() {", "return 1;", "}"});
    auto callerSym = fixture.symbol(callerPath, "caller", "demo::caller", 1, 3);
    auto calleeSym = fixture.symbol(calleePath, "callee", "demo::callee", 1, 3);
    fixture.upsertSymbols({callerSym, calleeSym});
    const auto callerId = fixture.upsertNodeFor(callerSym);
    const auto calleeId = fixture.upsertNodeFor(calleeSym);

    KGEdge edge;
    edge.srcNodeId = callerId;
    edge.dstNodeId = calleeId;
    edge.relation = "call";
    edge.weight = 0.75F;
    REQUIRE(fixture.kgStore->addEdge(edge).has_value());

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = "caller";
    auto result = service->explore(req);
    REQUIRE(result.has_value());

    const auto& response = result.value();
    REQUIRE(response.relationships.size() == 1);
    CHECK(response.relationships.front().relation == "calls");
    CHECK(response.relationships.front().sourceLabel == "caller");
    CHECK(response.relationships.front().targetLabel == "callee");
}

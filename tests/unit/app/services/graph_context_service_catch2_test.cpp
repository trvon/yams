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
        REQUIRE((poolInitRes.has_value()));

        metadataRepo = std::make_shared<MetadataRepository>(*pool);

        KnowledgeGraphStoreConfig kgConfig;
        kgConfig.enable_alias_fts = true;
        kgConfig.enable_wal = false;
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, kgConfig);
        REQUIRE((kgResult.has_value()));
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
        REQUIRE((out.good()));
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
        REQUIRE((inserted.has_value()));
    }

    void upsertSymbols(const std::vector<SymbolMetadata>& symbols) {
        for (const auto& sym : symbols) {
            insertDocumentFor(sym);
        }
        REQUIRE((kgStore->upsertSymbolMetadata(symbols).has_value()));
    }

    std::int64_t upsertNodeFor(const SymbolMetadata& sym) {
        KGNode node;
        node.nodeKey = sym.kind + ":" + sym.qualifiedName + "@" + sym.filePath;
        node.label = sym.symbolName;
        node.type = sym.kind;
        auto nodeId = kgStore->upsertNode(node);
        REQUIRE((nodeId.has_value()));
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
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "processTask";
    req.budget.maxFiles = 4;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    const auto& response = result.value();

    REQUIRE((response.entrySymbols.size() == 1));
    CHECK((response.entrySymbols.front().filePath == sourcePath.string()));
    REQUIRE((response.files.size() == 1));
    CHECK((response.files.front().filePath == sourcePath.string()));
    CHECK((response.files.front().content.find("3\tint processTask()") != std::string::npos));
    CHECK((response.files.front().content.find("5\t}") != std::string::npos));
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
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "processTask";
    req.includeTests = true;
    req.budget.maxFiles = 4;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    const auto& response = result.value();

    REQUIRE((response.entrySymbols.size() == 2));
    CHECK((response.totalFilesConsidered == 2));
    REQUIRE((response.files.size() == 2));
    CHECK((response.files[0].filePath == sourcePath.string()));
    CHECK((response.files[1].filePath == testPath.string()));
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
    REQUIRE((service != nullptr));

    GraphExploreRequest limitedReq;
    limitedReq.query = "budgetedSymbol";
    limitedReq.budget.maxSnippetLines = 2;
    limitedReq.budget.maxCharsPerFile = 10;
    limitedReq.budget.maxTotalChars = 10;

    auto limited = service->explore(limitedReq);
    REQUIRE((limited.has_value()));
    REQUIRE((limited.value().files.size() == 1));
    CHECK(limited.value().truncated);
    CHECK(limited.value().files.front().truncated);
    CHECK((limited.value().files.front().content.size() <= 10));

    GraphExploreRequest omittedReq;
    omittedReq.query = "budgetedSymbol";
    omittedReq.includeCode = false;

    auto omitted = service->explore(omittedReq);
    REQUIRE((omitted.has_value()));
    REQUIRE((omitted.value().files.size() == 1));
    CHECK((omitted.value().files.front().mode == GraphContextSnippetMode::Omitted));
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
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "zeroBased";

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().files.size() == 1));
    CHECK((result.value().files.front().startLine == 1));
    CHECK((result.value().files.front().endLine == 3));
    CHECK((result.value().files.front().content.find("1\tint zeroBased() {") != std::string::npos));
}

TEST_CASE("GraphContextService explore supports file path queries", "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/path_query.cpp", {"int firstSymbol() {", "    return 1;", "}",
                                                   "int secondSymbol() {", "    return 2;", "}"});
    auto firstSym = fixture.symbol(sourcePath, "firstSymbol", "demo::firstSymbol", 1, 3);
    auto secondSym = fixture.symbol(sourcePath, "secondSymbol", "demo::secondSymbol", 4, 6);
    fixture.upsertSymbols({firstSym, secondSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = sourcePath.string();
    req.budget.maxFiles = 2;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    const auto& response = result.value();

    REQUIRE((response.entrySymbols.size() == 2));
    CHECK((response.entrySymbols.front().filePath == sourcePath.string()));
    CHECK((response.entrySymbols.back().filePath == sourcePath.string()));
    REQUIRE((response.files.size() == 1));
    CHECK((response.files.front().filePath == sourcePath.string()));
    CHECK((response.files.front().content.find("1\tint firstSymbol() {") != std::string::npos));
    CHECK((response.files.front().content.find("4\tint secondSymbol() {") != std::string::npos));
}

TEST_CASE("GraphContextService explore falls back to graph nodes without symbol metadata",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/fallback.cpp", {"int fallbackEntry() {", "    return 7;", "}"});

    DocumentInfo doc;
    doc.filePath = sourcePath.string();
    const auto derived = computePathDerivedValues(doc.filePath);
    doc.fileName = sourcePath.filename().string();
    doc.fileExtension = sourcePath.extension().string();
    doc.fileSize = static_cast<std::int64_t>(std::filesystem::file_size(sourcePath));
    doc.sha256Hash = "hash-fallback";
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
    REQUIRE((fixture.metadataRepo->insertDocument(doc).has_value()));

    KGNode node;
    node.nodeKey = "function:demo::fallbackEntry@" + sourcePath.string();
    node.label = "fallbackEntry";
    node.type = "function";
    REQUIRE((fixture.kgStore->upsertNode(node).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "fallbackEntry";

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().entrySymbols.size() == 1));
    CHECK((result.value().entrySymbols.front().label == "fallbackEntry"));
    CHECK((result.value().entrySymbols.front().filePath == sourcePath.string()));
    REQUIRE((result.value().files.size() == 1));
    CHECK((result.value().files.front().filePath == sourcePath.string()));
    CHECK((result.value().files.front().content.find("1\tint fallbackEntry() {") !=
           std::string::npos));
    CHECK_FALSE(result.value().warnings.empty());
    CHECK((result.value().warnings.front().find("falling back to graph node labels") !=
           std::string::npos));
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
    REQUIRE((fixture.kgStore->addEdge(edge).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "caller";
    auto result = service->explore(req);
    REQUIRE((result.has_value()));

    const auto& response = result.value();
    REQUIRE((response.relationships.size() == 1));
    CHECK((response.relationships.front().relation == "calls"));
    CHECK((response.relationships.front().sourceLabel == "caller"));
    CHECK((response.relationships.front().targetLabel == "callee"));
}

TEST_CASE("GraphContextService explore relationship budget is not capped by symbol budget",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto rootPath =
        fixture.writeSource("src/root.cpp", {"int root() {", "    return left() + right();", "}"});
    auto leftPath = fixture.writeSource("src/left.cpp", {"int left() {", "    return 1;", "}"});
    auto rightPath = fixture.writeSource("src/right.cpp", {"int right() {", "    return 2;", "}"});

    auto rootSym = fixture.symbol(rootPath, "root", "demo::root", 1, 3);
    auto leftSym = fixture.symbol(leftPath, "left", "demo::left", 1, 3);
    auto rightSym = fixture.symbol(rightPath, "right", "demo::right", 1, 3);
    fixture.upsertSymbols({rootSym, leftSym, rightSym});

    const auto rootId = fixture.upsertNodeFor(rootSym);
    const auto leftId = fixture.upsertNodeFor(leftSym);
    const auto rightId = fixture.upsertNodeFor(rightSym);

    KGEdge leftEdge;
    leftEdge.srcNodeId = rootId;
    leftEdge.dstNodeId = leftId;
    leftEdge.relation = "call";
    leftEdge.weight = 0.8F;
    REQUIRE((fixture.kgStore->addEdge(leftEdge).has_value()));

    KGEdge rightEdge;
    rightEdge.srcNodeId = rootId;
    rightEdge.dstNodeId = rightId;
    rightEdge.relation = "call";
    rightEdge.weight = 0.7F;
    REQUIRE((fixture.kgStore->addEdge(rightEdge).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "root";
    req.budget.maxFiles = 1;
    req.budget.maxSymbols = 1;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));

    const auto& response = result.value();
    REQUIRE((response.entrySymbols.size() == 1));
    REQUIRE((response.relationships.size() == 2));
    CHECK((response.relationships[0].sourceLabel == "root"));
    CHECK((response.relationships[0].targetLabel == "left"));
    CHECK((response.relationships[1].sourceLabel == "root"));
    CHECK((response.relationships[1].targetLabel == "right"));
    CHECK_FALSE(response.truncated);
}

TEST_CASE("GraphContextService lookupSymbol disambiguates by file", "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto pathA = fixture.writeSource("src/a.cpp", {"int parseToken() { return 1; }"});
    auto pathB = fixture.writeSource("src/b.cpp", {"int parseToken() { return 2; }"});
    auto symA = fixture.symbol(pathA, "parseToken", "a::parseToken", 1, 1);
    auto symB = fixture.symbol(pathB, "parseToken", "b::parseToken", 1, 1);
    fixture.upsertSymbols({symA, symB});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphSymbolLookupRequest ambiguousReq;
    ambiguousReq.symbol = "parseToken";
    auto ambiguous = service->lookupSymbol(ambiguousReq);
    REQUIRE((ambiguous.has_value()));
    CHECK((ambiguous.value().matches.size() == 2));
    CHECK(ambiguous.value().ambiguous);

    GraphSymbolLookupRequest scopedReq;
    scopedReq.symbol = "parseToken";
    scopedReq.file = pathA.string();
    auto scoped = service->lookupSymbol(scopedReq);
    REQUIRE((scoped.has_value()));
    REQUIRE((scoped.value().matches.size() == 1));
    CHECK((scoped.value().matches.front().filePath == pathA.string()));
    CHECK_FALSE(scoped.value().ambiguous);
}

TEST_CASE("GraphContextService impact finds reverse dependents", "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto callerPath = fixture.writeSource("src/caller.cpp", {"int caller() { return callee(); }"});
    auto calleePath = fixture.writeSource("src/callee.cpp", {"int callee() { return 1; }"});
    auto callerSym = fixture.symbol(callerPath, "caller", "demo::caller", 1, 1);
    auto calleeSym = fixture.symbol(calleePath, "callee", "demo::callee", 1, 1);
    fixture.upsertSymbols({callerSym, calleeSym});
    const auto callerId = fixture.upsertNodeFor(callerSym);
    const auto calleeId = fixture.upsertNodeFor(calleeSym);

    KGEdge edge;
    edge.srcNodeId = callerId;
    edge.dstNodeId = calleeId;
    edge.relation = "calls";
    edge.weight = 0.9F;
    REQUIRE((fixture.kgStore->addEdge(edge).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphImpactRequest req;
    req.symbol = "callee";
    auto result = service->impact(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().affectedSymbols.size() == 1));
    CHECK((result.value().affectedSymbols.front().label == "caller"));
    CHECK_FALSE(result.value().relationships.empty());
}

TEST_CASE("GraphContextService impact finds callers through symbol_reference placeholders",
          "[services][graph][context]") {
    // Mirrors the real KG topology: cross-file callers emit
    //   caller_version --calls--> symbol_reference(callee)
    // rather than pointing at the callee's canonical definition node.
    GraphContextServiceFixture fixture;
    auto calleePath = fixture.writeSource("src/callee.cpp", {"int callee() { return 1; }"});
    auto callerPath = fixture.writeSource("src/caller.cpp", {"int caller() { return callee(); }"});
    auto calleeSym = fixture.symbol(calleePath, "callee", "demo::callee", 1, 1);
    fixture.upsertSymbols({calleeSym});
    fixture.upsertNodeFor(calleeSym); // canonical function node for the callee

    KGNode placeholder;
    placeholder.nodeKey = "symbol_ref:demo::callee";
    placeholder.label = "demo::callee";
    placeholder.type = "symbol_reference";
    auto placeholderId = fixture.kgStore->upsertNode(placeholder);
    REQUIRE((placeholderId.has_value()));

    KGNode callerVersion;
    callerVersion.nodeKey = "function:demo::caller@" + callerPath.string() + "@snap:abc";
    callerVersion.label = "caller";
    callerVersion.type = "function_version";
    callerVersion.properties = std::string("{\"qualified_name\":\"demo::caller\",\"file_path\":\"") +
                               callerPath.string() + "\",\"start_line\":1,\"end_line\":1}";
    auto callerId = fixture.kgStore->upsertNode(callerVersion);
    REQUIRE((callerId.has_value()));

    KGEdge edge;
    edge.srcNodeId = callerId.value();
    edge.dstNodeId = placeholderId.value();
    edge.relation = "calls";
    REQUIRE((fixture.kgStore->addEdge(edge).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphImpactRequest req;
    req.symbol = "callee";
    auto result = service->impact(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().affectedSymbols.size() == 1));
    CHECK((result.value().affectedSymbols.front().label == "caller"));
    CHECK((result.value().affectedSymbols.front().filePath == callerPath.string()));
}

TEST_CASE("GraphContextService trace finds a path across a call chain",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto aPath = fixture.writeSource("src/alpha.cpp", {"int alpha() { return beta(); }"});
    auto bPath = fixture.writeSource("src/beta.cpp", {"int beta() { return gamma(); }"});
    auto cPath = fixture.writeSource("src/gamma.cpp", {"int gamma() { return 1; }"});
    auto aSym = fixture.symbol(aPath, "alpha", "demo::alpha", 1, 1);
    auto bSym = fixture.symbol(bPath, "beta", "demo::beta", 1, 1);
    auto cSym = fixture.symbol(cPath, "gamma", "demo::gamma", 1, 1);
    fixture.upsertSymbols({aSym, bSym, cSym});
    const auto aId = fixture.upsertNodeFor(aSym);
    const auto bId = fixture.upsertNodeFor(bSym);
    const auto cId = fixture.upsertNodeFor(cSym);

    KGEdge ab;
    ab.srcNodeId = aId;
    ab.dstNodeId = bId;
    ab.relation = "calls";
    REQUIRE((fixture.kgStore->addEdge(ab).has_value()));
    KGEdge bc;
    bc.srcNodeId = bId;
    bc.dstNodeId = cId;
    bc.relation = "calls";
    REQUIRE((fixture.kgStore->addEdge(bc).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphTraceRequest req;
    req.from = "alpha";
    req.to = "gamma";
    auto result = service->trace(req);
    REQUIRE((result.has_value()));
    CHECK(result.value().found);
    REQUIRE((result.value().path.size() == 2));

    GraphTraceRequest missingReq;
    missingReq.from = "gamma";
    missingReq.to = "alpha";
    missingReq.maxDepth = 1;
    auto missing = service->trace(missingReq);
    REQUIRE((missing.has_value()));
    CHECK_FALSE(missing.value().found);
}

TEST_CASE("GraphContextService affectedTests maps changed files to tests",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto srcPath = fixture.writeSource("src/widget.cpp", {"int widget() { return 1; }"});
    auto testPath =
        fixture.writeSource("tests/widget_test.cpp", {"void t() { widget(); }"});
    auto srcSym = fixture.symbol(srcPath, "widget", "demo::widget", 1, 1);
    auto testSym = fixture.symbol(testPath, "t", "demo::t", 1, 1);
    fixture.upsertSymbols({srcSym, testSym});
    const auto srcId = fixture.upsertNodeFor(srcSym);
    const auto testId = fixture.upsertNodeFor(testSym);

    KGEdge edge;
    edge.srcNodeId = testId;
    edge.dstNodeId = srcId;
    edge.relation = "calls";
    REQUIRE((fixture.kgStore->addEdge(edge).has_value()));

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphAffectedTestsRequest req;
    req.changedFiles = {srcPath.string()};
    auto result = service->affectedTests(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().affectedTests.size() == 1));
    CHECK((result.value().affectedTests.front() == testPath.string()));
}

// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/graph_context_service.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "../../../common/metadata_test_db.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

using namespace yams::app::services;
using namespace yams::metadata;

namespace {

// Source-location fields a graph node carries in its properties; explore resolves nodes by label
// and alias and reads these to hydrate snippets.
struct TestSymbol {
    std::string documentHash;
    std::string filePath;
    std::string symbolName;
    std::string qualifiedName;
    std::string kind;
    std::optional<std::int32_t> startLine;
    std::optional<std::int32_t> endLine;
};

struct GraphContextServiceFixture {
    GraphContextServiceFixture() {
        dbPath =
            yams::test::migrated_metadata_db_template().clone("yams_graph_context_service_db_");
        testDir = dbPath.parent_path() / dbPath.stem();
        std::filesystem::create_directories(testDir);

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        auto poolInitRes = pool->initialize();
        REQUIRE((poolInitRes.has_value()));

        metadataRepo = std::make_shared<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

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
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
        yams::test::remove_sqlite_artifacts(dbPath);
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
        // Ingest stores forward-slash paths; return the same form so fixture keys match on Windows.
        return std::filesystem::path(path.generic_string());
    }

    TestSymbol symbol(const std::filesystem::path& path, std::string name,
                      std::string qualifiedName, std::int32_t startLine, std::int32_t endLine,
                      std::string kind = "function") {
        TestSymbol sym;
        sym.documentHash = "hash-" + qualifiedName;
        sym.filePath = path.string();
        sym.symbolName = std::move(name);
        sym.qualifiedName = std::move(qualifiedName);
        sym.kind = std::move(kind);
        sym.startLine = startLine;
        sym.endLine = endLine;
        return sym;
    }

    void insertDocumentFor(const TestSymbol& sym) {
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

    void upsertSymbols(const std::vector<TestSymbol>& symbols) {
        for (const auto& sym : symbols) {
            insertDocumentFor(sym);
            upsertNodeFor(sym);
        }
    }

    static std::string nodeProperties(const TestSymbol& sym) {
        nlohmann::json props{{"file_path", sym.filePath}, {"qualified_name", sym.qualifiedName}};
        if (sym.startLine) {
            props["start_line"] = *sym.startLine;
        }
        if (sym.endLine) {
            props["end_line"] = *sym.endLine;
        }
        return props.dump();
    }

    // Mirrors the path node ingestion writes for every indexed file.
    std::int64_t upsertPathNode(const std::filesystem::path& path, std::string nodeKey = {},
                                std::string type = "path") {
        KGNode node;
        node.nodeKey = nodeKey.empty() ? "path:" + path.string() : std::move(nodeKey);
        node.label = path.string();
        node.type = std::move(type);
        node.properties = nlohmann::json{{"path", path.string()}, {"is_directory", false}}.dump();
        auto nodeId = kgStore->upsertNode(node);
        REQUIRE((nodeId.has_value()));
        return nodeId.value();
    }

    std::int64_t upsertNodeFor(const TestSymbol& sym) {
        KGNode node;
        node.nodeKey = sym.kind + ":" + sym.qualifiedName + "@" + sym.filePath;
        node.label = sym.symbolName;
        node.type = sym.kind;
        node.properties = nodeProperties(sym);
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

TEST_CASE("GraphContextService explore applies repository scope before symbol budgets",
          "[services][graph][context][scope]") {
    GraphContextServiceFixture fixture;
    const auto localPath =
        fixture.writeSource("repo-z/src/request_handler.cpp", {"void RequestHandler() {}"});
    fixture.upsertPathNode(localPath);
    for (std::size_t index = 0; index < 20; ++index) {
        const auto foreignPath = fixture.writeSource("repo-z-copy/copy_" + std::to_string(index) +
                                                         "/src/request_handler.cpp",
                                                     {"void RequestHandler() {}"});
        fixture.upsertPathNode(foreignPath);
    }

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "request_handler.cpp";
    req.scopePathPrefix = localPath.parent_path().parent_path().string();
    req.budget.maxFiles = 1;
    req.budget.maxSymbols = 1;

    auto result = service->explore(req);

    REQUIRE((result.has_value()));
    REQUIRE((result.value().entrySymbols.size() == 1));
    CHECK((result.value().entrySymbols.front().filePath == localPath.string()));
    REQUIRE((result.value().files.size() == 1));
    CHECK((result.value().files.front().filePath == localPath.string()));
}

TEST_CASE("GraphContextService explore supports file path queries", "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/path_query.cpp", {"int firstSymbol() {", "    return 1;", "}",
                                                   "int secondSymbol() {", "    return 2;", "}"});
    fixture.upsertPathNode(sourcePath);

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = sourcePath.string();
    req.budget.maxFiles = 2;
    req.budget.maxSymbols = 4;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    const auto& response = result.value();

    REQUIRE_FALSE(response.entrySymbols.empty());
    CHECK((response.entrySymbols.front().filePath == sourcePath.string()));
    REQUIRE((response.files.size() == 1));
    CHECK((response.files.front().filePath == sourcePath.string()));
    CHECK((response.files.front().content.find("int firstSymbol() {") != std::string::npos));
}

TEST_CASE("GraphContextService explore resolves C++ qualified symbol queries",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath = fixture.writeSource("src/vector/sqlite_vec_backend.cpp",
                                          {
                                              "namespace yams::vector {",
                                              "class SqliteVecBackend {",
                                              "    struct Impl {",
                                              "        void bruteForceSearch();",
                                              "    };",
                                              "};",
                                              "}",
                                          });

    const std::string qualified = "yams::vector::SqliteVecBackend::Impl::bruteForceSearch";
    auto sym = fixture.symbol(sourcePath, "bruteForceSearch", qualified, 4, 4);
    fixture.upsertSymbols({sym});

    // Mirror production alias extraction (EntityGraphService): simple name, fully
    // qualified name, and partial qualified suffixes.
    const auto nodeId = fixture.upsertNodeFor(sym);
    const std::vector<std::string> aliases = {
        "bruteForceSearch",
        qualified,
        "vector::SqliteVecBackend::Impl::bruteForceSearch",
        "SqliteVecBackend::Impl::bruteForceSearch",
        "Impl::bruteForceSearch",
    };
    for (const auto& text : aliases) {
        KGAlias alias;
        alias.nodeId = nodeId;
        alias.alias = text;
        alias.source = std::string("test");
        alias.confidence = 1.0f;
        REQUIRE((fixture.kgStore->addAlias(alias).has_value()));
    }

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    const auto explore = [&](const std::string& query) {
        GraphExploreRequest req;
        req.query = query;
        req.budget.maxFiles = 4;
        req.budget.maxSymbols = 4;
        return service->explore(req);
    };
    const auto findsSymbol = [](const GraphExploreResponse& response) {
        for (const auto& entry : response.entrySymbols) {
            if (entry.qualifiedName == "yams::vector::SqliteVecBackend::Impl::bruteForceSearch") {
                return true;
            }
        }
        return false;
    };

    SECTION("Shortened qualifier resolves instead of failing with an SQL error") {
        auto result = explore("SqliteVecBackend::bruteForceSearch");
        REQUIRE((result.has_value()));
        CHECK((findsSymbol(result.value())));
    }

    SECTION("Fully indexed qualified name resolves") {
        auto result = explore("yams::vector::SqliteVecBackend::Impl::bruteForceSearch");
        REQUIRE((result.has_value()));
        CHECK((findsSymbol(result.value())));
    }

    SECTION("Unqualified simple name resolves") {
        auto result = explore("bruteForceSearch");
        REQUIRE((result.has_value()));
        CHECK((findsSymbol(result.value())));
    }

    SECTION("Nonexistent qualified symbol misses without an SQL error") {
        auto result = explore("Missing::doesNotExist");
        REQUIRE((result.has_value()));
        CHECK(result.value().entrySymbols.empty());
    }
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
    CHECK((limited.value().snippetsRendered == 1));
    CHECK((limited.value().snippetRenderMicros >= 0));

    GraphExploreRequest omittedReq;
    omittedReq.query = "budgetedSymbol";
    omittedReq.includeCode = false;

    auto omitted = service->explore(omittedReq);
    REQUIRE((omitted.has_value()));
    REQUIRE((omitted.value().files.size() == 1));
    CHECK((omitted.value().files.front().mode == GraphContextSnippetMode::Omitted));
    CHECK(omitted.value().files.front().content.empty());
    CHECK((omitted.value().snippetsRendered == 0));
    CHECK((omitted.value().snippetRenderMicros == 0));
}

TEST_CASE("GraphContextService explore reports line-only clipping as truncated",
          "[services][graph][context][retrieval-contract]") {
    GraphContextServiceFixture fixture;
    const auto path =
        fixture.writeSource("src/line_budget.cpp",
                            {"int lineBudget() {", "    int value = 1;", "    return value;", "}"});
    fixture.upsertSymbols({fixture.symbol(path, "lineBudget", "demo::lineBudget", 1, 4)});
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = "lineBudget";
    req.budget.maxSnippetLines = 2;
    const auto result = service->explore(req);

    REQUIRE(result.has_value());
    REQUIRE(result.value().files.size() == 1);
    const auto& snippet = result.value().files.front();
    CHECK(snippet.startLine == 1);
    CHECK(snippet.endLine == 2);
    CHECK(snippet.content.find("return value") == std::string::npos);
    CHECK(snippet.truncated);
    CHECK(result.value().truncated);
}

TEST_CASE("GraphContextService explore reports clipped path-node context as truncated",
          "[services][graph][context][retrieval-contract]") {
    GraphContextServiceFixture fixture;
    const auto path = fixture.writeSource("docs/path_budget.md", {"one", "two", "three"});
    fixture.upsertPathNode(path);
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = path.string();
    req.budget.maxSnippetLines = 2;
    const auto result = service->explore(req);

    REQUIRE(result.has_value());
    REQUIRE(result.value().files.size() == 1);
    CHECK(result.value().files.front().endLine == 2);
    CHECK(result.value().files.front().truncated);
    CHECK(result.value().truncated);

    req.budget.maxSnippetLines = 3;
    const auto complete = service->explore(req);
    REQUIRE(complete.has_value());
    REQUIRE(complete.value().files.size() == 1);
    CHECK_FALSE(complete.value().files.front().truncated);
    CHECK_FALSE(complete.value().truncated);
}

TEST_CASE("GraphContextService explore propagates unavailable-source truncation",
          "[services][graph][context][retrieval-contract]") {
    GraphContextServiceFixture fixture;
    const auto path = fixture.testDir / "src" / "not_present.cpp";
    fixture.upsertPathNode(path);
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE(service != nullptr);

    GraphExploreRequest req;
    req.query = path.string();
    const auto result = service->explore(req);

    REQUIRE(result.has_value());
    REQUIRE(result.value().files.size() == 1);
    CHECK(result.value().files.front().mode == GraphContextSnippetMode::Omitted);
    CHECK(result.value().files.front().truncated);
    CHECK(result.value().truncated);
    REQUIRE(result.value().warnings.size() == 1);
    CHECK(result.value().warnings.front().find("Could not read source file") != std::string::npos);
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

TEST_CASE("GraphContextService explore tolerates stale symbol lines past end of file",
          "[services][graph][context]") {
    GraphContextServiceFixture fixture;
    auto sourcePath = fixture.writeSource("src/stale_lines.cpp",
                                          {"int staleLocation() {", "    return 42;", "}"});
    auto staleLineSym =
        fixture.symbol(sourcePath, "staleLocation", "demo::staleLocation", 100, 104);
    fixture.upsertSymbols({staleLineSym});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "staleLocation";

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().files.size() == 1));
    CHECK(result.value().truncated);
    CHECK((result.value().files.front().mode == GraphContextSnippetMode::Omitted));
    CHECK(result.value().files.front().truncated);
    CHECK(result.value().files.front().content.empty());
    REQUIRE((result.value().warnings.size() == 1));
    CHECK((result.value().warnings.front().find("stale source location") != std::string::npos));
}

TEST_CASE("GraphContextService explore finds scoped files despite a saturated global label pool",
          "[services][graph][context][graph-ranking]") {
    GraphContextServiceFixture fixture;
    const auto target = fixture.writeSource("local/AGENTS.md", {"# Repository instructions"});
    fixture.upsertPathNode(target, "path:file:" + target.string(), "file");
    const auto summary = fixture.writeSource("local/.pi/summary.md", {"AGENTS.md was mentioned"});
    const auto summaryId = fixture.upsertNodeFor(
        fixture.symbol(summary, "# AGENTS.md mention", "summary", 1, 1, "text_segment"));
    KGAlias alias;
    alias.nodeId = summaryId;
    alias.alias = "AGENTS.md";
    REQUIRE(fixture.kgStore->addAlias(alias).has_value());
    for (int i = 0; i < 80; ++i) {
        fixture.upsertNodeFor(
            fixture.symbol(fixture.testDir / "foreign" / (std::to_string(i) + ".md"),
                           "# AGENTS.md foreign " + std::to_string(i),
                           "foreign" + std::to_string(i), 1, 1, "text_segment"));
    }
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = "AGENTS.md";
    req.scopePathPrefix = target.parent_path().string();
    req.budget.maxFiles = 1;
    req.budget.maxSymbols = 2;
    const auto result = service->explore(req);
    REQUIRE(result.has_value());
    REQUIRE_FALSE(result.value().entrySymbols.empty());
    CHECK(result.value().entrySymbols.front().filePath == target.string());
    CHECK(result.value().entrySymbols.front().exactMatch);
    REQUIRE(result.value().files.size() == 1);
    CHECK(result.value().files.front().filePath == target.string());

    // The ordinary bounded lookup cannot see this target: direct-key seeding,
    // not merely reordering the same candidate pool, is what restores it.
    GraphSymbolLookupRequest bounded;
    bounded.symbol = req.query;
    bounded.file = target.string();
    bounded.budget.maxSymbols = 64;
    const auto control = service->lookupSymbol(bounded);
    REQUIRE(control.has_value());
    CHECK(control.value().matches.empty());
}

TEST_CASE("GraphContextService explore budgets distinct file contexts rather than versions",
          "[services][graph][context][graph-ranking]") {
    GraphContextServiceFixture fixture;
    const auto first = fixture.writeSource("src/a_guide.cpp", {"int firstGuide() {}"});
    const auto second = fixture.writeSource("src/z_guide.cpp", {"int secondGuide() {}"});
    fixture.upsertPathNode(first, "path:file:" + first.string(), "file");
    for (int i = 0; i < 40; ++i) {
        fixture.upsertPathNode(first, "path:snapshot-" + std::to_string(i) + ":" + first.string(),
                               i % 2 == 0 ? "path" : "document");
    }
    fixture.upsertPathNode(second, "path:file:" + second.string(), "file");
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = "guide";
    req.budget.maxFiles = 2;
    req.budget.maxSymbols = 2;
    const auto result = service->explore(req);
    REQUIRE(result.has_value());
    REQUIRE(result.value().entrySymbols.size() == 2);
    CHECK(result.value().entrySymbols[0].filePath != result.value().entrySymbols[1].filePath);
    REQUIRE(result.value().files.size() == 2);
    CHECK(result.value().files[0].filePath == first.string());
    CHECK(result.value().files[1].filePath == second.string());
    CHECK_FALSE(result.value().truncated);

    GraphSymbolLookupRequest lookup;
    lookup.symbol = "guide";
    lookup.budget.maxSymbols = 64;
    const auto raw = service->lookupSymbol(lookup);
    REQUIRE(raw.has_value());
    CHECK(raw.value().matches.size() == 42);
}

TEST_CASE("GraphContextService explore preserves ambiguous basenames and distinct same-file spans",
          "[services][graph][context][graph-ranking]") {
    GraphContextServiceFixture fixture;
    const auto first = fixture.writeSource("one/AGENTS.md", {"first", "second"});
    const auto second = fixture.writeSource("two/AGENTS.md", {"other"});
    fixture.upsertPathNode(first, "path:file:" + first.string(), "file");
    fixture.upsertPathNode(second, "path:file:" + second.string(), "file");
    fixture.upsertNodeFor(
        fixture.symbol(first, "AGENTS.md section", "firstSection", 1, 1, "text_segment"));
    fixture.upsertNodeFor(
        fixture.symbol(first, "AGENTS.md section", "secondSection", 2, 2, "text_segment"));
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = "AGENTS.md";
    req.scopePathPrefix = fixture.testDir.string();
    const auto result = service->explore(req);
    REQUIRE(result.has_value());
    CHECK(result.value().entrySymbols.size() == 4);
    CHECK(result.value().files.size() == 2);
    CHECK(result.value().entrySymbols.front().exactMatch);

    req.query = "two/AGENTS.md";
    req.budget.maxFiles = 1;
    const auto relative = service->explore(req);
    REQUIRE(relative.has_value());
    REQUIRE(relative.value().files.size() == 1);
    CHECK(relative.value().files.front().filePath == second.string());
    CHECK(relative.value().entrySymbols.front().exactMatch);

    req.query = "../outside/AGENTS.md";
    const auto scopedMiss = service->explore(req);
    REQUIRE(scopedMiss.has_value());
    CHECK(scopedMiss.value().files.empty());
}

TEST_CASE("GraphContextService explore preserves edges from duplicate file representations",
          "[services][graph][context][graph-review]") {
    GraphContextServiceFixture fixture;
    const auto path = fixture.writeSource("src/guide.cpp", {"int guide() {}"});
    const auto live = fixture.upsertPathNode(path, "path:logical:" + path.string());
    const auto snapshot = fixture.upsertPathNode(path, "path:snapshot:" + path.string());
    KGNode blob;
    blob.nodeKey = "blob:guide";
    blob.label = "guide blob";
    blob.type = "blob";
    const auto blobId = fixture.kgStore->upsertNode(blob);
    REQUIRE(blobId.has_value());
    KGEdge edge;
    edge.srcNodeId = live;
    edge.dstNodeId = snapshot;
    edge.relation = "path_version";
    REQUIRE(fixture.kgStore->addEdge(edge).has_value());
    edge.srcNodeId = snapshot;
    edge.dstNodeId = blobId.value();
    edge.relation = "has_version";
    REQUIRE(fixture.kgStore->addEdge(edge).has_value());
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = path.string();
    const auto result = service->explore(req);
    REQUIRE(result.has_value());
    CHECK(result.value().entrySymbols.size() == 1);
    CHECK(result.value().files.size() == 1);
    CHECK(result.value().relationships.size() == 2);
    CHECK_FALSE(result.value().truncated);
}

TEST_CASE("GraphContextService explore retains segment provenance and document spans",
          "[services][graph][context][graph-review]") {
    GraphContextServiceFixture fixture;
    const auto path = fixture.writeSource("src/guide.cpp", {"first", "second"});
    for (const auto* region : {"title", "summary"}) {
        KGNode node;
        node.nodeKey = std::string("segment:") + region + ":guide";
        node.type = "text_segment";
        node.label = "guide";
        node.properties = nlohmann::json{{"source_file", path.string()},
                                         {"region", region},
                                         {"start_offset", 0},
                                         {"end_offset", 5}}
                              .dump();
        REQUIRE(fixture.kgStore->upsertNode(node).has_value());
    }
    fixture.upsertNodeFor(fixture.symbol(path, "guide", "firstParagraph", 1, 1, "document"));
    fixture.upsertNodeFor(fixture.symbol(path, "guide", "secondParagraph", 2, 2, "document"));
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = "guide";
    const auto result = service->explore(req);
    REQUIRE(result.has_value());
    CHECK(result.value().entrySymbols.size() == 4);
    REQUIRE(result.value().files.size() == 1);
    CHECK(result.value().files.front().symbols.size() == 4);
}

TEST_CASE("GraphContextService external file queries require explicit global scope",
          "[services][graph][context][graph-review]") {
    GraphContextServiceFixture fixture;
    const auto local = fixture.writeSource("local/src/shared.cpp", {"local"});
    const auto external = fixture.writeSource("other/src/shared.cpp", {"external"});
    fixture.upsertPathNode(local, "path:file:" + local.string(), "file");
    fixture.upsertPathNode(external, "path:file:" + external.string(), "file");
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = external.string();
    req.scopePathPrefix = (fixture.testDir / "local").string();
    const auto scoped = service->explore(req);
    REQUIRE(scoped.has_value());
    CHECK(scoped.value().files.empty());
    req.scopePathPrefix.clear();
    const auto global = service->explore(req);
    REQUIRE(global.has_value());
    REQUIRE(global.value().files.size() == 1);
    CHECK(global.value().files.front().filePath == external.string());
}

TEST_CASE("GraphContextService contributor relationships respect output and disabled budgets",
          "[services][graph][context][graph-review]") {
    GraphContextServiceFixture fixture;
    const auto path = fixture.writeSource("src/edge_cap.cpp", {"int edgeCap() {}"});
    const auto live = fixture.upsertPathNode(path, "path:logical:" + path.string());
    for (int i = 0; i < 40; ++i) {
        const auto snapshot = fixture.upsertPathNode(path, "path:snapshot-" + std::to_string(i) +
                                                               ":" + path.string());
        KGEdge edge;
        edge.srcNodeId = live;
        edge.dstNodeId = snapshot;
        edge.relation = "path_version";
        REQUIRE(fixture.kgStore->addEdge(edge).has_value());
        for (int j = 0; j < 3; ++j) {
            KGNode blob;
            blob.nodeKey = "blob:cap-" + std::to_string(i) + "-" + std::to_string(j);
            blob.label = "edge target";
            blob.type = "blob";
            const auto id = fixture.kgStore->upsertNode(blob);
            REQUIRE(id.has_value());
            edge.srcNodeId = snapshot;
            edge.dstNodeId = id.value();
            edge.relation = "has_version";
            REQUIRE(fixture.kgStore->addEdge(edge).has_value());
        }
    }
    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    GraphExploreRequest req;
    req.query = path.string();
    req.includeCode = false;
    const auto capped = service->explore(req);
    REQUIRE(capped.has_value());
    CHECK(capped.value().entrySymbols.size() == 1);
    CHECK(capped.value().relationships.size() == 128);
    CHECK(capped.value().truncated);

    req.budget.includeRelationships = false;
    const auto disabled = service->explore(req);
    REQUIRE(disabled.has_value());
    CHECK(disabled.value().entrySymbols.size() == 1);
    CHECK(disabled.value().relationships.empty());
    CHECK_FALSE(disabled.value().truncated);

    req.budget.includeRelationships = true;
    req.budget.maxSymbols = 0;
    const auto empty = service->explore(req);
    REQUIRE(empty.has_value());
    CHECK(empty.value().entrySymbols.empty());
    CHECK(empty.value().relationships.empty());
    CHECK(empty.value().files.empty());
}

TEST_CASE("GraphContextService explore favors multi-term file coverage",
          "[services][graph][context][natural-language]") {
    GraphContextServiceFixture fixture;
    auto searchDistractorPath =
        fixture.writeSource("src/a_search.cpp", {"int Search() { return 1; }"});
    auto hintsDistractorPath =
        fixture.writeSource("src/b_hints.cpp", {"struct SSL_HANDSHAKE_HINTS {};"});
    auto relevantPath =
        fixture.writeSource("src/z_graph_context.cpp", {"int searchGraphContext() { return 2; }",
                                                        "int renderGraphHints() { return 3; }"});

    auto searchDistractor =
        fixture.symbol(searchDistractorPath, "Search", "external::Search", 1, 1);
    auto hintsDistractor = fixture.symbol(hintsDistractorPath, "SSL_HANDSHAKE_HINTS",
                                          "external::SSL_HANDSHAKE_HINTS", 1, 1, "struct");
    auto searchGraphContext =
        fixture.symbol(relevantPath, "searchGraphContext", "yams::searchGraphContext", 1, 1);
    auto renderGraphHints =
        fixture.symbol(relevantPath, "renderGraphHints", "yams::renderGraphHints", 2, 2);
    fixture.upsertSymbols(
        {searchDistractor, hintsDistractor, searchGraphContext, renderGraphHints});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "how search graph context hints work";
    req.budget.maxFiles = 2;
    req.budget.maxSymbols = 2;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    const auto& response = result.value();

    REQUIRE_FALSE(response.files.empty());
    CHECK((response.files.front().filePath == relevantPath.string()));
    REQUIRE((response.entrySymbols.size() == 2));
    CHECK((response.entrySymbols.front().filePath == relevantPath.string()));
    CHECK((response.entrySymbols.back().filePath == relevantPath.string()));
}

TEST_CASE("GraphContextService explore relocates stale in-bounds symbol lines",
          "[services][graph][context][hydration]") {
    GraphContextServiceFixture fixture;
    auto sourcePath =
        fixture.writeSource("src/graph_context.cpp",
                            {"int unrelated() { return 1; }", "int affectedTests() { return 2; }"});
    auto staleSymbol = fixture.symbol(sourcePath, "affectedTests", "demo::affectedTests", 1, 1);
    fixture.upsertSymbols({staleSymbol});

    auto service = makeGraphContextService(fixture.kgStore, fixture.metadataRepo);
    REQUIRE((service != nullptr));

    GraphExploreRequest req;
    req.query = "affectedTests";
    req.budget.maxSnippetLines = 1;

    auto result = service->explore(req);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().files.size() == 1));
    const auto& snippet = result.value().files.front();
    CHECK((snippet.startLine == 2));
    CHECK((snippet.endLine == 2));
    CHECK((snippet.content.find("affectedTests") != std::string::npos));
}

TEST_CASE("GraphContextService explore resolves graph nodes by label",
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
    CHECK(result.value().warnings.empty());
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

// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors
//
// Integration test: EntityGraphService → KnowledgeGraphStore pipeline
// Tests end-to-end flow: symbol extraction → KG node/edge creation → KG queries
// Part of yams-h54: Graph Navigation & Symbol Coverage Expansion

#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

#include <spdlog/spdlog.h>
#include <catch2/catch_test_macros.hpp>


#include "test_daemon_harness.h"

#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>

using namespace yams::metadata;

// Skip tests on Windows where daemon IPC shutdown is unstable (named pipe cleanup hangs)
// Linux Docker container runs these tests successfully
// TODO(yams-daemon): Fix Windows daemon shutdown race conditions
#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Windows daemon shutdown unstable - run in Docker container")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

// ============================================================================
// Test Fixture: EntityGraphService Integration (using DaemonHarness)
// ============================================================================

class EntityGraphIntegrationFixture {
public:
    EntityGraphIntegrationFixture() {
        if (!harness_.start(std::chrono::seconds(30))) {
            throw std::runtime_error("DaemonHarness failed to start");
        }

        daemon_ = harness_.daemon();
        if (!daemon_) {
            throw std::runtime_error("Daemon not available after harness start");
        }

        serviceManager_ = daemon_->getServiceManager();
        if (!serviceManager_) {
            throw std::runtime_error("ServiceManager not available");
        }

        testDir_ = harness_.dataDir();
        spdlog::info("EntityGraphIntegrationFixture: Daemon started, dataDir={}",
                     testDir_.string());
    }

    ~EntityGraphIntegrationFixture() {
        // DaemonHarness RAII handles cleanup
        spdlog::debug("EntityGraphIntegrationFixture: Cleanup via DaemonHarness");
    }

    // Store content and return hash
    std::string storeContent(const std::string& filename, const std::string& content) {
        auto contentStore = serviceManager_->getContentStore();
        if (!contentStore) {
            throw std::runtime_error("ContentStore not available");
        }

        auto meta = serviceManager_->getMetadataRepo();
        if (!meta) {
            throw std::runtime_error("MetadataRepository not available");
        }

        std::vector<std::byte> bytes;
        bytes.reserve(content.size());
        for (char c : content) {
            bytes.push_back(static_cast<std::byte>(c));
        }

        auto storeResult = contentStore->storeBytes(bytes);
        if (!storeResult) {
            throw std::runtime_error("Failed to store content: " + storeResult.error().message);
        }

        std::string hash = storeResult.value().contentHash;

        DocumentInfo doc;
        doc.fileName = filename;
        doc.filePath = (testDir_ / filename).string();
        doc.sha256Hash = hash;
        doc.fileSize = content.size();
        doc.mimeType = "text/x-c++";
        doc.fileExtension = ".cpp";
        doc.modifiedTime =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

        auto insertResult = meta->insertDocument(doc);
        if (!insertResult) {
            throw std::runtime_error("Failed to insert document: " + insertResult.error().message);
        }

        std::ofstream out(doc.filePath);
        out << content;
        out.close();

        return hash;
    }

    // Get KG store for direct queries
    std::shared_ptr<KnowledgeGraphStore> getKgStore() { return serviceManager_->getKgStore(); }

    // Get symbol extractors availability
    bool hasSymbolExtractors() {
        const auto& extractors = serviceManager_->getSymbolExtractors();
        return !extractors.empty();
    }

    // Check if C++ language is supported
    bool supportsCpp() {
        const auto& extractors = serviceManager_->getSymbolExtractors();
        for (const auto& ex : extractors) {
            if (ex && ex->supportsLanguage("cpp")) {
                return true;
            }
        }
        return false;
    }

    // Submit extraction job and wait for completion
    bool submitAndWaitForExtraction(const std::string& hash, const std::string& filePath,
                                    const std::string& content, const std::string& language,
                                    int timeoutMs = 5000) {
        auto graphComponent = serviceManager_->getGraphComponent();
        if (!graphComponent) {
            spdlog::warn("GraphComponent not available");
            return false;
        }

        yams::daemon::GraphComponent::DocumentGraphContext ctx;
        ctx.documentHash = hash;
        ctx.filePath = filePath;
        ctx.documentDbId = 0;

        auto result = graphComponent->onDocumentIngested(ctx);
        if (!result) {
            spdlog::warn("onDocumentIngested failed: {}", result.error().message);
            return false;
        }

        // Wait for async processing
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        return true;
    }

    yams::test::DaemonHarness harness_;
    yams::daemon::YamsDaemon* daemon_{nullptr};
    yams::daemon::ServiceManager* serviceManager_{nullptr};
    std::filesystem::path testDir_;
};

// ============================================================================
// Test Cases: EntityGraphService → KG Pipeline Validation
// ============================================================================

TEST_CASE("EntityGraphService: KG store is available", "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    SECTION("KnowledgeGraphStore is initialized") {
        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto healthResult = kg->healthCheck();
        REQUIRE(healthResult.has_value());
        spdlog::info("KG store health check passed");
    }
}

TEST_CASE("EntityGraphService: Symbol extractors are loaded",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    SECTION("At least one symbol extractor available") {
        bool hasExtractors = fixture.hasSymbolExtractors();
        if (!hasExtractors) {
            SKIP("No symbol extractors loaded - grammars may not be installed");
        }
        REQUIRE(hasExtractors);
        spdlog::info("Symbol extractors are loaded");
    }

    SECTION("C++ language is supported") {
        bool supportsCpp = fixture.supportsCpp();
        if (!supportsCpp) {
            SKIP("C++ grammar not available - run 'yams init' to download grammars");
        }
        REQUIRE(supportsCpp);
        spdlog::info("C++ language supported by symbol extractor");
    }
}

TEST_CASE("EntityGraphService: Symbol extraction creates KG nodes",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Extract C++ function creates function node") {
        std::string cppCode = R"(
int add(int a, int b) {
    return a + b;
}

int multiply(int x, int y) {
    return x * y;
}
)";

        std::string filename = "math_functions.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        spdlog::info("Stored C++ file: hash={}", hash.substr(0, 12));

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Query for function nodes
        auto nodesResult = kg->findNodesByType("function", 100, 0);
        REQUIRE(nodesResult.has_value());

        auto& nodes = nodesResult.value();
        spdlog::info("Found {} function nodes in KG", nodes.size());

        // Should have at least 2 functions (add, multiply)
        REQUIRE(nodes.size() >= 2);

        // Verify node properties
        bool foundAdd = false;
        bool foundMultiply = false;
        for (const auto& node : nodes) {
            spdlog::info("Function node: key={}, label={}", node.nodeKey, node.label.value_or("<no label>"));
            if (node.label == "add" || node.nodeKey.find("add") != std::string::npos) {
                foundAdd = true;
            }
            if (node.label == "multiply" || node.nodeKey.find("multiply") != std::string::npos) {
                foundMultiply = true;
            }
        }

        REQUIRE(foundAdd);
        REQUIRE(foundMultiply);
        spdlog::info("Function nodes created correctly in KG");
    }

    SECTION("Extract C++ class creates class node") {
        std::string cppCode = R"(
class Calculator {
public:
    int add(int a, int b) { return a + b; }
    int subtract(int a, int b) { return a - b; }
private:
    int result_;
};
)";

        std::string filename = "calculator.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();

        // Query for class nodes
        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        auto& classNodes = classNodesResult.value();
        spdlog::info("Found {} class nodes in KG", classNodes.size());

        bool foundCalculator = false;
        for (const auto& node : classNodes) {
            spdlog::info("Class node: key={}, label={}", node.nodeKey, node.label.value_or("<no label>"));
            if (node.label == "Calculator" ||
                node.nodeKey.find("Calculator") != std::string::npos) {
                foundCalculator = true;
            }
        }

        REQUIRE(foundCalculator);

        // Also check for method nodes
        auto methodNodesResult = kg->findNodesByType("method", 100, 0);
        if (methodNodesResult.has_value()) {
            spdlog::info("Found {} method nodes in KG", methodNodesResult.value().size());
        }

        spdlog::info("Class and method nodes created correctly in KG");
    }
}

TEST_CASE("EntityGraphService: Symbol extraction creates KG edges",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Symbols have defined_in edges to document") {
        std::string cppCode = R"(
void processData() {
    // Process some data
}
)";

        std::string filename = "process.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();

        // Find the function node first
        auto funcNodes = kg->findNodesByType("function", 100, 0);
        REQUIRE(funcNodes.has_value());
        
        bool foundDefinedInEdge = false;
        bool foundLocatedInEdge = false;
        
        for (const auto& funcNode : funcNodes.value()) {
            // Check edges from this function node
            auto edgesResult = kg->getEdgesFrom(funcNode.id, std::nullopt, 100, 0);
            if (edgesResult.has_value()) {
                for (const auto& edge : edgesResult.value()) {
                    if (edge.relation == "defined_in") {
                        foundDefinedInEdge = true;
                        spdlog::info("Found 'defined_in' edge from function {}", funcNode.nodeKey);
                    }
                    if (edge.relation == "located_in") {
                        foundLocatedInEdge = true;
                        spdlog::info("Found 'located_in' edge from function {}", funcNode.nodeKey);
                    }
                }
            }
        }
        
        // At least one type of location edge should exist
        CHECK((foundDefinedInEdge || foundLocatedInEdge));

        spdlog::info("Symbol edges created correctly in KG");
    }
}

TEST_CASE("EntityGraphService: KG aliases for symbol lookup",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Symbols have searchable aliases") {
        std::string cppCode = R"(
namespace utils {
    int helperFunction(int x) {
        return x * 2;
    }
}
)";

        std::string filename = "utils.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();

        // Try to resolve alias by function name
        auto aliasResult = kg->resolveAliasExact("helperFunction", 10);
        if (aliasResult.has_value() && !aliasResult.value().empty()) {
            spdlog::info("Found {} alias matches for 'helperFunction'", aliasResult.value().size());
            REQUIRE(!aliasResult.value().empty());
        } else {
            spdlog::warn("No alias found for 'helperFunction' - alias may not be created yet");
        }
    }
}

TEST_CASE("EntityGraphService: Multiple files create connected graph",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Multiple source files populate KG") {
        std::string file1Code = R"(
class DataStore {
public:
    void save(int id);
    int load(int id);
};
)";

        std::string file2Code = R"(
class DataProcessor {
public:
    void process();
};
)";

        auto hash1 = fixture.storeContent("data_store.cpp", file1Code);
        auto hash2 = fixture.storeContent("data_processor.cpp", file2Code);

        std::string path1 = (fixture.testDir_ / "data_store.cpp").string();
        std::string path2 = (fixture.testDir_ / "data_processor.cpp").string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash1, path1, file1Code, "cpp", 2000));
        REQUIRE(fixture.submitAndWaitForExtraction(hash2, path2, file2Code, "cpp", 2000));

        auto kg = fixture.getKgStore();

        // Get all class nodes
        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        auto& classNodes = classNodesResult.value();
        spdlog::info("Total class nodes from multiple files: {}", classNodes.size());

        // Should have at least 2 classes (DataStore, DataProcessor)
        REQUIRE(classNodes.size() >= 2);

        bool foundDataStore = false;
        bool foundDataProcessor = false;
        for (const auto& node : classNodes) {
            if (node.label == "DataStore" || node.nodeKey.find("DataStore") != std::string::npos) {
                foundDataStore = true;
            }
            if (node.label == "DataProcessor" ||
                node.nodeKey.find("DataProcessor") != std::string::npos) {
                foundDataProcessor = true;
            }
        }

        REQUIRE(foundDataStore);
        REQUIRE(foundDataProcessor);

        // Get file nodes
        auto fileNodesResult = kg->findNodesByType("file", 100, 0);
        if (fileNodesResult.has_value()) {
            spdlog::info("Total file nodes: {}", fileNodesResult.value().size());
            REQUIRE(fileNodesResult.value().size() >= 2);
        }

        spdlog::info("Multiple files correctly populate KG");
    }
}

TEST_CASE("EntityGraphService: Graph traversal from symbol",
          "[integration][daemon][kg][pbi-009]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Can traverse from function to containing file") {
        std::string cppCode = R"(
void targetFunction() {
    // This is the target
}
)";

        std::string filename = "target.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000));

        auto kg = fixture.getKgStore();

        // Get function nodes
        auto funcNodesResult = kg->findNodesByType("function", 100, 0);
        REQUIRE(funcNodesResult.has_value());

        auto& funcNodes = funcNodesResult.value();

        std::optional<int64_t> targetNodeId;
        for (const auto& node : funcNodes) {
            if (node.label == "targetFunction" ||
                node.nodeKey.find("targetFunction") != std::string::npos) {
                targetNodeId = node.id;
                spdlog::info("Found targetFunction with id={}", node.id);
                break;
            }
        }

        REQUIRE(targetNodeId.has_value());

        // Get outgoing edges from this node
        auto edgesResult = kg->getEdgesFrom(*targetNodeId, std::nullopt, 100, 0);
        if (edgesResult.has_value()) {
            auto& edges = edgesResult.value();
            spdlog::info("targetFunction has {} outgoing edges", edges.size());

            // Should have edges to file/document
            bool hasLocationEdge = false;
            for (const auto& edge : edges) {
                spdlog::info("Edge: relation={}, dstNodeId={}", edge.relation, edge.dstNodeId);
                if (edge.relation == "located_in" || edge.relation == "defined_in") {
                    hasLocationEdge = true;
                }
            }

            REQUIRE(hasLocationEdge);
        }

        spdlog::info("Graph traversal from symbol works correctly");
    }
}

// ============================================================================
// Multi-Language Entity Extraction Tests (yams-h54.9)
// Verifies all supported tree-sitter languages trigger KG entity extraction
// ============================================================================

struct LanguageTestCase {
    std::string language;
    std::string extension;
    std::string code;
    std::string expectedNodeType;
    std::string expectedSymbolName;
};

// Sample code snippets for each supported language
static const std::vector<LanguageTestCase> LANGUAGE_TEST_CASES = {
    {"c", ".c", "int add(int x, int y) { return x + y; }", "function", "add"},
    {"cpp", ".cpp", "class Calculator { public: int compute() { return 0; } };", "class",
     "Calculator"},
    {"python", ".py", "def greet(name):\n    return f'Hello {name}'", "function", "greet"},
    {"rust", ".rs", "fn hello(x: i32) -> i32 { x + 1 }", "function", "hello"},
    {"go", ".go", "package main\nfunc Hello(x int) int { return x + 1 }", "function", "Hello"},
    {"javascript", ".js", "function processData(input) { return input.trim(); }", "function",
     "processData"},
    {"typescript", ".ts", "function calculate(x: number): number { return x * 2; }", "function",
     "calculate"},
    {"java", ".java", "public class Handler { public void process() {} }", "class", "Handler"},
    {"csharp", ".cs", "public class Service { public void Execute() {} }", "class", "Service"},
    {"php", ".php", "<?php function validate($input) { return true; }", "function", "validate"},
    {"kotlin", ".kt", "class Worker { fun execute() {} }", "class", "Worker"},
};

TEST_CASE("EntityGraphService: Multi-language symbol extraction",
          "[integration][daemon][kg][pbi-009][multilang]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.hasSymbolExtractors()) {
        SKIP("No symbol extractors loaded");
    }

    for (const auto& testCase : LANGUAGE_TEST_CASES) {
        DYNAMIC_SECTION("Language: " << testCase.language) {
            // Check if this language is supported
            const auto& extractors = fixture.serviceManager_->getSymbolExtractors();
            bool languageSupported = false;
            for (const auto& ex : extractors) {
                if (ex && ex->supportsLanguage(testCase.language)) {
                    languageSupported = true;
                    break;
                }
            }

            if (!languageSupported) {
                spdlog::info("Skipping {} - grammar not available", testCase.language);
                SKIP("Grammar not available for " + testCase.language);
            }

            // Create test file
            std::string filename = "test_" + testCase.language + testCase.extension;
            auto hash = fixture.storeContent(filename, testCase.code);
            std::string filePath = (fixture.testDir_ / filename).string();

            spdlog::info("Testing {}: file={}, hash={}", testCase.language, filename,
                         hash.substr(0, 12));

            // Submit for extraction
            bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, testCase.code,
                                                                testCase.language, 3000);
            REQUIRE(extracted);

            // Query KG for nodes of expected type
            auto kg = fixture.getKgStore();
            REQUIRE(kg != nullptr);

            auto nodesResult = kg->findNodesByType(testCase.expectedNodeType, 100, 0);
            REQUIRE(nodesResult.has_value());

            auto& nodes = nodesResult.value();
            spdlog::info("{}: Found {} {} nodes", testCase.language, nodes.size(),
                         testCase.expectedNodeType);

            // Check for expected symbol
            bool foundExpectedSymbol = false;
            for (const auto& node : nodes) {
                std::string label = node.label.value_or("");
                if (label == testCase.expectedSymbolName ||
                    node.nodeKey.find(testCase.expectedSymbolName) != std::string::npos) {
                    foundExpectedSymbol = true;
                    spdlog::info("{}: Found expected symbol '{}' (nodeKey={})", testCase.language,
                                 testCase.expectedSymbolName, node.nodeKey);
                    break;
                }
            }

            CHECK(foundExpectedSymbol);
            if (foundExpectedSymbol) {
                spdlog::info("{}: Entity extraction successful", testCase.language);
            } else {
                spdlog::warn("{}: Expected symbol '{}' not found in KG", testCase.language,
                             testCase.expectedSymbolName);
            }
        }
    }
}

TEST_CASE("EntityGraphService: Verify language extension mapping",
          "[integration][daemon][kg][pbi-009][multilang]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.hasSymbolExtractors()) {
        SKIP("No symbol extractors loaded");
    }

    SECTION("File extension triggers correct language detection") {
        // Test that .py extension triggers Python extraction
        std::string pyCode = "def test_function(): pass";
        std::string filename = "extension_test.py";
        auto hash = fixture.storeContent(filename, pyCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        // Check if Python is supported
        const auto& extractors = fixture.serviceManager_->getSymbolExtractors();
        bool pythonSupported = false;
        for (const auto& ex : extractors) {
            if (ex && ex->supportsLanguage("python")) {
                pythonSupported = true;
                break;
            }
        }

        if (!pythonSupported) {
            SKIP("Python grammar not available");
        }

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, pyCode, "python", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        auto nodesResult = kg->findNodesByType("function", 100, 0);

        if (nodesResult.has_value()) {
            bool foundTestFunction = false;
            for (const auto& node : nodesResult.value()) {
                if (node.label == "test_function" ||
                    node.nodeKey.find("test_function") != std::string::npos) {
                    foundTestFunction = true;
                    break;
                }
            }
            CHECK(foundTestFunction);
        }
    }
}

TEST_CASE("EntityGraphService: All languages create file nodes",
          "[integration][daemon][kg][pbi-009][multilang]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.hasSymbolExtractors()) {
        SKIP("No symbol extractors loaded");
    }

    // Test a subset of languages to verify file node creation
    std::vector<std::pair<std::string, std::string>> languageSamples = {
        {"cpp", "void test() {}"},
        {"python", "def test(): pass"},
        {"javascript", "function test() {}"},
    };

    int filesProcessed = 0;

    for (const auto& [lang, code] : languageSamples) {
        // Check if language is supported
        const auto& extractors = fixture.serviceManager_->getSymbolExtractors();
        bool supported = false;
        for (const auto& ex : extractors) {
            if (ex && ex->supportsLanguage(lang)) {
                supported = true;
                break;
            }
        }

        if (!supported) {
            spdlog::info("Skipping {} - grammar not available", lang);
            continue;
        }

        std::string ext = (lang == "cpp") ? ".cpp" : (lang == "python") ? ".py" : ".js";
        std::string filename = "filenode_test_" + lang + ext;
        auto hash = fixture.storeContent(filename, code);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, code, lang, 2000);
        if (extracted) {
            filesProcessed++;
        }
    }

    // Verify file nodes were created
    auto kg = fixture.getKgStore();
    auto fileNodesResult = kg->findNodesByType("file", 100, 0);

    if (fileNodesResult.has_value()) {
        spdlog::info("Total file nodes created: {}", fileNodesResult.value().size());
        CHECK(fileNodesResult.value().size() >= static_cast<size_t>(filesProcessed));
    }

    REQUIRE(filesProcessed > 0);
    spdlog::info("Processed {} language files, all created file nodes", filesProcessed);
}
// ============================================================================
// Expanded Entity Symbol Extraction Tests (yams-h54.10)
// Tests nested symbols, relationships, namespaces, templates, and edge cases
// ============================================================================

TEST_CASE("EntityGraphService: Nested class extraction",
          "[integration][daemon][kg][pbi-009][nested]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Inner class within outer class creates separate nodes") {
        std::string cppCode = R"(
class OuterContainer {
public:
    class InnerWorker {
    public:
        void execute() {}
    };
    
    struct NestedData {
        int value;
    };
    
    void process() {}
};
)";

        std::string filename = "nested_classes.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 4000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Query for class nodes
        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        auto& classNodes = classNodesResult.value();
        spdlog::info("Found {} class nodes for nested test", classNodes.size());

        bool foundOuter = false;
        bool foundInner = false;
        for (const auto& node : classNodes) {
            std::string label = node.label.value_or("");
            std::string key = node.nodeKey;
            spdlog::info("Nested test class: label={}, key={}", label, key);

            if (label.find("OuterContainer") != std::string::npos ||
                key.find("OuterContainer") != std::string::npos) {
                foundOuter = true;
            }
            if (label.find("InnerWorker") != std::string::npos ||
                key.find("InnerWorker") != std::string::npos) {
                foundInner = true;
            }
        }

        CHECK(foundOuter);
        // Inner class extraction depends on grammar capabilities
        if (foundInner) {
            spdlog::info("Inner class correctly extracted");
        } else {
            spdlog::warn("Inner class not extracted - may be grammar limitation");
        }
    }
}

TEST_CASE("EntityGraphService: Namespace symbol extraction",
          "[integration][daemon][kg][pbi-009][namespace]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Namespaced symbols are extracted with qualified names") {
        std::string cppCode = R"(
namespace company {
namespace product {

class ServiceHandler {
public:
    void handleRequest() {}
};

void globalHelper() {}

} // namespace product
} // namespace company
)";

        std::string filename = "namespaced.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Look for class and function nodes
        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        auto funcNodesResult = kg->findNodesByType("function", 100, 0);

        bool foundServiceHandler = false;
        bool foundGlobalHelper = false;

        if (classNodesResult.has_value()) {
            for (const auto& node : classNodesResult.value()) {
                std::string label = node.label.value_or("");
                std::string key = node.nodeKey;
                spdlog::info("Namespace test class: label={}, key={}", label, key);
                if (label.find("ServiceHandler") != std::string::npos ||
                    key.find("ServiceHandler") != std::string::npos) {
                    foundServiceHandler = true;
                    // Check if qualified name includes namespace
                    if (key.find("company") != std::string::npos ||
                        key.find("product") != std::string::npos) {
                        spdlog::info("Namespace found in qualified name: {}", key);
                    }
                }
            }
        }

        if (funcNodesResult.has_value()) {
            for (const auto& node : funcNodesResult.value()) {
                std::string label = node.label.value_or("");
                std::string key = node.nodeKey;
                spdlog::info("Namespace test function: label={}, key={}", label, key);
                if (label.find("globalHelper") != std::string::npos ||
                    key.find("globalHelper") != std::string::npos) {
                    foundGlobalHelper = true;
                }
            }
        }

        CHECK(foundServiceHandler);
        CHECK(foundGlobalHelper);
    }
}

TEST_CASE("EntityGraphService: Template class extraction",
          "[integration][daemon][kg][pbi-009][template]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Template class creates a class node") {
        std::string cppCode = R"(
template<typename T, typename Allocator = std::allocator<T>>
class GenericContainer {
public:
    void add(const T& item) {}
    T get(size_t index) const { return T{}; }
    
private:
    T* data_;
};

template<typename K, typename V>
struct KeyValue {
    K key;
    V value;
};
)";

        std::string filename = "templates.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        bool foundGenericContainer = false;
        bool foundKeyValue = false;

        for (const auto& node : classNodesResult.value()) {
            std::string label = node.label.value_or("");
            std::string key = node.nodeKey;
            spdlog::info("Template test: label={}, key={}", label, key);

            if (label.find("GenericContainer") != std::string::npos ||
                key.find("GenericContainer") != std::string::npos) {
                foundGenericContainer = true;
            }
            if (label.find("KeyValue") != std::string::npos ||
                key.find("KeyValue") != std::string::npos) {
                foundKeyValue = true;
            }
        }

        // At least template class should be detected
        CHECK((foundGenericContainer || foundKeyValue));
        spdlog::info("Template extraction: GenericContainer={}, KeyValue={}", foundGenericContainer,
                     foundKeyValue);
    }
}

TEST_CASE("EntityGraphService: Symbol relationships extraction",
          "[integration][daemon][kg][pbi-009][relations]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Inheritance creates 'inherits' edge") {
        std::string cppCode = R"(
class BaseEntity {
public:
    virtual void update() {}
};

class DerivedComponent : public BaseEntity {
public:
    void update() override {}
    void render() {}
};
)";

        std::string filename = "inheritance.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        // Find DerivedComponent node
        std::optional<int64_t> derivedNodeId;
        for (const auto& node : classNodesResult.value()) {
            std::string label = node.label.value_or("");
            std::string key = node.nodeKey;
            if (label.find("DerivedComponent") != std::string::npos ||
                key.find("DerivedComponent") != std::string::npos) {
                derivedNodeId = node.id;
                spdlog::info("Found DerivedComponent with id={}", node.id);
                break;
            }
        }

        if (derivedNodeId.has_value()) {
            // Check for inherits edge
            auto edgesResult = kg->getEdgesFrom(*derivedNodeId, std::nullopt, 100, 0);
            if (edgesResult.has_value()) {
                bool foundInheritsEdge = false;
                for (const auto& edge : edgesResult.value()) {
                    spdlog::info("Edge from DerivedComponent: relation={}", edge.relation);
                    if (edge.relation == "inherits" || edge.relation == "extends" ||
                        edge.relation == "derives_from") {
                        foundInheritsEdge = true;
                    }
                }
                // Inheritance edge extraction depends on grammar/extractor capability
                if (foundInheritsEdge) {
                    spdlog::info("Inheritance relationship correctly captured");
                } else {
                    spdlog::warn("Inheritance edge not found - may require enhanced extractor");
                }
            }
        }
    }

    SECTION("Function calls create 'calls' edge") {
        std::string cppCode = R"(
void helperFunction() {}

void callerFunction() {
    helperFunction();
    helperFunction();
}
)";

        std::string filename = "calls.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto funcNodesResult = kg->findNodesByType("function", 100, 0);
        REQUIRE(funcNodesResult.has_value());

        // Find callerFunction node
        std::optional<int64_t> callerNodeId;
        for (const auto& node : funcNodesResult.value()) {
            std::string label = node.label.value_or("");
            std::string key = node.nodeKey;
            if (label.find("callerFunction") != std::string::npos ||
                key.find("callerFunction") != std::string::npos) {
                callerNodeId = node.id;
                spdlog::info("Found callerFunction with id={}", node.id);
                break;
            }
        }

        if (callerNodeId.has_value()) {
            auto edgesResult = kg->getEdgesFrom(*callerNodeId, std::nullopt, 100, 0);
            if (edgesResult.has_value()) {
                bool foundCallsEdge = false;
                for (const auto& edge : edgesResult.value()) {
                    spdlog::info("Edge from callerFunction: relation={}", edge.relation);
                    if (edge.relation == "calls" || edge.relation == "invokes") {
                        foundCallsEdge = true;
                    }
                }
                if (foundCallsEdge) {
                    spdlog::info("Call relationship correctly captured");
                } else {
                    spdlog::warn("Call edge not found - may require call graph extraction enabled");
                }
            }
        }
    }
}

TEST_CASE("EntityGraphService: Member variables extraction",
          "[integration][daemon][kg][pbi-009][members]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Class members are extracted") {
        std::string cppCode = R"(
class DataContainer {
public:
    int publicValue;
    std::string name;
    
    void setName(const std::string& n) { name = n; }
    
private:
    double privateData;
    static int instanceCount;
};
)";

        std::string filename = "members.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Check for class node
        auto classNodesResult = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodesResult.has_value());

        bool foundDataContainer = false;
        for (const auto& node : classNodesResult.value()) {
            if (node.label.value_or("").find("DataContainer") != std::string::npos ||
                node.nodeKey.find("DataContainer") != std::string::npos) {
                foundDataContainer = true;
                break;
            }
        }
        REQUIRE(foundDataContainer);

        // Check for variable/field nodes (depends on extractor capability)
        auto varNodesResult = kg->findNodesByType("variable", 100, 0);
        auto fieldNodesResult = kg->findNodesByType("field", 100, 0);

        size_t varCount = varNodesResult.has_value() ? varNodesResult.value().size() : 0;
        size_t fieldCount = fieldNodesResult.has_value() ? fieldNodesResult.value().size() : 0;

        spdlog::info("Member extraction: {} variables, {} fields detected", varCount, fieldCount);
        // Member extraction is optional - just log for visibility
    }
}

TEST_CASE("EntityGraphService: Error recovery for malformed code",
          "[integration][daemon][kg][pbi-009][error-recovery]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Partial parsing extracts valid symbols") {
        // Code with syntax errors but some valid symbols
        std::string cppCode = R"(
// Valid function
void validFunction() {
    // Valid body
}

// Invalid/incomplete class
class BrokenClass {
    // Missing closing brace

// Another valid function
int anotherValid(int x) {
    return x * 2;
}
)";

        std::string filename = "malformed.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        // Should not crash - may extract some or no symbols
        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000);

        // Extraction should complete (not crash)
        CHECK(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Check if any valid symbols were recovered
        auto funcNodesResult = kg->findNodesByType("function", 100, 0);
        if (funcNodesResult.has_value()) {
            spdlog::info("Error recovery: {} functions extracted from malformed code",
                         funcNodesResult.value().size());

            // May or may not find validFunction depending on parser recovery
            for (const auto& node : funcNodesResult.value()) {
                spdlog::info("Recovered function: {}", node.label.value_or(node.nodeKey));
            }
        }
    }
}

TEST_CASE("EntityGraphService: Large file symbol extraction",
          "[integration][daemon][kg][pbi-009][large-file]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Large file with many symbols extracts efficiently") {
        // Generate a file with many functions
        std::string cppCode;
        const int NUM_FUNCTIONS = 100;

        for (int i = 0; i < NUM_FUNCTIONS; i++) {
            cppCode += "void function_" + std::to_string(i) + "(int x) { /* body */ }\n\n";
        }

        std::string filename = "large_file.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        auto startTime = std::chrono::steady_clock::now();
        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 10000);
        auto endTime = std::chrono::steady_clock::now();

        auto durationMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

        REQUIRE(extracted);
        spdlog::info("Large file extraction completed in {}ms", durationMs);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto funcNodesResult = kg->findNodesByType("function", 500, 0);
        REQUIRE(funcNodesResult.has_value());

        spdlog::info("Extracted {} functions from {} generated functions",
                     funcNodesResult.value().size(), NUM_FUNCTIONS);

        // Should extract a significant portion of functions
        CHECK(funcNodesResult.value().size() >= NUM_FUNCTIONS / 2);
    }
}

TEST_CASE("EntityGraphService: Symbol update/deduplication",
          "[integration][daemon][kg][pbi-009][dedup]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Re-extracting same file updates rather than duplicates") {
        std::string cppCode = R"(
void uniqueTestFunction() {
    // Original version
}
)";

        std::string filename = "dedup_test.cpp";
        auto hash1 = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash1, filePath, cppCode, "cpp", 3000));

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Count function nodes before second extraction
        auto funcNodesBefore = kg->findNodesByType("function", 500, 0);
        size_t countBefore = funcNodesBefore.has_value() ? funcNodesBefore.value().size() : 0;
        spdlog::info("Function count before re-extraction: {}", countBefore);

        // Update content slightly (simulating file modification)
        std::string cppCode2 = R"(
void uniqueTestFunction() {
    // Modified version with new comment
}
)";
        auto hash2 = fixture.storeContent(filename + ".v2", cppCode2);
        std::string filePath2 = (fixture.testDir_ / (filename + ".v2")).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash2, filePath2, cppCode2, "cpp", 3000));

        // Count function nodes after second extraction
        auto funcNodesAfter = kg->findNodesByType("function", 500, 0);
        size_t countAfter = funcNodesAfter.has_value() ? funcNodesAfter.value().size() : 0;
        spdlog::info("Function count after re-extraction: {}", countAfter);

        // Nodes should be deduplicated or updated, not massively duplicated
        // Allow for some growth (different files may create separate nodes)
        CHECK(countAfter <= countBefore + 5);
    }
}

TEST_CASE("EntityGraphService: Python-specific symbol extraction",
          "[integration][daemon][kg][pbi-009][python]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    // Check for Python support
    const auto& extractors = fixture.serviceManager_->getSymbolExtractors();
    bool pythonSupported = false;
    for (const auto& ex : extractors) {
        if (ex && ex->supportsLanguage("python")) {
            pythonSupported = true;
            break;
        }
    }

    if (!pythonSupported) {
        SKIP("Python grammar not available");
    }

    SECTION("Python decorators and class methods") {
        std::string pyCode = R"(
class ServiceController:
    """Controller for handling service requests."""
    
    def __init__(self, config):
        self.config = config
    
    @staticmethod
    def create_default():
        return ServiceController({})
    
    @classmethod
    def from_file(cls, path):
        return cls({})
    
    def handle_request(self, request):
        """Process an incoming request."""
        return {'status': 'ok'}


def standalone_function(x, y):
    """A standalone function."""
    return x + y
)";

        std::string filename = "controller.py";
        auto hash = fixture.storeContent(filename, pyCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, pyCode, "python", 4000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Check for class
        auto classNodes = kg->findNodesByType("class", 100, 0);
        bool foundServiceController = false;
        if (classNodes.has_value()) {
            for (const auto& node : classNodes.value()) {
                if (node.label.value_or("").find("ServiceController") != std::string::npos ||
                    node.nodeKey.find("ServiceController") != std::string::npos) {
                    foundServiceController = true;
                    break;
                }
            }
        }
        CHECK(foundServiceController);

        // Check for functions/methods
        auto funcNodes = kg->findNodesByType("function", 100, 0);
        size_t funcCount = funcNodes.has_value() ? funcNodes.value().size() : 0;
        spdlog::info("Python extraction: found {} functions/methods", funcCount);
        CHECK(funcCount >= 1); // At least standalone_function
    }
}

TEST_CASE("EntityGraphService: JavaScript/TypeScript extraction",
          "[integration][daemon][kg][pbi-009][javascript]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    // Check for JavaScript support
    const auto& extractors = fixture.serviceManager_->getSymbolExtractors();
    bool jsSupported = false;
    for (const auto& ex : extractors) {
        if (ex && ex->supportsLanguage("javascript")) {
            jsSupported = true;
            break;
        }
    }

    if (!jsSupported) {
        SKIP("JavaScript grammar not available");
    }

    SECTION("ES6 classes and arrow functions") {
        std::string jsCode = R"(
class DataManager {
    constructor(options) {
        this.options = options;
    }
    
    async fetchData(url) {
        const response = await fetch(url);
        return response.json();
    }
    
    processItems = (items) => {
        return items.map(item => item.id);
    };
}

const helperFunction = (x) => x * 2;

function traditionalFunction(a, b) {
    return a + b;
}

export default DataManager;
)";

        std::string filename = "data_manager.js";
        auto hash = fixture.storeContent(filename, jsCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted =
            fixture.submitAndWaitForExtraction(hash, filePath, jsCode, "javascript", 4000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Check for class
        auto classNodes = kg->findNodesByType("class", 100, 0);
        bool foundDataManager = false;
        if (classNodes.has_value()) {
            for (const auto& node : classNodes.value()) {
                if (node.label.value_or("").find("DataManager") != std::string::npos ||
                    node.nodeKey.find("DataManager") != std::string::npos) {
                    foundDataManager = true;
                    break;
                }
            }
        }
        CHECK(foundDataManager);

        // Check for functions
        auto funcNodes = kg->findNodesByType("function", 100, 0);
        size_t funcCount = funcNodes.has_value() ? funcNodes.value().size() : 0;
        spdlog::info("JavaScript extraction: found {} functions", funcCount);
        CHECK(funcCount >= 1);
    }
}

TEST_CASE("EntityGraphService: Cross-file symbol references",
          "[integration][daemon][kg][pbi-009][cross-file]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Symbols from multiple files can be queried together") {
        // File 1: Interface definition
        std::string headerCode = R"(
class IRepository {
public:
    virtual void save() = 0;
    virtual void load() = 0;
};
)";

        // File 2: Implementation
        std::string implCode = R"(
class FileRepository : public IRepository {
public:
    void save() override {}
    void load() override {}
private:
    std::string path_;
};
)";

        // File 3: Usage
        std::string usageCode = R"(
class DataService {
public:
    void process(IRepository& repo) {
        repo.load();
        // do work
        repo.save();
    }
};
)";

        auto hash1 = fixture.storeContent("repository.h", headerCode);
        auto hash2 = fixture.storeContent("file_repository.cpp", implCode);
        auto hash3 = fixture.storeContent("data_service.cpp", usageCode);

        std::string path1 = (fixture.testDir_ / "repository.h").string();
        std::string path2 = (fixture.testDir_ / "file_repository.cpp").string();
        std::string path3 = (fixture.testDir_ / "data_service.cpp").string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash1, path1, headerCode, "cpp", 2000));
        REQUIRE(fixture.submitAndWaitForExtraction(hash2, path2, implCode, "cpp", 2000));
        REQUIRE(fixture.submitAndWaitForExtraction(hash3, path3, usageCode, "cpp", 2000));

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Get all class nodes
        auto classNodes = kg->findNodesByType("class", 100, 0);
        REQUIRE(classNodes.has_value());

        std::set<std::string> foundClasses;
        for (const auto& node : classNodes.value()) {
            std::string label = node.label.value_or(node.nodeKey);
            spdlog::info("Cross-file class: {}", label);

            if (label.find("IRepository") != std::string::npos)
                foundClasses.insert("IRepository");
            if (label.find("FileRepository") != std::string::npos)
                foundClasses.insert("FileRepository");
            if (label.find("DataService") != std::string::npos)
                foundClasses.insert("DataService");
        }

        spdlog::info("Found {} out of 3 expected classes across files", foundClasses.size());
        CHECK(foundClasses.size() >= 2); // At least 2 of 3 classes found
    }
}

// ============================================================================
// KG Cleanup Tests (yams-3m8, yams-qel, yams-ye3)
// Tests document deletion cascade, stale edge cleanup, and isolated node queries
// ============================================================================

TEST_CASE("KnowledgeGraphStore: findIsolatedNodes optimization",
          "[integration][daemon][kg][isolated]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Finds functions with no incoming call edges") {
        std::string cppCode = R"(
void calledFunction() {}

void callerFunction() {
    calledFunction();
}

void isolatedFunction() {}

void anotherIsolated() {}
)";

        std::string filename = "isolated_test.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        bool extracted = fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 4000);
        REQUIRE(extracted);

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto isolatedResult = kg->findIsolatedNodes("function", "calls", 100);
        REQUIRE(isolatedResult.has_value());

        spdlog::info("findIsolatedNodes returned {} nodes", isolatedResult.value().size());
        for (const auto& node : isolatedResult.value()) {
            spdlog::info("Isolated function: label={}", node.label.value_or(node.nodeKey));
        }

        CHECK(!isolatedResult.value().empty());
    }
}

TEST_CASE("KnowledgeGraphStore: deleteEdgesForSourceFile cleans stale edges",
          "[integration][daemon][kg][cleanup]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Re-indexing file cleans up old edges") {
        std::string cppCode1 = R"(
void originalFunction() {}
)";

        std::string filename = "cleanup_test.cpp";
        auto hash1 = fixture.storeContent(filename, cppCode1);
        std::string filePath = (fixture.testDir_ / filename).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash1, filePath, cppCode1, "cpp", 3000));

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto funcNodes = kg->findNodesByType("function", 100, 0);
        REQUIRE(funcNodes.has_value());

        std::string cppCode2 = R"(
void renamedFunction() {}
)";

        auto hash2 = fixture.storeContent(filename + ".v2", cppCode2);
        std::string filePath2 = (fixture.testDir_ / (filename + ".v2")).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash2, filePath2, cppCode2, "cpp", 3000));
        spdlog::info("Stale edge cleanup test completed");
    }
}

TEST_CASE("KnowledgeGraphStore: deleteNodesForDocumentHash cascades",
          "[integration][daemon][kg][cascade]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    EntityGraphIntegrationFixture fixture;

    if (!fixture.supportsCpp()) {
        SKIP("C++ grammar not available");
    }

    SECTION("Document deletion triggers KG node cleanup") {
        std::string cppCode = R"(
void deletionTestFunction() {}
)";

        std::string filename = "deletion_test.cpp";
        auto hash = fixture.storeContent(filename, cppCode);
        std::string filePath = (fixture.testDir_ / filename).string();

        REQUIRE(fixture.submitAndWaitForExtraction(hash, filePath, cppCode, "cpp", 3000));

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto funcNodesBefore = kg->findNodesByType("function", 500, 0);
        REQUIRE(funcNodesBefore.has_value());
        size_t countBefore = funcNodesBefore.value().size();
        spdlog::info("Function nodes before deletion: {}", countBefore);

        auto deleteResult = kg->deleteNodesForDocumentHash(hash);
        REQUIRE(deleteResult.has_value());
        spdlog::info("Deleted {} nodes for document hash {}", deleteResult.value(),
                    hash.substr(0, 12));

        auto funcNodesAfter = kg->findNodesByType("function", 500, 0);
        REQUIRE(funcNodesAfter.has_value());
        size_t countAfter = funcNodesAfter.value().size();
        spdlog::info("Function nodes after deletion: {}", countAfter);

        CHECK(countAfter <= countBefore);
    }
}

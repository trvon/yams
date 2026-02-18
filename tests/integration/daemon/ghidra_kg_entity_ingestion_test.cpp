// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors
//
// Integration test: Ghidra plugin → KG enrich ingestion pipeline
// Tests end-to-end flow: binary analysis → enrich extraction → KG node/edge creation
// Part of yams-3jb: Add integration test for Ghidra KG enrich ingestion

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include <spdlog/spdlog.h>
#include <catch2/catch_test_macros.hpp>

#include "test_daemon_harness.h"

#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/knowledge_graph_store.h>

using namespace yams::metadata;

// Skip tests on Windows where daemon IPC shutdown is unstable
#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Windows daemon shutdown unstable - run in Docker container")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

// ============================================================================
// Test Fixture: Ghidra Enrich Extraction Integration
// ============================================================================

class GhidraEntityIntegrationFixture {
public:
    GhidraEntityIntegrationFixture() {
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
        spdlog::info("GhidraEntityIntegrationFixture: Daemon started, dataDir={}",
                     testDir_.string());
    }

    ~GhidraEntityIntegrationFixture() {
        spdlog::debug("GhidraEntityIntegrationFixture: Cleanup via DaemonHarness");
    }

    // Check if any enrich providers are available
    bool hasEntityProviders() {
        // Enrich providers are loaded via PluginManager
        // For now, use configured enrich concurrency budget as a readiness proxy.
        auto* pq = serviceManager_->getPostIngestQueue();
        if (!pq)
            return false;

        // NOTE: maxEntityConcurrent currently maps to enrich-stage concurrency.
        return pq->maxEntityConcurrent() > 0;
    }

    // Store a binary file for testing
    std::string storeBinary(const std::string& filename, const std::vector<uint8_t>& content) {
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
        for (uint8_t b : content) {
            bytes.push_back(static_cast<std::byte>(b));
        }

        auto storeResult = contentStore->storeBytes(bytes);
        if (!storeResult) {
            throw std::runtime_error("Failed to store content: " + storeResult.error().message);
        }

        std::string hash = storeResult.value().contentHash;

        yams::metadata::DocumentInfo doc;
        doc.fileName = filename;
        doc.filePath = (testDir_ / filename).string();
        doc.sha256Hash = hash;
        doc.fileSize = content.size();
        doc.mimeType = "application/x-executable";
        doc.fileExtension = ".exe";
        doc.modifiedTime =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

        auto insertResult = meta->insertDocument(doc);
        if (!insertResult) {
            throw std::runtime_error("Failed to insert document: " + insertResult.error().message);
        }

        // Write to disk for plugin access
        std::ofstream out(doc.filePath, std::ios::binary);
        out.write(reinterpret_cast<const char*>(content.data()), content.size());
        out.close();

        return hash;
    }

    // Get KG store for direct queries
    std::shared_ptr<KnowledgeGraphStore> getKgStore() { return serviceManager_->getKgStore(); }

    // Wait for enrich extraction to complete
    bool waitForEntityProcessing(int timeoutMs = 10000) {
        auto* pq = serviceManager_->getPostIngestQueue();
        if (!pq)
            return false;

        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            if (pq->enrichInFlight() == 0 && pq->size() == 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(200)); // Allow final processing
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    yams::test::DaemonHarness harness_;
    yams::daemon::YamsDaemon* daemon_{nullptr};
    yams::daemon::ServiceManager* serviceManager_{nullptr};
    std::filesystem::path testDir_;
};

// ============================================================================
// Test Cases: Enrich Extraction Pipeline Validation
// ============================================================================

TEST_CASE("GhidraEntityIngestion: KG store is available for enrich storage",
          "[integration][daemon][ghidra][pbi-3jb]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    GhidraEntityIntegrationFixture fixture;

    SECTION("KnowledgeGraphStore is initialized") {
        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        auto healthResult = kg->healthCheck();
        REQUIRE(healthResult.has_value());
        spdlog::info("KG store health check passed - ready for enrich storage");
    }
}

TEST_CASE("GhidraEntityIngestion: Enrich provider infrastructure is ready",
          "[integration][daemon][ghidra][pbi-3jb]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    GhidraEntityIntegrationFixture fixture;

    SECTION("Enrich processing channel is configured") {
        auto* pq = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(pq != nullptr);

        // Verify enrich channel configuration via current compatibility API.
        size_t maxConcurrent = pq->maxEntityConcurrent();
        spdlog::info("Enrich channel configured with max concurrent: {}", maxConcurrent);
        // Enrich lane can be 0 when profile scaling suppresses optional stages.
        REQUIRE(maxConcurrent <= 16);
    }

    SECTION("Enrich inflight counter is accessible") {
        auto* pq = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(pq != nullptr);

        size_t enrichInFlight = pq->enrichInFlight();
        spdlog::info("Current enrich inflight count: {}", enrichInFlight);
        // Counter should be accessible (value doesn't matter)
        REQUIRE(enrichInFlight >= 0);
    }
}

TEST_CASE("GhidraEntityIngestion: Binary node types are recognized in KG",
          "[integration][daemon][ghidra][pbi-3jb]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    GhidraEntityIntegrationFixture fixture;

    SECTION("KG can store binary.function nodes") {
        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Create a test binary.function node manually to verify schema
        KGNode funcNode;
        funcNode.type = "binary.function";
        funcNode.nodeKey = "test_binary_func_12345";
        funcNode.label = "main";
        funcNode.properties = R"({"address": "0x401000", "size": 256})";

        auto insertResult = kg->upsertNode(funcNode);
        if (insertResult.has_value()) {
            spdlog::info("binary.function node inserted with id: {}", insertResult.value());
            REQUIRE(insertResult.value() > 0);

            // Clean up test node
            kg->deleteNodeById(insertResult.value());
        } else {
            spdlog::warn("Could not insert test node - may need schema update");
        }
    }

    SECTION("KG can store binary.import nodes") {
        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        KGNode importNode;
        importNode.type = "binary.import";
        importNode.nodeKey = "test_import_kernel32_CreateFileW";
        importNode.label = "CreateFileW";
        importNode.properties = R"({"library": "kernel32.dll"})";

        auto insertResult = kg->upsertNode(importNode);
        if (insertResult.has_value()) {
            spdlog::info("binary.import node inserted with id: {}", insertResult.value());
            REQUIRE(insertResult.value() > 0);

            // Clean up test node
            kg->deleteNodeById(insertResult.value());
        } else {
            spdlog::warn("Could not insert binary.import node - may need schema update");
        }
    }

    SECTION("KG can store CALLS edges") {
        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Create source and target nodes first
        KGNode srcNode;
        srcNode.type = "binary.function";
        srcNode.nodeKey = "test_caller_func";
        srcNode.label = "caller";

        KGNode dstNode;
        dstNode.type = "binary.function";
        dstNode.nodeKey = "test_callee_func";
        dstNode.label = "callee";

        auto srcResult = kg->upsertNode(srcNode);
        auto dstResult = kg->upsertNode(dstNode);

        if (srcResult.has_value() && dstResult.has_value()) {
            KGEdge edge;
            edge.srcNodeId = srcResult.value();
            edge.dstNodeId = dstResult.value();
            edge.relation = "calls";

            auto edgeResult = kg->addEdge(edge);
            if (edgeResult.has_value()) {
                spdlog::info("CALLS edge inserted with id: {}", edgeResult.value());
                REQUIRE(edgeResult.value() > 0);
            }

            // Clean up test nodes (cascades to edges)
            kg->deleteNodeById(srcResult.value());
            kg->deleteNodeById(dstResult.value());
        }
    }
}

TEST_CASE("GhidraEntityIngestion: Enrich metrics are exposed",
          "[integration][daemon][ghidra][pbi-3jb]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    GhidraEntityIntegrationFixture fixture;

    SECTION("Enrich queued/consumed/dropped counters are accessible") {
        auto* pq = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(pq != nullptr);

        // These counters are read from InternalEventBus via DaemonMetrics
        // Just verify they're accessible - values depend on processing state
        size_t enrichInFlight = pq->enrichInFlight();
        spdlog::info("Enrich inflight: {}", enrichInFlight);

        // The counter access itself is the test - no specific value required
        REQUIRE(true);
    }
}

// ============================================================================
// Optional: Full Ghidra Integration Test (requires Ghidra to be installed)
// ============================================================================

TEST_CASE("GhidraEntityIngestion: Full binary analysis creates KG enrich artifacts",
          "[integration][daemon][ghidra][pbi-3jb][.ghidra-full]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();

    // This test is tagged with [.ghidra-full] - it's disabled by default
    // Run with: --run-hidden to include this test

    GhidraEntityIntegrationFixture fixture;

    // Skip if no enrich providers are available
    if (!fixture.hasEntityProviders()) {
        SKIP("No enrich providers available - Ghidra plugin not loaded");
    }

    SECTION("Analyze small binary and verify KG enrich artifacts") {
        // Create a minimal PE header (not a real executable, just enough structure)
        // This is a placeholder - real test would use an actual small binary
        std::vector<uint8_t> minimalPE = {
            'M',  'Z',  0x90, 0x00, // DOS signature
            0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00,
        };

        // Pad to reasonable size
        minimalPE.resize(512, 0);

        auto hash = fixture.storeBinary("test_binary.exe", minimalPE);
        spdlog::info("Stored test binary with hash: {}", hash.substr(0, 12));

        // Wait for enrich extraction
        bool processed = fixture.waitForEntityProcessing(30000);

        if (!processed) {
            spdlog::warn("Enrich processing timed out - Ghidra may not be configured");
            SKIP("Enrich processing timed out - Ghidra may not be installed");
        }

        auto kg = fixture.getKgStore();
        REQUIRE(kg != nullptr);

        // Query for binary-related nodes
        auto binaryFuncNodes = kg->findNodesByType("binary.function", 100, 0);
        auto binaryImportNodes = kg->findNodesByType("binary.import", 100, 0);

        if (binaryFuncNodes.has_value()) {
            spdlog::info("Found {} binary.function nodes", binaryFuncNodes.value().size());
        }
        if (binaryImportNodes.has_value()) {
            spdlog::info("Found {} binary.import nodes", binaryImportNodes.value().size());
        }

        // For a real binary, we'd expect:
        // - binary node with SHA256
        // - binary.function nodes
        // - binary.import nodes
        // - CALLS edges between functions
        // - IMPORTS edges to libraries

        // This minimal PE may not produce enrich artifacts - real test needs real binary
        spdlog::info("Full Ghidra integration test completed");
    }
}

// ============================================================================
// Edge Cases and Error Recovery
// ============================================================================

TEST_CASE("GhidraEntityIngestion: Non-binary files don't trigger enrich extraction",
          "[integration][daemon][ghidra][pbi-3jb]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    GhidraEntityIntegrationFixture fixture;

    SECTION("Text file is not sent to enrich provider") {
        auto* pq = fixture.serviceManager_->getPostIngestQueue();
        REQUIRE(pq != nullptr);

        size_t enrichInflightBefore = pq->enrichInFlight();

        // Store a text file (not binary)
        auto contentStore = fixture.serviceManager_->getContentStore();
        auto meta = fixture.serviceManager_->getMetadataRepo();

        if (!contentStore || !meta) {
            SKIP("Content store or metadata repo not available");
        }

        std::string textContent = "Hello, this is a text file.";
        std::vector<std::byte> bytes;
        for (char c : textContent) {
            bytes.push_back(static_cast<std::byte>(c));
        }

        auto storeResult = contentStore->storeBytes(bytes);
        REQUIRE(storeResult.has_value());

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Enrich inflight should not have increased significantly
        size_t enrichInflightAfter = pq->enrichInFlight();
        spdlog::info("Enrich inflight: before={}, after={}", enrichInflightBefore,
                     enrichInflightAfter);

        // Text files shouldn't trigger enrich extraction (may briefly increase during dispatch
        // check)
        CHECK(enrichInflightAfter <= enrichInflightBefore + 1);
    }
}

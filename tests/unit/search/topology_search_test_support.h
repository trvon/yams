// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Shared fixtures for SearchEngine topology tests: a two-cluster corpus with a metadata
// repository, knowledge graph store and in-memory vector database, plus the configuration and
// search helpers that drive the topology-routed vector stage. Header-only so the topology test
// and the vector-stage characterization test exercise the same corpus and configuration.

#pragma once

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <boost/asio/any_io_executor.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_execution_context.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

namespace yams::search::topology_test_support {

inline std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

class FixedEmbeddingBackend final : public vector::IEmbeddingBackend {
public:
    explicit FixedEmbeddingBackend(std::vector<float> embedding)
        : embedding_(std::move(embedding)) {}

    bool initialize() override {
        initialized_ = true;
        return true;
    }
    void shutdown() override { initialized_ = false; }
    bool isInitialized() const override { return initialized_; }

    Result<std::vector<float>> generateEmbedding(const std::string&) override { return embedding_; }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        return std::vector<std::vector<float>>(texts.size(), embedding_);
    }

    size_t getEmbeddingDimension() const override { return embedding_.size(); }
    size_t getMaxSequenceLength() const override { return 512; }
    std::string getBackendName() const override { return "fixed-test"; }
    std::string getEmbeddingSpaceIdentity() const override { return "test-space-v1"; }
    bool isAvailable() const override { return true; }
    vector::GenerationStats getStats() const override { return {}; }
    void resetStats() override {}

private:
    std::vector<float> embedding_;
    bool initialized_{false};
};

// CI runs the unit lane with vectors disabled and in-memory overlays; the routed fixtures need a
// real vector leg, so pin every vector switch for the duration of the case.
struct TopologyVectorEnvPin {
    yams::test::ScopedEnvVar disable{"YAMS_DISABLE_VECTORS", std::optional<std::string>("0")};
    yams::test::ScopedEnvVar disableSingular{"YAMS_DISABLE_VECTOR", std::nullopt};
    yams::test::ScopedEnvVar disableDb{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar skipVecInit{"YAMS_SQLITE_VEC_SKIP_INIT",
                                         std::optional<std::string>("0")};
    yams::test::ScopedEnvVar inMemory{"YAMS_VDB_IN_MEMORY", std::nullopt};
};

struct TopologySearchFixture {
    explicit TopologySearchFixture(
        vector::VectorSearchEngine searchEngine = vector::VectorSearchEngine::SimeonPqAdc)
        : disableVectors("YAMS_DISABLE_VECTORS", std::optional<std::string>("0")),
          skipVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::optional<std::string>("0")) {
        dbPath = tempDbPath("search_topology_");
        metadata::ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<metadata::ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<metadata::MetadataRepository>(*pool);
        auto kgResult =
            metadata::makeSqliteKnowledgeGraphStore(*pool, metadata::KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<metadata::KnowledgeGraphStore>(kgResult.value().release());

        vector::VectorDatabaseConfig vectorConfig;
        vectorConfig.database_path = ":memory:";
        vectorConfig.embedding_dim = 2;
        vectorConfig.create_if_missing = true;
        vectorConfig.use_in_memory = true;
        vectorConfig.search_engine = searchEngine;
        vectorConfig.vec0_phss_enabled = searchEngine == vector::VectorSearchEngine::Vec0L2;
        vectorConfig.vec0_phss_candidates = 4;
        vectorConfig.simeon_pq_centroids = 4;
        vectorDb = std::make_shared<vector::VectorDatabase>(vectorConfig);
        REQUIRE(vectorDb->initialize());
    }

    ~TopologySearchFixture() {
        vectorDb.reset();
        kgStore.reset();
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    TopologySearchFixture(const TopologySearchFixture&) = delete;
    TopologySearchFixture& operator=(const TopologySearchFixture&) = delete;

    void addDocument(const std::string& hash, const std::string& content,
                     const std::vector<float>& embedding) {
        metadata::DocumentInfo info;
        info.filePath = "/tmp/" + hash + ".txt";
        info.fileName = hash + ".txt";
        info.fileExtension = ".txt";
        info.fileSize = static_cast<int64_t>(content.size());
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        auto docId = repo->insertDocument(info);
        REQUIRE(docId.has_value());
        REQUIRE(repo->indexDocumentContent(docId.value(), hash, content, "text/plain").has_value());

        metadata::DocumentContent docContent;
        docContent.documentId = docId.value();
        docContent.contentText = content;
        docContent.contentLength = static_cast<int64_t>(content.size());
        docContent.extractionMethod = "test";
        docContent.language = "en";
        REQUIRE(repo->insertContent(docContent).has_value());

        vector::VectorRecord record;
        record.chunk_id = "doc-" + hash;
        record.document_hash = hash;
        record.embedding = embedding;
        record.level = vector::EmbeddingLevel::DOCUMENT;
        REQUIRE(vectorDb->insertVector(record));
    }

    yams::test::ScopedEnvVar disableVectors;
    yams::test::ScopedEnvVar skipVecInit;
    std::filesystem::path dbPath;
    std::unique_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<metadata::MetadataRepository> repo;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
};

// Two reciprocal clusters: {x1, x2} near (1, 0) and {y1, y2} near (0, 1).
inline yams::topology::TopologyArtifactBatch buildTwoClusterTopologyBatch() {
    yams::topology::ConnectedComponentTopologyEngine topologyEngine;
    yams::topology::TopologyBuildConfig topologyConfig;
    topologyConfig.reciprocalOnly = true;
    topologyConfig.minEdgeScore = 0.5;
    topologyConfig.embeddingSpaceIdentity = "test-space-v1";
    std::vector<yams::topology::TopologyDocumentInput> topologyDocs{
        {.documentHash = "x1",
         .filePath = "/tmp/x1.txt",
         .embedding = {1.0F, 0.0F},
         .neighbors = {{.documentHash = "x2", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "x2",
         .filePath = "/tmp/x2.txt",
         .embedding = {0.9F, 0.1F},
         .neighbors = {{.documentHash = "x1", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "y1",
         .filePath = "/tmp/y1.txt",
         .embedding = {0.0F, 1.0F},
         .neighbors = {{.documentHash = "y2", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "y2",
         .filePath = "/tmp/y2.txt",
         .embedding = {0.1F, 0.9F},
         .neighbors = {{.documentHash = "y1", .score = 0.9F, .reciprocal = true}}},
    };
    auto topologyBatch = topologyEngine.buildArtifacts(topologyDocs, topologyConfig);
    REQUIRE(topologyBatch.has_value());
    return topologyBatch.value();
}

inline void seedTwoClusterTopology(TopologySearchFixture& fix) {
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(buildTwoClusterTopologyBatch()).has_value());
}

inline void seedTopologyDocuments(TopologySearchFixture& fix) {
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});
    fix.addDocument("y1", "omega one", {0.0F, 1.0F});
    fix.addDocument("y2", "omega two", {0.1F, 0.9F});
}

inline std::shared_ptr<vector::EmbeddingGenerator>
makeFixedGenerator(std::vector<float> embedding) {
    vector::EmbeddingConfig embeddingConfig;
    embeddingConfig.embedding_dim = embedding.size();
    auto generator = std::make_shared<vector::EmbeddingGenerator>(
        std::make_unique<FixedEmbeddingBackend>(std::move(embedding)), embeddingConfig);
    REQUIRE(generator->initialize());
    return generator;
}

inline SearchEngineConfig topologyRoutingTestConfig(bool enabled) {
    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = true;
    config.textWeight = 1.0F;
    config.simeonTextWeight = 0.0F;
    config.pathTreeWeight = 0.0F;
    config.kgWeight = 0.0F;
    config.vectorWeight = 1.0F;
    config.entityVectorWeight = 0.0F;
    config.tagWeight = 0.0F;
    config.metadataWeight = 0.0F;
    config.vectorOnlyThreshold = 0.0F;
    config.vectorOnlyPenalty = 1.0F;
    config.vectorMaxResults = 4;
    config.topologyRoutingMode = enabled ? SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly
                                         : SearchEngineConfig::TopologyRoutingMode::Disabled;
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 2;
    return config;
}

// Runs one search with every index marked ready. An executor, when given, backs the component
// fanout so the vector stage runs off the calling thread.
inline Result<SearchResponse> runTopologySearch(
    TopologySearchFixture& fix, const std::shared_ptr<vector::EmbeddingGenerator>& generator,
    const SearchEngineConfig& config, std::size_t limit, std::string query = "unmatched query",
    std::optional<boost::asio::any_io_executor> executor = std::nullopt) {
    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    if (executor.has_value()) {
        engine.setExecutor(std::move(executor));
    }
    SearchParams params;
    params.limit = limit;
    return engine.searchWithResponse(query, params);
}

struct CertifiedTopologyRouteOptions {
    std::size_t maxRowsVisited = 2;
    std::size_t maxExactDistanceEvaluations = 2;
    std::size_t maxAnnCandidates = 2;
    // SearchEngine clears topologyMaxDocs before routing when the backend supports allowed-set
    // ANN under Narrow (Simeon PQ). The policy fingerprint must match that resolved value.
    bool deferDocumentLimitToVectorRanker = true;
};

// Fills the calibration and fingerprints that let a Narrow route pass every theorem certificate
// for `batch`, so the only remaining admission input is the observed vector work.
inline void configureCertifiedTopologyRoute(SearchEngineConfig& config,
                                            const yams::topology::TopologyArtifactBatch& batch,
                                            const CertifiedTopologyRouteOptions& options) {
    config.topologyRouteRiskCalibration.constructionFingerprint =
        topologyRoutingConstructionFingerprint(batch);
    auto routingOptions =
        makeTopologyRoutingOptions(config, config.topologyRoutingMode, true, true);
    if (options.deferDocumentLimitToVectorRanker) {
        routingOptions.maxDocs = 0;
    }
    config.topologyRouteRiskCalibration.routingPolicyFingerprint = topologyRoutingPolicyFingerprint(
        topologyRoutingRepresentationFingerprint(batch), routingOptions);
    config.topologyRouteRiskCalibration.datasetIdentity = "fixture-held-out-v1";
    config.topologyRouteRiskCalibration.calibrationQueries = 50;
    config.topologyRouteRiskCalibration.protectedCandidates = 100;
    config.topologyRouteRiskCalibration.missedProtectedCandidates = 0;
    config.topologyRouteRiskCalibration.minCalibrationQueries = 30;
    config.topologyRouteRiskCalibration.maxMissesPerThousand = 10;
    config.topologyRouteRiskCalibration.minBoundaryMargin = 0.0F;
    config.topologyRouteRiskCalibration.minSeedHits = 1;

    config.topologyRouteWorkBudget.maxRowsVisited = options.maxRowsVisited;
    config.topologyRouteWorkBudget.maxExactDistanceEvaluations =
        options.maxExactDistanceEvaluations;
    config.topologyRouteWorkBudget.maxAnnCandidates = options.maxAnnCandidates;
}

inline void configureCertifiedTopologyRoute(SearchEngineConfig& config,
                                            const yams::topology::TopologyArtifactBatch& batch,
                                            std::size_t maxRowsVisited = 2) {
    configureCertifiedTopologyRoute(
        config, batch, CertifiedTopologyRouteOptions{.maxRowsVisited = maxRowsVisited});
}

inline std::vector<std::string> resultHashes(const SearchResponse& response) {
    std::vector<std::string> hashes;
    hashes.reserve(response.results.size());
    for (const auto& result : response.results) {
        hashes.push_back(result.document.sha256Hash);
    }
    return hashes;
}

} // namespace yams::search::topology_test_support

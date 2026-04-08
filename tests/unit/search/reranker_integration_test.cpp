// SPDX-License-Identifier: GPL-3.0-or-later
// Unit tests for reranker integration into search pipeline

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/compat/unistd.h>
#include <yams/core/types.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/internal_benchmark.h>
#include <yams/search/reranker_adapter.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace yams::search {
namespace {

using Catch::Approx;

// =============================================================================
// Mock Reranker for Testing
// =============================================================================

class MockReranker : public IReranker {
public:
    MockReranker() = default;

    void setReady(bool ready) { ready_ = ready; }
    void setScores(const std::vector<float>& scores) { fixedScores_ = scores; }
    void setFailOnCall(bool fail) { failOnCall_ = fail; }

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        callCount_++;
        lastQuery_ = query;
        lastDocuments_ = documents;

        if (failOnCall_) {
            return Error{ErrorCode::InternalError, "Mock reranker failure"};
        }

        if (!fixedScores_.empty()) {
            return fixedScores_;
        }

        // Default: return descending scores based on document index
        std::vector<float> scores;
        scores.reserve(documents.size());
        for (size_t i = 0; i < documents.size(); ++i) {
            scores.push_back(1.0f - (static_cast<float>(i) / static_cast<float>(documents.size())));
        }
        return scores;
    }

    [[nodiscard]] bool isReady() const override { return ready_; }

    // Test helpers
    int getCallCount() const { return callCount_; }
    const std::string& getLastQuery() const { return lastQuery_; }
    const std::vector<std::string>& getLastDocuments() const { return lastDocuments_; }
    void reset() {
        callCount_ = 0;
        lastQuery_.clear();
        lastDocuments_.clear();
    }

private:
    bool ready_ = true;
    bool failOnCall_ = false;
    std::vector<float> fixedScores_;
    mutable int callCount_ = 0;
    mutable std::string lastQuery_;
    mutable std::vector<std::string> lastDocuments_;
};

// =============================================================================
// Mock Model Provider for Adapter Testing
// =============================================================================

class MockModelProvider : public daemon::IModelProvider {
public:
    MockModelProvider() = default;

    void setAvailable(bool available) { available_ = available; }
    void setScores(const std::vector<float>& scores) { scores_ = scores; }

    // Provider state
    bool isAvailable() const override { return available_; }
    std::string getProviderName() const override { return "mock-provider"; }
    std::string getProviderVersion() const override { return "1.0.0"; }

    // Reranking - the main functionality we're testing
    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        if (!available_) {
            return Error{ErrorCode::InvalidState, "Provider not available"};
        }
        if (!scores_.empty()) {
            return scores_;
        }
        // Default behavior: return word overlap scores
        std::vector<float> result;
        result.reserve(documents.size());
        for (const auto& doc : documents) {
            float score = (doc.find(query) != std::string::npos) ? 0.9f : 0.1f;
            result.push_back(score);
        }
        return result;
    }

    // Model operations - stubs
    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return std::vector<float>(384, 0.0f);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        std::vector<std::vector<float>> results;
        results.reserve(texts.size());
        for (size_t i = 0; i < texts.size(); ++i) {
            results.emplace_back(384, 0.0f);
        }
        return results;
    }
    Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                    const std::string&) override {
        return std::vector<float>(384, 0.0f);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>& texts) override {
        std::vector<std::vector<float>> results;
        results.reserve(texts.size());
        for (size_t i = 0; i < texts.size(); ++i) {
            results.emplace_back(384, 0.0f);
        }
        return results;
    }

    // Model management - stubs
    Result<void> loadModel(const std::string&) override { return {}; }
    Result<void> unloadModel(const std::string&) override { return {}; }
    bool isModelLoaded(const std::string&) const override { return true; }
    std::vector<std::string> getLoadedModels() const override { return {"mock-model"}; }
    size_t getLoadedModelCount() const override { return 1; }

    // Model info - stubs
    Result<daemon::ModelInfo> getModelInfo(const std::string& modelName) const override {
        daemon::ModelInfo info;
        info.name = modelName;
        info.embeddingDim = 384;
        return info;
    }
    size_t getEmbeddingDim(const std::string&) const override { return 384; }
    std::shared_ptr<vector::EmbeddingGenerator> getEmbeddingGenerator(const std::string&) override {
        return nullptr;
    }

    // Resource management - stubs
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

private:
    bool available_ = true;
    std::vector<float> scores_;
};

} // namespace

// =============================================================================
// IReranker Interface Tests
// =============================================================================

TEST_CASE("IReranker: Basic interface contract", "[search][reranker][interface]") {
    auto reranker = std::make_shared<MockReranker>();

    SECTION("isReady returns correct state") {
        REQUIRE(reranker->isReady());

        reranker->setReady(false);
        REQUIRE_FALSE(reranker->isReady());
    }

    SECTION("scoreDocuments returns correct number of scores") {
        std::vector<std::string> docs = {"doc1", "doc2", "doc3"};
        auto result = reranker->scoreDocuments("query", docs);

        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 3);
    }

    SECTION("scoreDocuments passes query and documents correctly") {
        std::vector<std::string> docs = {"first", "second"};
        auto result = reranker->scoreDocuments("test query", docs);

        REQUIRE(result.has_value());
        REQUIRE(reranker->getLastQuery() == "test query");
        REQUIRE(reranker->getLastDocuments() == docs);
    }

    SECTION("scoreDocuments can return fixed scores") {
        reranker->setScores({0.9f, 0.5f, 0.1f});
        std::vector<std::string> docs = {"a", "b", "c"};

        auto result = reranker->scoreDocuments("query", docs);

        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 3);
        CHECK_THAT(result.value()[0], Catch::Matchers::WithinAbs(0.9f, 0.001f));
        CHECK_THAT(result.value()[1], Catch::Matchers::WithinAbs(0.5f, 0.001f));
        CHECK_THAT(result.value()[2], Catch::Matchers::WithinAbs(0.1f, 0.001f));
    }

    SECTION("scoreDocuments can return error") {
        reranker->setFailOnCall(true);
        std::vector<std::string> docs = {"doc"};

        auto result = reranker->scoreDocuments("query", docs);

        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().code == ErrorCode::InternalError);
    }
}

// =============================================================================
// ModelProviderRerankerAdapter Tests
// =============================================================================

TEST_CASE("ModelProviderRerankerAdapter: Wraps model provider correctly",
          "[search][reranker][adapter]") {
    auto mockProvider = std::make_shared<MockModelProvider>();

    ModelProviderRerankerAdapter adapter([mockProvider]() { return mockProvider; });

    SECTION("isReady reflects provider availability") {
        REQUIRE(adapter.isReady());

        mockProvider->setAvailable(false);
        REQUIRE_FALSE(adapter.isReady());
    }

    SECTION("scoreDocuments delegates to provider") {
        mockProvider->setScores({0.8f, 0.6f, 0.4f});
        std::vector<std::string> docs = {"doc1", "doc2", "doc3"};

        auto result = adapter.scoreDocuments("query", docs);

        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 3);
        CHECK_THAT(result.value()[0], Catch::Matchers::WithinAbs(0.8f, 0.001f));
        CHECK_THAT(result.value()[1], Catch::Matchers::WithinAbs(0.6f, 0.001f));
        CHECK_THAT(result.value()[2], Catch::Matchers::WithinAbs(0.4f, 0.001f));
    }

    SECTION("Returns error when provider unavailable") {
        mockProvider->setAvailable(false);
        std::vector<std::string> docs = {"doc"};

        auto result = adapter.scoreDocuments("query", docs);

        REQUIRE_FALSE(result.has_value());
    }

    SECTION("Handles null provider gracefully") {
        ModelProviderRerankerAdapter nullAdapter([]() { return nullptr; });

        REQUIRE_FALSE(nullAdapter.isReady());

        std::vector<std::string> docs = {"doc"};
        auto result = nullAdapter.scoreDocuments("query", docs);
        REQUIRE_FALSE(result.has_value());
    }
}

// =============================================================================
// SearchEngineConfig Reranker Settings Tests
// =============================================================================

TEST_CASE("SearchEngineConfig: Reranker settings", "[search][reranker][config]") {
    SearchEngineConfig config;

    SECTION("Default reranker settings") {
        REQUIRE(config.enableReranking);
        REQUIRE(config.rerankTopK == 5);
        CHECK_THAT(config.rerankWeight, Catch::Matchers::WithinAbs(0.60f, 0.001f));
        REQUIRE(config.rerankReplaceScores == true);
    }

    SECTION("Can enable reranking") {
        config.enableReranking = true;
        REQUIRE(config.enableReranking);
    }

    SECTION("Can configure topK") {
        config.rerankTopK = 100;
        REQUIRE(config.rerankTopK == 100);
    }

    SECTION("Can configure weight blending") {
        config.rerankWeight = 0.8f;
        config.rerankReplaceScores = false;
        CHECK_THAT(config.rerankWeight, Catch::Matchers::WithinAbs(0.8f, 0.001f));
        REQUIRE_FALSE(config.rerankReplaceScores);
    }
}

// =============================================================================
// SearchResult rerankerScore Field Tests
// =============================================================================

TEST_CASE("SearchResult: rerankerScore field", "[search][reranker][result]") {
    metadata::SearchResult result;

    SECTION("rerankerScore is optional and initially empty") {
        REQUIRE_FALSE(result.rerankerScore.has_value());
    }

    SECTION("Can set rerankerScore") {
        result.rerankerScore = 0.95;
        REQUIRE(result.rerankerScore.has_value());
        CHECK_THAT(result.rerankerScore.value(), Catch::Matchers::WithinAbs(0.95, 0.001));
    }

    SECTION("rerankerScore is separate from main score") {
        result.score = 0.5;
        result.rerankerScore = 0.9;

        CHECK_THAT(result.score, Catch::Matchers::WithinAbs(0.5, 0.001));
        CHECK_THAT(result.rerankerScore.value(), Catch::Matchers::WithinAbs(0.9, 0.001));
    }
}

namespace {

std::filesystem::path makeTempDbPath() {
    namespace fs = std::filesystem;
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dbPath = fs::temp_directory_path() / ("reranker_cooldown_" + std::to_string(::getpid()) +
                                               "_" + std::to_string(now) + ".db");
    std::error_code ec;
    fs::remove(dbPath, ec);
    return dbPath;
}

yams::metadata::DocumentInfo makeDocument(const std::string& filePath, const std::string& hash) {
    yams::metadata::DocumentInfo doc;
    doc.filePath = filePath;
    auto derived = yams::metadata::computePathDerivedValues(filePath);
    doc.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
    doc.fileExtension = std::filesystem::path(doc.fileName).extension().string();
    doc.sha256Hash = hash;
    doc.pathPrefix = derived.pathPrefix;
    doc.reversePath = derived.reversePath;
    doc.pathHash = derived.pathHash;
    doc.parentHash = derived.parentHash;
    doc.pathDepth = derived.pathDepth;
    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.createdTime = now;
    doc.modifiedTime = now;
    doc.indexedTime = now;
    return doc;
}

class NotImplementedReranker : public IReranker {
public:
    Result<std::vector<float>> scoreDocuments(const std::string&,
                                              const std::vector<std::string>&) override {
        callCount_.fetch_add(1, std::memory_order_relaxed);
        return Error{ErrorCode::NotImplemented, "plugin error: score_documents"};
    }

    bool isReady() const override { return true; }

    int callCount() const { return callCount_.load(std::memory_order_relaxed); }

private:
    std::atomic<int> callCount_{0};
};

class SearchEngineRerankerFixture {
public:
    SearchEngineRerankerFixture() : dbPath_(makeTempDbPath()) {
        pool_ = std::make_unique<yams::metadata::ConnectionPool>(dbPath_.string());
        auto init = pool_->initialize();
        if (!init) {
            throw std::runtime_error("Failed to initialize connection pool");
        }
        repo_ = std::make_shared<yams::metadata::MetadataRepository>(*pool_);
        yams::metadata::KnowledgeGraphStoreConfig kgConfig;
        auto kgResult = yams::metadata::makeSqliteKnowledgeGraphStore(*pool_, kgConfig);
        if (!kgResult) {
            throw std::runtime_error("Failed to initialize KG store");
        }
        kgStore_ =
            std::shared_ptr<yams::metadata::KnowledgeGraphStore>(std::move(kgResult).value());
        repo_->setKnowledgeGraphStore(kgStore_);
    }

    ~SearchEngineRerankerFixture() {
        if (pool_) {
            pool_->shutdown();
        }
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    void addIndexedDocument(const std::string& filePath, const std::string& hash,
                            const std::string& title, const std::string& content) {
        auto doc = makeDocument(filePath, hash);
        auto idRes = repo_->insertDocument(doc);
        if (!idRes) {
            throw std::runtime_error("Failed to insert test document");
        }

        yams::metadata::DocumentContent storedContent;
        storedContent.documentId = idRes.value();
        storedContent.contentText = content;
        storedContent.contentLength = static_cast<int64_t>(content.size());
        storedContent.extractionMethod = "test";
        storedContent.language = "en";
        auto contentRes = repo_->insertContent(storedContent);
        if (!contentRes) {
            throw std::runtime_error("Failed to insert test document content");
        }

        auto indexRes = repo_->indexDocumentContent(idRes.value(), title, content, "text/plain");
        if (!indexRes) {
            throw std::runtime_error("Failed to index test document content");
        }

        yams::metadata::KGNode docNode;
        docNode.nodeKey = "doc:" + hash;
        docNode.label = filePath;
        docNode.type = "document";
        auto nodeId = kgStore_->upsertNode(docNode);
        if (!nodeId) {
            throw std::runtime_error("Failed to upsert test KG document node");
        }
    }

    void addDocEntityByHash(const std::string& hash, const std::string& entityText) {
        auto docId = kgStore_->getDocumentIdByHash(hash);
        if (!docId || !docId.value().has_value()) {
            throw std::runtime_error("Failed to resolve test document by hash");
        }

        yams::metadata::KGNode entityNode;
        entityNode.nodeKey = "entity:" + entityText;
        entityNode.label = entityText;
        entityNode.type = "topic";
        auto entityId = kgStore_->upsertNode(entityNode);
        if (!entityId) {
            throw std::runtime_error("Failed to upsert test entity node");
        }

        yams::metadata::DocEntity docEntity;
        docEntity.documentId = *docId.value();
        docEntity.entityText = entityText;
        docEntity.nodeId = entityId.value();
        docEntity.confidence = 1.0f;
        auto entityRes = kgStore_->addDocEntities({docEntity});
        if (!entityRes) {
            throw std::runtime_error("Failed to insert test doc entity");
        }
    }

    void addReciprocalSemanticNeighborsByHash(const std::string& leftHash,
                                              const std::string& rightHash, float weight = 0.95f) {
        auto leftNode = kgStore_->getNodeByKey("doc:" + leftHash);
        auto rightNode = kgStore_->getNodeByKey("doc:" + rightHash);
        if (!leftNode || !leftNode.value().has_value() || !rightNode ||
            !rightNode.value().has_value()) {
            throw std::runtime_error("Failed to resolve test KG doc nodes for semantic neighbors");
        }

        yams::metadata::KGEdge lr;
        lr.srcNodeId = leftNode.value()->id;
        lr.dstNodeId = rightNode.value()->id;
        lr.relation = "semantic_neighbor";
        lr.weight = weight;

        yams::metadata::KGEdge rl;
        rl.srcNodeId = rightNode.value()->id;
        rl.dstNodeId = leftNode.value()->id;
        rl.relation = "semantic_neighbor";
        rl.weight = weight;

        auto edgeRes = kgStore_->addEdgesUnique({lr, rl});
        if (!edgeRes) {
            throw std::runtime_error("Failed to insert reciprocal semantic neighbors");
        }
    }

    std::shared_ptr<yams::metadata::MetadataRepository> repo() const { return repo_; }
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore() const { return kgStore_; }

private:
    std::filesystem::path dbPath_;
    std::unique_ptr<yams::metadata::ConnectionPool> pool_;
    std::shared_ptr<yams::metadata::MetadataRepository> repo_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
};

class EnvGuard {
public:
    EnvGuard(const std::string& key, const std::string& value) : key_(key) {
        const char* existing = std::getenv(key_.c_str());
        if (existing) {
            oldValue_ = existing;
            hadValue_ = true;
        }
        setenv(key_.c_str(), value.c_str(), 1);
    }

    ~EnvGuard() {
        if (hadValue_) {
            setenv(key_.c_str(), oldValue_.c_str(), 1);
        } else {
            unsetenv(key_.c_str());
        }
    }

private:
    std::string key_;
    std::string oldValue_;
    bool hadValue_{false};
};

} // namespace

TEST_CASE("SearchEngine: reranker not-implemented errors enter cooldown",
          "[search][reranker][cooldown]") {
    SearchEngineRerankerFixture fixture;
    fixture.addIndexedDocument(
        "/tmp/reranker_cooldown_doc.md", "HASH_RERANK_COOLDOWN_DOC", "How YAMS stores documents",
        "YAMS can store documents with metadata. Explain how YAMS stores documents.");

    SearchEngineConfig config;
    config.textWeight = 0.0f;
    config.pathTreeWeight = 1.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableReranking = true;
    config.rerankTopK = 5;
    config.rerankScoreGapThreshold = 0.0f;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, nullptr, config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<NotImplementedReranker>();
    engine->setReranker(reranker);

    const std::string query = "/tmp/reranker_cooldown_doc.md";

    auto first = engine->searchWithResponse(query, {});
    REQUIRE(first.has_value());
    REQUIRE_FALSE(first.value().results.empty());
    CHECK(reranker->callCount() == 1);

    auto second = engine->searchWithResponse(query, {});
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().results.empty());

    // Cooldown should prevent repeated plugin calls immediately after an
    // unsupported reranker failure.
    CHECK(reranker->callCount() == 1);
    CHECK(std::find(second.value().skippedComponents.begin(),
                    second.value().skippedComponents.end(),
                    "reranker") != second.value().skippedComponents.end());
}

TEST_CASE("SearchEngine: reranker falls back to metadata content preview",
          "[search][reranker][metadata-preview]") {
    SearchEngineRerankerFixture fixture;
    const std::string filePath = "/tmp/reranker_preview_doc.md";
    const std::string hash = "HASH_RERANK_PREVIEW_DOC";
    const std::string content =
        "0-dimensional biomaterials show inductive properties in scaffold studies.";
    fixture.addIndexedDocument(filePath, hash, "Biomaterials note", content);

    SearchEngineConfig config;
    config.textWeight = 0.0f;
    config.pathTreeWeight = 1.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableReranking = true;
    config.rerankTopK = 5;
    config.rerankScoreGapThreshold = 0.0f;
    config.rerankSnippetMaxChars = 48;
    // Use Map zoom to prevent zoom calibration from overriding rerankSnippetMaxChars upward
    // (Neighborhood/Street use std::max which bumps small test values to 256/384)
    config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, nullptr, config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.8f});
    engine->setReranker(reranker);

    auto response = engine->searchWithResponse(filePath, {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() == 1);
    REQUIRE(reranker->getCallCount() == 1);
    REQUIRE(reranker->getLastDocuments().size() == 1);

    CHECK(reranker->getLastDocuments()[0].find("0-dimensional") != std::string::npos);
    CHECK(reranker->getLastDocuments()[0].size() <= config.rerankSnippetMaxChars + 3);
    REQUIRE(response.value().results[0].rerankerScore.has_value());
    CHECK_THAT(response.value().results[0].rerankerScore.value(),
               Catch::Matchers::WithinAbs(0.8, 0.001));
}

TEST_CASE("SearchEngine: reranker preview preserves tail evidence for long metadata content",
          "[search][reranker][metadata-preview]") {
    SearchEngineRerankerFixture fixture;
    const std::string filePath = "/tmp/reranker_preview_tail_doc.md";
    const std::string hash = "HASH_RERANK_PREVIEW_TAIL_DOC";
    const std::string content =
        "New opportunities: the use of nanotechnologies to manipulate and track stem cells. "
        "Nanotechnologies are emerging platforms that could be useful in measuring and "
        "manipulating stem cells. Examples include magnetic nanoparticles and quantum dots "
        "for stem cell labeling and in vivo tracking; engineered nanometer-scale scaffolds "
        "for stem cell differentiation and transplantation demonstrate biomaterial inductive "
        "properties.";
    fixture.addIndexedDocument(filePath, hash, "Biomaterials note", content);

    SearchEngineConfig config;
    config.textWeight = 0.0f;
    config.pathTreeWeight = 1.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableReranking = true;
    config.rerankTopK = 5;
    config.rerankScoreGapThreshold = 0.0f;
    config.rerankSnippetMaxChars = 96;
    config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, nullptr, config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.8f});
    engine->setReranker(reranker);

    auto response = engine->searchWithResponse(filePath, {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() == 1);
    REQUIRE(reranker->getCallCount() == 1);
    REQUIRE(reranker->getLastDocuments().size() >= 1);

    const bool hasTailEvidence =
        std::any_of(reranker->getLastDocuments().begin(), reranker->getLastDocuments().end(),
                    [](const std::string& doc) {
                        return doc.find("inductive properties") != std::string::npos;
                    });
    CHECK(hasTailEvidence);
    CHECK(std::all_of(
        reranker->getLastDocuments().begin(), reranker->getLastDocuments().end(),
        [&](const std::string& doc) { return doc.size() <= config.rerankSnippetMaxChars + 3; }));
}

TEST_CASE("SearchEngine: score gap guard still reranks when another strong fused candidate exists",
          "[search][reranker][score-gap]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTop = "/tmp/reranker_gap_top.md";
    const std::string pathSecond = "/tmp/reranker_gap_second.md";
    const std::string pathThird = "/tmp/reranker_gap_third.md";
    const std::string hashTop = "HASH_RERANK_GAP_TOP";
    const std::string hashSecond = "HASH_RERANK_GAP_SECOND";
    const std::string hashThird = "HASH_RERANK_GAP_THIRD";

    fixture.addIndexedDocument(pathTop, hashTop, "alpha target top",
                               "alpha target top alpha target top");
    fixture.addIndexedDocument(pathSecond, hashSecond, "alpha filler", "alpha filler");
    fixture.addIndexedDocument(pathThird, hashThird, "alpha target third",
                               "alpha target third alpha target third alpha target third");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableReranking = true;
    config.rerankTopK = 3;
    config.rerankScoreGapThreshold = 0.05f;
    config.rerankReplaceScores = true;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, nullptr, config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.2f, 0.1f, 0.95f});
    engine->setReranker(reranker);

    auto response = engine->searchWithResponse("alpha target", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() >= 3);

    CHECK(reranker->getCallCount() == 1);
    CHECK(std::find(response.value().skippedComponents.begin(),
                    response.value().skippedComponents.end(),
                    "reranker") == response.value().skippedComponents.end());
    CHECK(std::any_of(response.value().results.begin(), response.value().results.end(),
                      [](const auto& result) { return result.rerankerScore.has_value(); }));
}

TEST_CASE("SearchEngine: score gap guard yields to corroborated anchored evidence deeper in window",
          "[search][reranker][score-gap]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTop = "/tmp/reranker_gap_anchor_top.md";
    const std::string pathMid = "/tmp/reranker_gap_anchor_mid.md";
    const std::string pathDeep = "/tmp/reranker_gap_anchor_deep.md";
    const std::string hashTop = "HASH_RERANK_GAP_ANCHOR_TOP";
    const std::string hashMid = "HASH_RERANK_GAP_ANCHOR_MID";
    const std::string hashDeep = "HASH_RERANK_GAP_ANCHOR_DEEP";

    fixture.addIndexedDocument(pathTop, hashTop, "alpha top", "alpha top alpha top");
    fixture.addIndexedDocument(pathMid, hashMid, "alpha middle", "alpha middle");
    fixture.addIndexedDocument(pathDeep, hashDeep, "alpha deep target",
                               "alpha deep target alpha deep target alpha deep target");
    fixture.addDocEntityByHash(hashDeep, "alpha");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 1.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 3;
    config.rerankScoreGapThreshold = 0.05f;
    config.rerankReplaceScores = true;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.1f, 0.2f, 0.9f});
    engine->setReranker(reranker);

    auto response = engine->searchWithResponse("alpha", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() >= 3);

    CHECK(reranker->getCallCount() == 1);
    CHECK(std::find(response.value().skippedComponents.begin(),
                    response.value().skippedComponents.end(),
                    "reranker") == response.value().skippedComponents.end());
    CHECK(std::any_of(
        response.value().results.begin(), response.value().results.end(), [&](const auto& result) {
            return result.document.sha256Hash == hashDeep && result.rerankerScore.has_value();
        }));
}

TEST_CASE("SearchEngine: score gap guard yields to corroborated anchored evidence in second slot",
          "[search][reranker][score-gap]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTop = "/tmp/reranker_gap_anchor2_top.md";
    const std::string pathSecond = "/tmp/reranker_gap_anchor2_second.md";
    const std::string pathThird = "/tmp/reranker_gap_anchor2_third.md";
    const std::string hashTop = "HASH_RERANK_GAP_ANCHOR2_TOP";
    const std::string hashSecond = "HASH_RERANK_GAP_ANCHOR2_SECOND";
    const std::string hashThird = "HASH_RERANK_GAP_ANCHOR2_THIRD";

    fixture.addIndexedDocument(pathTop, hashTop, "alpha top", "alpha top alpha top alpha top");
    fixture.addIndexedDocument(pathSecond, hashSecond, "alpha corroborated",
                               "alpha corroborated alpha corroborated");
    fixture.addIndexedDocument(pathThird, hashThird, "alpha third", "alpha third");
    fixture.addDocEntityByHash(hashSecond, "alpha");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 1.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 3;
    config.rerankScoreGapThreshold = 0.05f;
    config.rerankReplaceScores = true;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.1f, 0.95f, 0.2f});
    engine->setReranker(reranker);

    auto response = engine->searchWithResponse("alpha", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() >= 3);

    CHECK(reranker->getCallCount() == 1);
    CHECK(std::find(response.value().skippedComponents.begin(),
                    response.value().skippedComponents.end(),
                    "reranker") == response.value().skippedComponents.end());
    CHECK(std::any_of(
        response.value().results.begin(), response.value().results.end(), [&](const auto& result) {
            return result.document.sha256Hash == hashSecond && result.rerankerScore.has_value();
        }));
}

TEST_CASE("SearchEngine: evidence rescue keeps strongest reranked winner in top slot",
          "[search][reranker][evidence-rescue]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTop = "/tmp/reranker_evidence_top.md";
    const std::string pathMid = "/tmp/reranker_evidence_mid.md";
    const std::string pathTail = "/tmp/reranker_evidence_tail.md";
    const std::string hashTop = "HASH_RERANK_EVIDENCE_TOP";
    const std::string hashMid = "HASH_RERANK_EVIDENCE_MID";
    const std::string hashTail = "HASH_RERANK_EVIDENCE_TAIL";

    fixture.addIndexedDocument(pathTop, hashTop, "alpha top", "alpha top alpha top");
    fixture.addIndexedDocument(pathMid, hashMid, "alpha mid", "alpha mid alpha mid");
    fixture.addIndexedDocument(pathTail, hashTail, "alpha tail target",
                               "alpha tail target alpha tail target alpha tail target");
    fixture.addDocEntityByHash(hashMid, "alpha");
    fixture.addDocEntityByHash(hashTail, "alpha");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 1.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 5;
    config.rerankScoreGapThreshold = 0.05f;
    config.rerankReplaceScores = true;
    config.fusionEvidenceRescueSlots = 1;
    config.fusionEvidenceRescueMinScore = 0.01f;
    config.maxResults = 1;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.01f, 0.02f, 0.9f});
    engine->setReranker(reranker);

    SearchParams params;
    params.limit = 1;
    auto response = engine->searchWithResponse("alpha", params);
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() == 1);

    CHECK(reranker->getCallCount() == 1);
    CHECK(response.value().results[0].document.sha256Hash == hashTail);
    CHECK(response.value().results[0].rerankerScore.has_value());
    CHECK(response.value().results[0].rerankerScore.value() == Catch::Approx(0.9f));
    CHECK(response.value().debugStats.at("evidence_rescue_displaced_doc_ids").find(hashTail) ==
          std::string::npos);
}

TEST_CASE("SearchEngine: prose sentences are not misclassified as code intent",
          "[search][intent]") {
    EnvGuard traceGuard("YAMS_SEARCH_STAGE_TRACE", "1");

    SearchEngineRerankerFixture fixture;
    fixture.addIndexedDocument(
        "/tmp/intent_prose_doc.md", "HASH_INTENT_PROSE", "Prose doc",
        "The genomic aberrations found in metastases are similar to those in the primary tumor.");

    SearchEngineConfig config;
    config.enableParallelExecution = false;
    config.enableIntentAdaptiveWeighting = true;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto response = engine->searchWithResponse("The genomic aberrations found in matasteses are "
                                               "very similar to those found in the primary tumor.",
                                               {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().debugStats.contains("trace_query_intent"));
    CHECK(response.value().debugStats.at("trace_query_intent") == "prose");
}

TEST_CASE("SearchEngine: camelCase identifiers remain code intent", "[search][intent]") {
    EnvGuard traceGuard("YAMS_SEARCH_STAGE_TRACE", "1");

    SearchEngineRerankerFixture fixture;
    fixture.addIndexedDocument("/tmp/intent_code_doc.md", "HASH_INTENT_CODE", "Code doc",
                               "Search for renderGraphNode and related helpers.");

    SearchEngineConfig config;
    config.enableParallelExecution = false;
    config.enableIntentAdaptiveWeighting = true;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto response = engine->searchWithResponse("renderGraphNode", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().debugStats.contains("trace_query_intent"));
    CHECK(response.value().debugStats.at("trace_query_intent") == "code");
}

TEST_CASE("SearchEngine: rerank replacement keeps reranked docs ahead of untouched fused docs",
          "[search][reranker][replace-order]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTop = "/tmp/rerank_replace_top.md";
    const std::string pathSecond = "/tmp/rerank_replace_second.md";
    const std::string pathTail = "/tmp/rerank_replace_tail.md";
    const std::string hashTop = "HASH_RERANK_REPLACE_TOP";
    const std::string hashSecond = "HASH_RERANK_REPLACE_SECOND";
    const std::string hashTail = "HASH_RERANK_REPLACE_TAIL";

    fixture.addIndexedDocument(pathTop, hashTop, "alpha top", "alpha top alpha top");
    fixture.addIndexedDocument(pathSecond, hashSecond, "alpha second", "alpha second alpha second");
    fixture.addIndexedDocument(pathTail, hashTail, "alpha tail", "alpha tail alpha tail");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 2;
    config.rerankScoreGapThreshold = 0.0f;
    config.rerankReplaceScores = true;
    config.fusionEvidenceRescueSlots = 0;
    config.includeDebugInfo = true;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto reranker = std::make_shared<MockReranker>();
    reranker->setScores({0.8f, 0.7f});
    engine->setReranker(reranker);

    SearchParams params;
    params.limit = 1;
    auto response = engine->searchWithResponse("alpha", params);
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() == 1);

    CHECK(reranker->getCallCount() == 1);
    CHECK(response.value().results[0].document.sha256Hash == hashTop);
    CHECK(response.value().results[0].rerankerScore.has_value());
    CHECK(response.value().results[0].rerankerScore.value() == Catch::Approx(0.8f));
}

TEST_CASE("SearchEngine: graph rerank emits reciprocal community telemetry",
          "[search][graph][community]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathA = "/tmp/community_alpha.md";
    const std::string pathB = "/tmp/community_beta.md";
    const std::string hashA = "HASH_COMMUNITY_ALPHA";
    const std::string hashB = "HASH_COMMUNITY_BETA";

    fixture.addIndexedDocument(pathA, hashA, "Alpha topic", "alpha matching content");
    fixture.addIndexedDocument(pathB, hashB, "Beta topic", "alpha matching companion content");
    fixture.addDocEntityByHash(hashA, "alpha");
    fixture.addDocEntityByHash(hashB, "alpha");
    fixture.addReciprocalSemanticNeighborsByHash(hashA, hashB);

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = true;
    config.graphRerankTopN = 5;
    config.graphRerankWeight = 1.0f;
    config.graphRerankMaxBoost = 1.0f;
    config.graphRerankMinSignal = 0.01f;
    config.includeDebugInfo = true;
    config.enableReranking = false;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto response = engine->searchWithResponse("alpha", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() >= 2);
    CHECK(response.value().debugStats.at("graph_community_supported_docs") == "2");
    CHECK(response.value().debugStats.at("graph_community_edge_count") == "1");
    CHECK(response.value().debugStats.at("graph_community_largest_size") == "2");
    CHECK(response.value().debugStats.at("graph_community_boosted_docs") == "2");
    CHECK(std::stod(response.value().debugStats.at("graph_community_signal_mass")) > 0.0);
    CHECK(response.value().debugStats.at("graph_community_weight") == "0.1000");
}

TEST_CASE("SearchEngine: graph rerank can disable community influence while preserving telemetry",
          "[search][graph][community]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathA = "/tmp/community_zero_alpha.md";
    const std::string pathB = "/tmp/community_zero_beta.md";
    const std::string hashA = "HASH_COMMUNITY_ZERO_ALPHA";
    const std::string hashB = "HASH_COMMUNITY_ZERO_BETA";

    fixture.addIndexedDocument(pathA, hashA, "Alpha topic", "alpha matching content");
    fixture.addIndexedDocument(pathB, hashB, "Beta topic", "alpha matching companion content");
    fixture.addDocEntityByHash(hashA, "alpha");
    fixture.addDocEntityByHash(hashB, "alpha");
    fixture.addReciprocalSemanticNeighborsByHash(hashA, hashB);

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = true;
    config.graphRerankTopN = 5;
    config.graphRerankWeight = 1.0f;
    config.graphRerankMaxBoost = 1.0f;
    config.graphRerankMinSignal = 0.01f;
    config.graphCommunityWeight = 0.0f;
    config.includeDebugInfo = true;
    config.enableReranking = false;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    auto response = engine->searchWithResponse("alpha", {});
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() >= 2);
    CHECK(response.value().debugStats.at("graph_community_supported_docs") == "2");
    CHECK(response.value().debugStats.at("graph_community_edge_count") == "1");
    CHECK(response.value().debugStats.at("graph_community_largest_size") == "2");
    CHECK(response.value().debugStats.at("graph_community_boosted_docs") == "0");
    CHECK(std::stod(response.value().debugStats.at("graph_community_signal_mass")) > 0.0);
    CHECK(response.value().debugStats.at("graph_community_weight") == "0.0000");
}

TEST_CASE("InternalBenchmark: reciprocal community signal improves rank when enabled",
          "[search][graph][community][benchmark]") {
    SearchEngineRerankerFixture fixture;
    const std::string pathTarget = "/tmp/community_bench_target.md";
    const std::string pathPartner = "/tmp/community_bench_partner.md";
    const std::string pathRival = "/tmp/community_bench_rival.md";
    const std::string hashTarget = "HASH_COMMUNITY_BENCH_TARGET";
    const std::string hashPartner = "HASH_COMMUNITY_BENCH_PARTNER";
    const std::string hashRival = "HASH_COMMUNITY_BENCH_RIVAL";

    fixture.addIndexedDocument(pathTarget, hashTarget, "Target note", "alpha target evidence");
    fixture.addIndexedDocument(pathPartner, hashPartner, "Partner note",
                               "alpha companion evidence");
    fixture.addIndexedDocument(pathRival, hashRival, "Rival alpha note",
                               "alpha rival alpha rival evidence");
    fixture.addReciprocalSemanticNeighborsByHash(hashTarget, hashPartner);

    auto makeEngine = [&](float communityWeight) {
        SearchEngineConfig config;
        config.textWeight = 1.0f;
        config.pathTreeWeight = 0.0f;
        config.kgWeight = 0.0f;
        config.vectorWeight = 0.0f;
        config.entityVectorWeight = 0.0f;
        config.tagWeight = 0.0f;
        config.metadataWeight = 0.0f;
        config.graphTextWeight = 0.0f;
        config.graphVectorWeight = 0.0f;
        config.enableParallelExecution = false;
        config.enableGraphRerank = true;
        config.graphRerankTopN = 3;
        config.graphRerankWeight = 1.0f;
        config.graphRerankMaxBoost = 1.0f;
        config.graphRerankMinSignal = 0.01f;
        config.graphCommunityWeight = communityWeight;
        config.includeDebugInfo = true;
        config.enableReranking = false;
        config.maxResults = 3;

        auto engine =
            createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
        REQUIRE(engine != nullptr);
        return std::shared_ptr<SearchEngine>(std::move(engine));
    };

    std::vector<SyntheticQuery> queries;
    queries.push_back(SyntheticQuery{.text = "alpha rival",
                                     .expectedDocHash = hashTarget,
                                     .expectedFilePath = pathTarget,
                                     .type = QueryType::KNOWN_ITEM,
                                     .sourcePhrase = "alpha rival"});

    BenchmarkConfig benchConfig;
    benchConfig.k = 1;
    benchConfig.warmupQueries = 0;
    benchConfig.includeExecutions = true;

    auto withoutCommunity = makeEngine(0.0f);
    auto withCommunity = makeEngine(0.10f);

    InternalBenchmark baselineBench(withoutCommunity, fixture.repo());
    auto baselineResult = baselineBench.runWithQueries(queries, benchConfig);
    REQUIRE(baselineResult.has_value());

    InternalBenchmark communityBench(withCommunity, fixture.repo());
    auto communityResult = communityBench.runWithQueries(queries, benchConfig);
    REQUIRE(communityResult.has_value());

    REQUIRE(baselineResult.value().executions.size() == 1);
    REQUIRE(communityResult.value().executions.size() == 1);

    CHECK(baselineResult.value().executions[0].reciprocalRank == 0);
    CHECK_FALSE(baselineResult.value().executions[0].foundInTopK);
    CHECK(baselineResult.value().mrr < 1.0f);
    CHECK(baselineResult.value().recallAtK == Approx(0.0f));

    CHECK(communityResult.value().executions[0].reciprocalRank == 1);
    CHECK(communityResult.value().executions[0].foundInTopK);
    CHECK(communityResult.value().mrr == Approx(1.0f));
    CHECK(communityResult.value().recallAtK == Approx(1.0f));

    auto comparison =
        InternalBenchmark::compare(baselineResult.value(), communityResult.value(), 0.05f);
    CHECK(comparison.isImprovement);
    CHECK_FALSE(comparison.isRegression);
    CHECK(comparison.mrrDelta > 0.4f);
    CHECK(comparison.recallDelta > 0.9f);

    auto baselineResponse = withoutCommunity->searchWithResponse("alpha rival", {});
    REQUIRE(baselineResponse.has_value());
    CHECK(baselineResponse.value().debugStats.at("graph_community_weight") == "0.0000");
    CHECK(baselineResponse.value().debugStats.at("graph_community_boosted_docs") == "0");
    REQUIRE_FALSE(baselineResponse.value().results.empty());
    CHECK(baselineResponse.value().results.front().document.sha256Hash == hashRival);

    auto communityResponse = withCommunity->searchWithResponse("alpha rival", {});
    REQUIRE(communityResponse.has_value());
    CHECK(communityResponse.value().debugStats.at("graph_community_weight") == "0.1000");
    CHECK(communityResponse.value().debugStats.at("graph_community_supported_docs") == "2");
    CHECK(communityResponse.value().debugStats.at("graph_community_edge_count") == "1");
    CHECK(communityResponse.value().debugStats.at("graph_community_boosted_docs") == "2");
    CHECK(std::stod(communityResponse.value().debugStats.at("graph_community_signal_mass")) > 0.0);
    REQUIRE_FALSE(communityResponse.value().results.empty());
    CHECK(communityResponse.value().results.front().document.sha256Hash == hashTarget);
}

TEST_CASE(
    "SearchTuner: SCIENTIFIC profile applies lower similarity threshold and sub-phrase rescoring",
    "[search][tuner][rescoring]") {
    auto params = getTunedParams(TuningState::SCIENTIFIC);
    CHECK(params.similarityThreshold == Approx(0.40f));
    CHECK(params.enableSubPhraseRescoring == true);
    CHECK(params.subPhraseScoringPenalty == Approx(0.70f));

    // Verify applyTo() propagates them to SearchEngineConfig.
    SearchEngineConfig config;
    params.applyTo(config);
    CHECK(config.similarityThreshold == Approx(0.40f));
    CHECK(config.enableSubPhraseRescoring == true);
    CHECK(config.subPhraseScoringPenalty == Approx(0.70f));
}

TEST_CASE("SearchEngine: sub-phrase rescoring does not discard already-retrieved documents",
          "[search][expansion][rescoring]") {
    SearchEngineRerankerFixture fixture;

    // Multiple docs all containing some query keywords (simulating the "entire corpus
    // retrieved at low BM25 scores" pattern seen with long prose queries in SciFact).
    fixture.addIndexedDocument("/tmp/rescore_alpha.md", "HASH_RESCORE_ALPHA", "Alpha doc",
                               "chronic disease effects research study treatment");
    fixture.addIndexedDocument("/tmp/rescore_beta.md", "HASH_RESCORE_BETA", "Beta doc",
                               "chronic disease treatment management effects care");
    fixture.addIndexedDocument("/tmp/rescore_gamma.md", "HASH_RESCORE_GAMMA", "Gamma doc",
                               "effects treatment chronic disease analysis report");

    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.vectorWeight = 0.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.graphTextWeight = 0.0f;
    config.graphVectorWeight = 0.0f;
    config.enableParallelExecution = false;
    config.enableGraphRerank = false;
    config.enableReranking = false;
    config.enableSubPhraseRescoring = true;
    config.subPhraseScoringPenalty = 0.70f;
    config.maxResults = 10;

    auto engine = createSearchEngine(fixture.repo(), nullptr, nullptr, fixture.kgStore(), config);
    REQUIRE(engine != nullptr);

    SearchParams params;
    params.limit = 10;
    auto response = engine->searchWithResponse("chronic disease treatment effects", params);
    REQUIRE(response.has_value());
    REQUIRE_FALSE(response.value().results.empty());

    // All three docs should still be retrievable — rescoring must not silently drop docs.
    std::unordered_set<std::string> returned;
    for (const auto& r : response.value().results) {
        returned.insert(r.document.sha256Hash);
    }
    CHECK(returned.contains("HASH_RESCORE_ALPHA"));
    CHECK(returned.contains("HASH_RESCORE_BETA"));
    CHECK(returned.contains("HASH_RESCORE_GAMMA"));
}

} // namespace yams::search

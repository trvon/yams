// SPDX-License-Identifier: GPL-3.0-or-later
// Unit tests for reranker integration into search pipeline

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/compat/unistd.h>
#include <yams/core/types.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/reranker_adapter.h>
#include <yams/search/search_engine.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace yams::search {
namespace {

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
            scores.push_back(1.0f - static_cast<float>(i) / static_cast<float>(documents.size()));
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
            results.push_back(std::vector<float>(384, 0.0f));
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
            results.push_back(std::vector<float>(384, 0.0f));
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

        auto indexRes = repo_->indexDocumentContent(idRes.value(), title, content, "text/plain");
        if (!indexRes) {
            throw std::runtime_error("Failed to index test document content");
        }
    }

    std::shared_ptr<yams::metadata::MetadataRepository> repo() const { return repo_; }

private:
    std::filesystem::path dbPath_;
    std::unique_ptr<yams::metadata::ConnectionPool> pool_;
    std::shared_ptr<yams::metadata::MetadataRepository> repo_;
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

} // namespace yams::search

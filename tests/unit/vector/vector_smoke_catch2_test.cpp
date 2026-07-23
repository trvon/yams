// Catch2 tests for VectorDatabase smoke tests
// Migrated from GTest: vector_smoke_test.cpp
// PBI-040: Minimal vector smoke test (conditional on sqlite-vec availability)

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

using namespace yams::vector;

// Catch2 comparison macros trigger false-positive chained-comparison diagnostics in this file.
// NOLINTBEGIN(bugprone-chained-comparison)

namespace {

struct VectorSmokeFixture {
    VectorSmokeFixture() {
        // Check if sqlite-vec is available by attempting to load it
        // If YAMS_SQLITE_VEC_SKIP_INIT=1 or YAMS_DISABLE_VECTORS=1, skip these tests
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping vector smoke test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping vector smoke test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::string skipReason;
};

} // namespace

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke initialize in-memory database",
                 "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 128;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());
    INFO("Failed to initialize in-memory vector DB");
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke insert and search basic",
                 "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4; // Small dimension for speed
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert a simple vector
    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "test_hash_001";
    rec.document_hash = "test_doc_001";
    rec.embedding = embedding;
    rec.content = "Test content";
    rec.start_offset = 0;
    rec.end_offset = 12;
    REQUIRE(db.insertVector(rec));

    // Search for it
    VectorSearchParams params;
    params.k = 1;
    auto results = db.search(embedding, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "test_hash_001");
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke insert and search with vec0 engine",
                 "[vector][smoke][vec0][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::Vec0L2;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "vec0_hash_001";
    rec.document_hash = "vec0_doc_001";
    rec.embedding = embedding;
    rec.content = "Vec0 test content";
    rec.start_offset = 0;
    rec.end_offset = 17;
    REQUIRE(db.insertVector(rec));

    VectorSearchParams params;
    params.k = 1;
    auto results = db.search(embedding, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "vec0_hash_001");
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke insert and search with Simeon PQ engine",
                 "[vector][smoke][spq][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "spq_hash_001";
    rec.document_hash = "spq_doc_001";
    rec.embedding = embedding;
    rec.content = "Simeon PQ test content";
    rec.start_offset = 0;
    rec.end_offset = 27;
    REQUIRE(db.insertVector(rec));
    REQUIRE(db.buildIndex());

    VectorSearchParams params;
    params.k = 1;
    auto results = db.search(embedding, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "spq_hash_001");
}

TEST_CASE("Vector search engine names and compatibility aliases are stable",
          "[vector][contract][engine]") {
    CHECK(std::string_view(vectorSearchEngineName(VectorSearchEngine::SimeonPqAdc)) ==
          "simeon_pq_adc");
    CHECK(std::string_view(vectorSearchEngineName(VectorSearchEngine::Vec0L2)) == "vec0_l2");
    CHECK(std::string_view(vectorSearchEngineName(VectorSearchEngine::ExactScan)) == "exact_scan");

    CHECK(parseVectorSearchEngine("simeon_pq_adc") == VectorSearchEngine::SimeonPqAdc);
    CHECK(parseVectorSearchEngine("compressed_primary") == VectorSearchEngine::SimeonPqAdc);
    CHECK(parseVectorSearchEngine("hnsw") == VectorSearchEngine::SimeonPqAdc);
    CHECK(parseVectorSearchEngine("vec0_l2") == VectorSearchEngine::Vec0L2);
    CHECK(parseVectorSearchEngine("exact_scan") == VectorSearchEngine::ExactScan);
    CHECK_FALSE(parseVectorSearchEngine("unknown").has_value());
}

TEST_CASE("Embedding generator factory remains a linkable public API",
          "[vector][contract][embedding-generator]") {
    using Factory = std::unique_ptr<EmbeddingGenerator> (*)(const EmbeddingConfig&);
    Factory factory = &createEmbeddingGenerator;
    CHECK(factory != nullptr);
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorDatabase exact scan is a global vector reference",
                 "[vector][contract][exact-scan][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::ExactScan;

    VectorDatabase db(config);
    REQUIRE(db.initializeChecked().has_value());
    for (std::size_t i = 0; i < 6; ++i) {
        VectorRecord record;
        record.chunk_id = "exact_" + std::to_string(i);
        record.document_hash = "doc_" + std::to_string(i);
        record.embedding = {1.0F, static_cast<float>(i), 0.0F, 0.0F};
        REQUIRE(db.insertVectorChecked(record).has_value());
    }

    VectorSearchDiagnostics diagnostics;
    VectorSearchParams params;
    params.k = 3;
    params.similarity_threshold = -1.0F;
    params.diagnostics = &diagnostics;
    auto results = db.searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);

    REQUIRE(results.has_value());
    REQUIRE(results.value().size() == 3U);
    CHECK(results.value().front().chunk_id == "exact_0");
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.rowsVisitedObserved);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK_FALSE(diagnostics.annCandidateBudgetObserved);
    CHECK(diagnostics.rowsVisited == 6U);
    CHECK(diagnostics.exactDistanceEvaluations == 6U);
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorDatabase exact scan rejects invalid query vectors",
                 "[vector][contract][exact-scan][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::ExactScan;

    VectorDatabase db(config);
    REQUIRE(db.initializeChecked().has_value());
    VectorRecord record;
    record.chunk_id = "valid";
    record.document_hash = "doc";
    record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    REQUIRE(db.insertVectorChecked(record).has_value());

    VectorSearchParams params;
    params.k = 1;
    params.similarity_threshold = -1.0F;

    SECTION("zero norm") {
        auto result = db.searchSimilarChecked({0.0F, 0.0F, 0.0F, 0.0F}, params);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    }

    SECTION("non-finite") {
        auto result = db.searchSimilarChecked(
            {1.0F, std::numeric_limits<float>::quiet_NaN(), 0.0F, 0.0F}, params);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    }
}

TEST_CASE_METHOD(VectorSmokeFixture,
                 "VectorDatabase exact scan keeps large finite scores consistent across paths",
                 "[vector][contract][exact-scan][numerics][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::ExactScan;

    VectorDatabase db(config);
    REQUIRE(db.initializeChecked().has_value());
    const float large = std::numeric_limits<float>::max() / 4.0F;
    const std::vector<float> embedding = {large, -large, large, -large};
    VectorRecord record;
    record.chunk_id = "large_finite";
    record.document_hash = "doc_large_finite";
    record.embedding = embedding;
    record.metadata["lane"] = "large";
    REQUIRE(db.insertVectorChecked(record).has_value());

    VectorSearchParams unfiltered;
    unfiltered.k = 1;
    unfiltered.similarity_threshold = -1.0F;
    auto global = db.searchSimilarChecked(embedding, unfiltered);
    REQUIRE(global.has_value());
    REQUIRE(global.value().size() == 1U);

    VectorSearchParams filtered = unfiltered;
    filtered.metadata_filters["lane"] = "large";
    auto constrained = db.searchSimilarChecked(embedding, filtered);
    REQUIRE(constrained.has_value());
    REQUIRE(constrained.value().size() == 1U);

    CHECK(std::isfinite(global.value().front().relevance_score));
    CHECK(std::isfinite(constrained.value().front().relevance_score));
    CHECK(global.value().front().relevance_score > 0.999F);
    CHECK(constrained.value().front().relevance_score > 0.999F);
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorDatabase exact scan breaks score ties deterministically",
                 "[vector][contract][exact-scan][catch2]") {
    skipIfNeeded();

    const auto searchAfterInsertion = [](const std::vector<std::string>& chunkIds,
                                         bool useMetadataFilter) {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = 4;
        config.use_in_memory = true;
        config.search_engine = VectorSearchEngine::ExactScan;

        VectorDatabase db(config);
        REQUIRE(db.initializeChecked().has_value());
        for (const auto& chunkId : chunkIds) {
            VectorRecord record;
            record.chunk_id = chunkId;
            record.document_hash = "doc_" + chunkId;
            record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
            record.metadata["lane"] = "tie";
            REQUIRE(db.insertVectorChecked(record).has_value());
        }

        VectorSearchParams params;
        params.k = 2;
        params.similarity_threshold = -1.0F;
        if (useMetadataFilter) {
            params.metadata_filters["lane"] = "tie";
        }
        auto result = db.searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);
        REQUIRE(result.has_value());

        std::vector<std::string> resultIds;
        for (const auto& record : result.value()) {
            resultIds.push_back(record.chunk_id);
        }
        return resultIds;
    };

    const auto first = searchAfterInsertion({"tie_c", "tie_a", "tie_b"}, false);
    const auto second = searchAfterInsertion({"tie_b", "tie_a", "tie_c"}, false);
    CHECK(first == std::vector<std::string>{"tie_a", "tie_b"});
    CHECK(second == first);

    const auto filteredFirst = searchAfterInsertion({"tie_c", "tie_a", "tie_b"}, true);
    const auto filteredSecond = searchAfterInsertion({"tie_b", "tie_a", "tie_c"}, true);
    CHECK(filteredFirst == std::vector<std::string>{"tie_a", "tie_b"});
    CHECK(filteredSecond == filteredFirst);
}

TEST_CASE_METHOD(VectorSmokeFixture,
                 "VectorDatabase exact candidate mode scores only allowed documents",
                 "[vector][contract][exact-candidates][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initializeChecked().has_value());

    const auto insert = [&](std::string chunkId, std::string documentHash,
                            std::vector<float> embedding) {
        VectorRecord record;
        record.chunk_id = std::move(chunkId);
        record.document_hash = std::move(documentHash);
        record.embedding = std::move(embedding);
        return db.insertVectorChecked(record);
    };

    REQUIRE(insert("allowed_best", "allowed", {1.0F, 0.0F, 0.0F, 0.0F}).has_value());
    REQUIRE(insert("allowed_second", "allowed", {0.8F, 0.6F, 0.0F, 0.0F}).has_value());
    REQUIRE(insert("blocked", "blocked", {1.0F, 0.0F, 0.0F, 0.0F}).has_value());

    VectorSearchDiagnostics diagnostics;
    VectorSearchParams params;
    params.k = 4;
    params.similarity_threshold = -1.0F;
    params.candidate_hashes = {"allowed"};
    params.candidate_filter_mode = CandidateFilterMode::Exact;
    params.diagnostics = &diagnostics;

    auto results = db.searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);
    REQUIRE(results.has_value());
    REQUIRE(results.value().size() == 2U);
    CHECK(results.value().front().chunk_id == "allowed_best");
    CHECK(results.value().at(1).chunk_id == "allowed_second");
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.rowsVisitedObserved);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK(diagnostics.rowsVisited == 2U);
    CHECK(diagnostics.exactDistanceEvaluations == 2U);
    CHECK(diagnostics.returnedRows == 2U);
}

TEST_CASE_METHOD(VectorSmokeFixture,
                 "VectorDatabase deletion invalidates a built Simeon PQ snapshot",
                 "[vector][contract][delete][spq][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 2;
    config.simeon_pq_centroids = 4;
    config.simeon_pq_train_limit = 16;

    VectorDatabase db(config);
    REQUIRE(db.initializeChecked().has_value());
    for (std::size_t i = 0; i < 12; ++i) {
        VectorRecord record;
        record.chunk_id = "delete_" + std::to_string(i);
        record.document_hash = "doc_" + std::to_string(i);
        record.embedding = {1.0F, static_cast<float>(i) / 12.0F, 0.0F, 0.0F};
        REQUIRE(db.insertVectorChecked(record).has_value());
    }
    REQUIRE(db.buildIndexChecked().has_value());

    VectorSearchParams params;
    params.k = 4;
    params.similarity_threshold = -1.0F;
    auto before = db.searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);
    REQUIRE(before.has_value());
    REQUIRE_FALSE(before.value().empty());
    const auto deletedChunk = before.value().front().chunk_id;

    REQUIRE(db.deleteVectorChecked(deletedChunk).has_value());
    VectorSearchDiagnostics diagnostics;
    params.diagnostics = &diagnostics;
    auto after = db.searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);
    REQUIRE(after.has_value());
    CHECK(diagnostics.usedAnn);
    CHECK(std::ranges::none_of(after.value(), [&](const VectorRecord& record) {
        return record.chunk_id == deletedChunk;
    }));
    CHECK_FALSE(db.getVector(deletedChunk).has_value());
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke get vector count", "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    size_t count = db.getVectorCount();
    CHECK(count == 0);

    // Add a vector
    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "test_hash_002";
    rec.document_hash = "test_doc_002";
    rec.embedding = embedding;
    rec.content = "Test content";
    rec.start_offset = 0;
    rec.end_offset = 12;
    REQUIRE(db.insertVector(rec));

    count = db.getVectorCount();
    CHECK(count == 1);
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke operations before initialize return false",
                 "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);

    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "pre_init";
    rec.document_hash = "pre_init_doc";
    rec.embedding = embedding;
    rec.content = "pre-init";
    rec.start_offset = 0;
    rec.end_offset = 7;

    CHECK_FALSE(db.isInitialized());
    CHECK_FALSE(db.insertVector(rec));
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke operations after close return false",
                 "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "post_close";
    rec.document_hash = "post_close_doc";
    rec.embedding = embedding;
    rec.content = "post-close";
    rec.start_offset = 0;
    rec.end_offset = 9;

    REQUIRE(db.insertVector(rec));

    db.close();

    CHECK_FALSE(db.isInitialized());
    CHECK_FALSE(db.insertVector(rec));
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke insert batch and delete",
                 "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> emb1 = {1.0f, 0.0f, 0.0f, 0.0f};
    std::vector<float> emb2 = {0.0f, 1.0f, 0.0f, 0.0f};
    std::vector<VectorRecord> batch;
    VectorRecord r1;
    r1.chunk_id = "batch_1";
    r1.document_hash = "doc_batch";
    r1.embedding = emb1;
    r1.content = "batch 1";
    r1.start_offset = 0;
    r1.end_offset = 7;
    batch.push_back(r1);

    VectorRecord r2;
    r2.chunk_id = "batch_2";
    r2.document_hash = "doc_batch";
    r2.embedding = emb2;
    r2.content = "batch 2";
    r2.start_offset = 0;
    r2.end_offset = 7;
    batch.push_back(r2);

    REQUIRE(db.insertVectorsBatch(batch));
    CHECK(db.getVectorCount() == 2);

    auto got = db.getVector("batch_1");
    CHECK(got.has_value());
    CHECK(got.value().chunk_id == "batch_1");

    REQUIRE(db.deleteVector("batch_1"));
    CHECK(db.getVectorCount() == 1);

    REQUIRE(db.deleteVectorsByDocument("doc_batch"));
    CHECK(db.getVectorCount() == 0);
}

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke update vector", "[vector][smoke][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> emb = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "to_update";
    rec.document_hash = "doc_up";
    rec.embedding = emb;
    rec.content = "original";
    rec.start_offset = 0;
    rec.end_offset = 8;
    REQUIRE(db.insertVector(rec));

    VectorRecord updated;
    updated.chunk_id = "to_update";
    updated.document_hash = "doc_up";
    updated.embedding = {0.0f, 1.0f, 0.0f, 0.0f};
    updated.content = "updated content";
    updated.start_offset = 0;
    updated.end_offset = 14;
    REQUIRE(db.updateVector("to_update", updated));

    auto got = db.getVector("to_update");
    REQUIRE(got.has_value());
    CHECK(got.value().content == "updated content");
}

TEST_CASE_METHOD(VectorSmokeFixture,
                 "VectorSmoke lifecycle helpers update versioned and stale embeddings",
                 "[vector][smoke][lifecycle][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    VectorRecord rec;
    rec.chunk_id = "lifecycle_1";
    rec.document_hash = "doc_lifecycle";
    rec.embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    rec.content = "original lifecycle content";
    rec.start_offset = 0;
    rec.end_offset = rec.content.size();
    rec.model_id = "model-alpha";
    rec.model_version = "v1";
    rec.embedding_version = 1;
    rec.embedded_at = std::chrono::system_clock::now() - std::chrono::hours(2);
    REQUIRE(db.insertVector(rec));

    VectorRecord updated = rec;
    updated.embedding = {0.0f, 1.0f, 0.0f, 0.0f};
    updated.content = "updated lifecycle content";
    updated.end_offset = updated.content.size();
    updated.model_version = "v2";
    updated.embedding_version = 2;
    updated.embedded_at = std::chrono::system_clock::now();

    auto updateResult = db.updateEmbeddings({updated});
    REQUIRE(updateResult.has_value());

    auto updatedRecord = db.getVector("lifecycle_1");
    REQUIRE(updatedRecord.has_value());
    CHECK(updatedRecord.value().model_version == "v2");
    CHECK(updatedRecord.value().embedding_version == 2);
    CHECK(updatedRecord.value().content == "updated lifecycle content");

    auto byVersion = db.getEmbeddingsByVersion("v2", 10);
    REQUIRE(byVersion.has_value());
    REQUIRE(byVersion.value().size() == 1);
    CHECK(byVersion.value().front().chunk_id == "lifecycle_1");
    CHECK(byVersion.value().front().embedding_version == 2);
    CHECK(byVersion.value().front().content == "updated lifecycle content");

    auto staleBefore = db.getStaleEmbeddings("model-alpha", "v2");
    REQUIRE(staleBefore.has_value());
    CHECK(staleBefore.value().empty());

    auto staleMark = db.markAsStale("lifecycle_1");
    REQUIRE(staleMark.has_value());

    auto staleAfter = db.getStaleEmbeddings("model-alpha", "v2");
    REQUIRE(staleAfter.has_value());
    REQUIRE(staleAfter.value().size() == 1);
    CHECK(staleAfter.value().front() == "lifecycle_1");

    auto got = db.getVector("lifecycle_1");
    REQUIRE(got.has_value());
    CHECK(got.value().is_stale);
    CHECK(got.value().model_version == "v2");
}

TEST_CASE_METHOD(VectorSmokeFixture,
                 "VectorSmoke soft delete helpers fail explicitly until schema supports them",
                 "[vector][smoke][lifecycle][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    VectorRecord rec;
    rec.chunk_id = "delete_lifecycle_1";
    rec.document_hash = "doc_delete_lifecycle";
    rec.embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    rec.content = "delete lifecycle content";
    rec.start_offset = 0;
    rec.end_offset = rec.content.size();
    REQUIRE(db.insertVector(rec));

    auto softDelete = db.markAsDeleted("delete_lifecycle_1");
    REQUIRE_FALSE(softDelete.has_value());
    CHECK(softDelete.error().code == yams::ErrorCode::NotSupported);

    auto purgeDeleted = db.purgeDeleted(std::chrono::hours(24));
    REQUIRE_FALSE(purgeDeleted.has_value());
    CHECK(purgeDeleted.error().code == yams::ErrorCode::NotSupported);
}

// NOLINTEND(bugprone-chained-comparison)

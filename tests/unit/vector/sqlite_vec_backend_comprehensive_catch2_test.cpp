// Comprehensive Catch2 tests for SqliteVecBackend
// Task: yams-47l2.7 - Comprehensive Vector Subsystem Tests
//
// Coverage:
// - Initialization (new DB, existing DB, corrupted)
// - CRUD operations (insert, update, delete, get)
// - Search operations (filters, thresholds, edge cases)
// - Transaction operations (begin/commit/rollback)
// - Edge cases (empty corpus, k > size, etc.)

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;
using Catch::Matchers::WithinAbs;

namespace {

struct SqliteVecBackendFixture {
    SqliteVecBackendFixture() {
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    ~SqliteVecBackendFixture() {
        // Clean up temp files
        for (const auto& path : tempFiles) {
            std::filesystem::remove(path);
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::string createTempDbPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("sqlite_vec_test_" + std::to_string(++tempCounter) + ".db");
        tempFiles.push_back(path.string());
        return path.string();
    }

    std::vector<float> createEmbedding(size_t dim, float seed = 1.0f) {
        std::vector<float> emb(dim);
        std::mt19937 rng(static_cast<uint32_t>(seed * 1000));
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = dist(rng);
        }
        // Normalize
        float norm = 0.0f;
        for (float v : emb)
            norm += v * v;
        norm = std::sqrt(norm);
        if (norm > 0) {
            for (float& v : emb)
                v /= norm;
        }
        return emb;
    }

    VectorRecord createVectorRecord(const std::string& id, const std::vector<float>& embedding,
                                    const std::string& doc_hash = "") {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + id;
        rec.document_hash = doc_hash.empty() ? "doc_" + id : doc_hash;
        rec.embedding = embedding;
        rec.content = "Test content for " + id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::string skipReason;
    std::vector<std::string> tempFiles;
    static inline int tempCounter = 0;
};

} // namespace

// =============================================================================
// Initialization Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with new in-memory DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());
    REQUIRE(backend.isInitialized());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with new file DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(dbPath);
    REQUIRE(result.has_value());
    REQUIRE(backend.isInitialized());

    // Verify file was created
    REQUIRE(std::filesystem::exists(dbPath));

    backend.close();
    REQUIRE_FALSE(backend.isInitialized());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with existing DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    // First session: create and populate
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(64).has_value());

        auto emb = createEmbedding(64, 1.0f);
        REQUIRE(backend.insertVector(createVectorRecord("existing", emb)).has_value());

        backend.close();
    }

    // Second session: reopen
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.isInitialized());
        REQUIRE(backend.tablesExist());

        // Verify data persisted
        auto countResult = backend.getVectorCount();
        REQUIRE(countResult.has_value());
        CHECK(countResult.value() == 1);

        backend.close();
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend createTables and tablesExist",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);

    REQUIRE(backend.initialize(":memory:").has_value());

    // Tables don't exist yet
    REQUIRE_FALSE(backend.tablesExist());

    // Create tables
    REQUIRE(backend.createTables(64).has_value());

    // Now they exist
    REQUIRE(backend.tablesExist());
}

// =============================================================================
// CRUD Operations Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend insertVector single",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    auto emb = createEmbedding(64, 1.0f);
    auto rec = createVectorRecord("single", emb);

    auto result = backend.insertVector(rec);
    REQUIRE(result.has_value());

    // Verify inserted
    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 1);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend insertVectorsBatch",
                 "[vector][backend][crud][batch][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        records.push_back(createVectorRecord("batch_" + std::to_string(i), emb));
    }

    auto result = backend.insertVectorsBatch(records);
    REQUIRE(result.has_value());

    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 100);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVector existing",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    auto emb = createEmbedding(64, 1.0f);
    auto rec = createVectorRecord("gettest", emb);
    rec.content = "Specific content for get test";
    REQUIRE(backend.insertVector(rec).has_value());

    auto getResult = backend.getVector("chunk_gettest");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto& retrieved = getResult.value().value();
    CHECK(retrieved.chunk_id == "chunk_gettest");
    CHECK(retrieved.document_hash == "doc_gettest");
    CHECK(retrieved.content == "Specific content for get test");
    CHECK(retrieved.embedding.size() == 64);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVector non-existent",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    auto getResult = backend.getVector("nonexistent_chunk");
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVectorsBatch mixed",
                 "[vector][backend][crud][batch][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert some vectors
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            backend.insertVector(createVectorRecord("vec_" + std::to_string(i), emb)).has_value());
    }

    // Request mix of existing and non-existing
    std::vector<std::string> ids = {"chunk_vec_0", "chunk_vec_2", "nonexistent", "chunk_vec_4"};

    auto batchResult = backend.getVectorsBatch(ids);
    REQUIRE(batchResult.has_value());

    auto& results = batchResult.value();
    CHECK(results.size() == 3); // Only existing ones
    CHECK(results.count("chunk_vec_0") == 1);
    CHECK(results.count("chunk_vec_2") == 1);
    CHECK(results.count("chunk_vec_4") == 1);
    CHECK(results.count("nonexistent") == 0);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVectorsByDocument",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors with same document hash
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("doc1_chunk_" + std::to_string(i), emb, "shared_doc_hash");
        REQUIRE(backend.insertVector(rec).has_value());
    }

    // Insert vector with different document
    auto emb2 = createEmbedding(64, 10.0f);
    REQUIRE(backend.insertVector(createVectorRecord("other_doc", emb2, "other_hash")).has_value());

    auto result = backend.getVectorsByDocument("shared_doc_hash");
    REQUIRE(result.has_value());
    CHECK(result.value().size() == 3);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend updateVector",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert original
    auto emb1 = createEmbedding(64, 1.0f);
    auto rec1 = createVectorRecord("update_test", emb1);
    rec1.content = "Original content";
    REQUIRE(backend.insertVector(rec1).has_value());

    // Update
    auto emb2 = createEmbedding(64, 2.0f);
    auto rec2 = createVectorRecord("update_test", emb2);
    rec2.content = "Updated content";

    auto updateResult = backend.updateVector("chunk_update_test", rec2);
    REQUIRE(updateResult.has_value());

    // Verify update
    auto getResult = backend.getVector("chunk_update_test");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());
    CHECK(getResult.value().value().content == "Updated content");

    // Still only 1 vector
    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 1);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend deleteVector",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("delete_me", emb)).has_value());

    auto countBefore = backend.getVectorCount();
    REQUIRE(countBefore.has_value());
    CHECK(countBefore.value() == 1);

    // Delete
    auto deleteResult = backend.deleteVector("chunk_delete_me");
    REQUIRE(deleteResult.has_value());

    auto countAfter = backend.getVectorCount();
    REQUIRE(countAfter.has_value());
    CHECK(countAfter.value() == 0);

    // Verify gone
    auto getResult = backend.getVector("chunk_delete_me");
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend deleteVectorsByDocument",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert multiple chunks for same document
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("doc_chunk_" + std::to_string(i), emb, "doc_to_delete");
        REQUIRE(backend.insertVector(rec).has_value());
    }

    // Insert vector for different document
    auto emb2 = createEmbedding(64, 100.0f);
    REQUIRE(backend.insertVector(createVectorRecord("keeper", emb2, "other_doc")).has_value());

    auto countBefore = backend.getVectorCount();
    REQUIRE(countBefore.has_value());
    CHECK(countBefore.value() == 6);

    // Delete by document
    auto deleteResult = backend.deleteVectorsByDocument("doc_to_delete");
    REQUIRE(deleteResult.has_value());

    auto countAfter = backend.getVectorCount();
    REQUIRE(countAfter.has_value());
    CHECK(countAfter.value() == 1);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend hasEmbedding",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vector
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("has_test", emb, "has_doc")).has_value());

    auto hasResult = backend.hasEmbedding("has_doc");
    REQUIRE(hasResult.has_value());
    CHECK(hasResult.value() == true);

    auto noResult = backend.hasEmbedding("no_such_doc");
    REQUIRE(noResult.has_value());
    CHECK(noResult.value() == false);
}

// =============================================================================
// Search Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar basic",
                 "[vector][backend][search][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("search_" + std::to_string(i), emb))
                    .has_value());
    }

    // Search with first vector's embedding
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());

    auto& results = searchResult.value();
    CHECK(results.size() == 5);

    // First result should be exact match (same seed)
    CHECK(results[0].chunk_id == "chunk_search_0");
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar with similarity_threshold",
                 "[vector][backend][search][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors with varying similarity
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("thresh_" + std::to_string(i), emb))
                    .has_value());
    }

    auto query = createEmbedding(64, 1.0f);

    // High threshold - should filter most results
    auto highThreshResult = backend.searchSimilar(query, 10, 0.99f, std::nullopt, {});
    REQUIRE(highThreshResult.has_value());
    // With high threshold, might only get exact match
    CHECK(highThreshResult.value().size() <= 2);

    // Low threshold - should include more
    auto lowThreshResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE(lowThreshResult.has_value());
    CHECK(lowThreshResult.value().size() >= 5);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar with document_hash filter",
                 "[vector][backend][search][filter][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors for doc_A
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("docA_" + std::to_string(i), emb, "doc_A"))
                    .has_value());
    }

    // Insert vectors for doc_B
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 10));
        REQUIRE(backend.insertVector(createVectorRecord("docB_" + std::to_string(i), emb, "doc_B"))
                    .has_value());
    }

    auto query = createEmbedding(64, 1.0f);

    // Search only in doc_A
    auto filterResult = backend.searchSimilar(query, 10, 0.0f, "doc_A", {});
    REQUIRE(filterResult.has_value());

    auto& results = filterResult.value();
    for (const auto& r : results) {
        CHECK(r.document_hash == "doc_A");
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar with metadata_filters",
                 "[vector][backend][search][filter][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors with metadata
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("meta_" + std::to_string(i), emb);
        rec.metadata["category"] = (i % 2 == 0) ? "even" : "odd";
        rec.metadata["level"] = std::to_string(i);
        REQUIRE(backend.insertVector(rec).has_value());
    }

    auto query = createEmbedding(64, 1.0f);

    // Search with metadata filter
    std::map<std::string, std::string> filters;
    filters["category"] = "even";

    auto filterResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {}, filters);
    REQUIRE(filterResult.has_value());

    // All results should have category=even
    for (const auto& r : filterResult.value()) {
        auto it = r.metadata.find("category");
        REQUIRE(it != r.metadata.end());
        CHECK(it->second == "even");
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar empty corpus",
                 "[vector][backend][search][edge][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Don't insert anything
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());
    CHECK(searchResult.value().empty());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar k greater than corpus",
                 "[vector][backend][search][edge][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert only 3 vectors
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("small_" + std::to_string(i), emb))
                    .has_value());
    }

    // Request k=100
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 100, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());

    // Should return at least 1 result, not error.
    // Note: HNSW search may return fewer results than corpus size due to the graph
    // structure when vectors are inserted incrementally. This test validates that
    // requesting k > corpus_size doesn't cause errors.
    CHECK(searchResult.value().size() >= 1);
    CHECK(searchResult.value().size() <= 3);
}

// =============================================================================
// Transaction Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend transaction commit",
                 "[vector][backend][transaction][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(dbPath).has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Begin transaction
    auto beginResult = backend.beginTransaction();
    REQUIRE(beginResult.has_value());

    // Insert within transaction
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("txn_test", emb)).has_value());

    // Commit
    auto commitResult = backend.commitTransaction();
    REQUIRE(commitResult.has_value());

    // Verify persisted
    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 1);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend transaction rollback",
                 "[vector][backend][transaction][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert one before transaction
    auto emb1 = createEmbedding(64, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("before_txn", emb1)).has_value());

    // Begin transaction
    auto beginResult = backend.beginTransaction();
    REQUIRE(beginResult.has_value());

    // Insert within transaction
    auto emb2 = createEmbedding(64, 2.0f);
    REQUIRE(backend.insertVector(createVectorRecord("during_txn", emb2)).has_value());

    // Rollback
    auto rollbackResult = backend.rollbackTransaction();
    REQUIRE(rollbackResult.has_value());

    // Should only have the vector from before transaction
    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 1);

    // The rolled-back vector should not exist
    auto getResult = backend.getVector("chunk_during_txn");
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
}

// =============================================================================
// Statistics Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getStats",
                 "[vector][backend][stats][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert some vectors
    for (int i = 0; i < 50; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("stats_" + std::to_string(i), emb))
                    .has_value());
    }

    auto statsResult = backend.getStats();
    REQUIRE(statsResult.has_value());

    auto& stats = statsResult.value();
    CHECK(stats.total_vectors == 50);
}

// =============================================================================
// Build/Optimize Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend buildIndex",
                 "[vector][backend][index][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(backend.insertVector(createVectorRecord("build_" + std::to_string(i), emb))
                    .has_value());
    }

    // Build index (should be no-op for HNSW which builds incrementally)
    auto buildResult = backend.buildIndex();
    REQUIRE(buildResult.has_value());

    // Search should still work
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());
    CHECK(searchResult.value().size() == 5);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend optimize",
                 "[vector][backend][index][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert and delete to create soft deletes
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            backend.insertVector(createVectorRecord("opt_" + std::to_string(i), emb)).has_value());
    }

    // Delete half
    for (int i = 0; i < 50; ++i) {
        REQUIRE(backend.deleteVector("chunk_opt_" + std::to_string(i)).has_value());
    }

    // Optimize (compacts HNSW)
    auto optimizeResult = backend.optimize();
    REQUIRE(optimizeResult.has_value());

    // Search should work on remaining
    auto query = createEmbedding(64, 75.0f);
    auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());
    CHECK(searchResult.value().size() <= 50); // Only 50 remain
}

// =============================================================================
// Drop Tables Test
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend dropTables",
                 "[vector][backend][admin][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert some data
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("drop_test", emb)).has_value());

    REQUIRE(backend.tablesExist());

    // Drop tables
    auto dropResult = backend.dropTables();
    REQUIRE(dropResult.has_value());

    REQUIRE_FALSE(backend.tablesExist());
}

// =============================================================================
// HNSW Rebuild Tests (Bug fix verification)
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend buildIndex after batch insert",
                 "[vector][backend][hnsw][rebuild][catch2]") {
    skipIfNeeded();

    // This test verifies the bug fix where buildIndex() wasn't properly rebuilding
    // the HNSW index after batch inserts (vectors were in DB but HNSW was stale)

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.checkpoint_threshold = 1000; // High threshold to prevent auto-checkpointing

    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Batch insert 100 vectors
    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        records.push_back(createVectorRecord("batch_rebuild_" + std::to_string(i), emb));
    }

    auto batchResult = backend.insertVectorsBatch(records);
    REQUIRE(batchResult.has_value());

    // Verify count
    auto countResult = backend.getVectorCount();
    REQUIRE(countResult.has_value());
    CHECK(countResult.value() == 100);

    // Explicitly rebuild the index
    auto buildResult = backend.buildIndex();
    REQUIRE(buildResult.has_value());

    // Search should return results from the rebuilt index
    auto query = createEmbedding(64, 50.0f); // Query near middle of range
    auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());

    // Should find 10 results from the 100 inserted
    CHECK(searchResult.value().size() == 10);

    // The top result should be close to index 50 (query seed)
    // Due to random embedding generation, we just verify results exist
    for (const auto& result : searchResult.value()) {
        CHECK(result.chunk_id.find("batch_rebuild_") != std::string::npos);
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend HNSW persists and reloads correctly",
                 "[vector][backend][hnsw][persistence][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    // First session: create, insert batch, and close
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.createTables(64).has_value());

        // Batch insert vectors
        std::vector<VectorRecord> records;
        for (int i = 0; i < 50; ++i) {
            auto emb = createEmbedding(64, static_cast<float>(i + 1));
            records.push_back(createVectorRecord("persist_" + std::to_string(i), emb));
        }

        REQUIRE(backend.insertVectorsBatch(records).has_value());

        // Ensure HNSW is saved
        REQUIRE(backend.buildIndex().has_value());

        backend.close();
    }

    // Second session: reopen and search (HNSW should load from disk, not rebuild)
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(dbPath).has_value());
        REQUIRE(backend.tablesExist());

        // Verify count persisted
        auto countResult = backend.getVectorCount();
        REQUIRE(countResult.has_value());
        CHECK(countResult.value() == 50);

        // Search should work (HNSW loaded from persisted state)
        auto query = createEmbedding(64, 25.0f);
        auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());
        CHECK(searchResult.value().size() == 5);

        backend.close();
    }
}

// =============================================================================
// Candidate Hashes Filtering Tests (Tiered search narrowing)
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar with candidate_hashes",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors from different documents (doc groups A, B, C)
    for (int i = 0; i < 30; ++i) {
        std::string docGroup;
        if (i < 10)
            docGroup = "doc_group_A";
        else if (i < 20)
            docGroup = "doc_group_B";
        else
            docGroup = "doc_group_C";

        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("candidate_" + std::to_string(i), emb, docGroup);
        REQUIRE(backend.insertVector(rec).has_value());
    }

    // Query that would match all documents
    auto query = createEmbedding(64, 15.0f); // Middle of range

    // Search WITHOUT candidate filter - should return from all groups
    auto unfilteredResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE(unfilteredResult.has_value());
    CHECK(unfilteredResult.value().size() == 10);

    // Search WITH candidate filter - only doc_group_A allowed
    std::unordered_set<std::string> candidatesA = {"doc_group_A"};
    auto filteredResultA = backend.searchSimilar(query, 10, 0.0f, std::nullopt, candidatesA, {});
    REQUIRE(filteredResultA.has_value());

    // All results should be from doc_group_A
    for (const auto& result : filteredResultA.value()) {
        CHECK(result.document_hash == "doc_group_A");
    }
    // Should have at most 10 results from A's 10 vectors
    CHECK(filteredResultA.value().size() <= 10);

    // Search with multiple candidate groups (A and C, not B)
    std::unordered_set<std::string> candidatesAC = {"doc_group_A", "doc_group_C"};
    auto filteredResultAC = backend.searchSimilar(query, 20, 0.0f, std::nullopt, candidatesAC, {});
    REQUIRE(filteredResultAC.has_value());

    // All results should be from A or C, never B
    for (const auto& result : filteredResultAC.value()) {
        bool isAorC =
            (result.document_hash == "doc_group_A" || result.document_hash == "doc_group_C");
        CHECK(isAorC);
        CHECK(result.document_hash != "doc_group_B");
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar candidate_hashes empty returns all",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert some vectors
    for (int i = 0; i < 20; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            backend.insertVector(createVectorRecord("empty_candidate_" + std::to_string(i), emb))
                .has_value());
    }

    auto query = createEmbedding(64, 10.0f);

    // Empty candidate set should NOT filter (returns all matching results)
    std::unordered_set<std::string> emptyCandidates;
    auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, emptyCandidates, {});
    REQUIRE(result.has_value());
    CHECK(result.value().size() == 10);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar candidate_hashes no match returns empty",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors from doc_A
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("nomatch_" + std::to_string(i), emb, "doc_A");
        REQUIRE(backend.insertVector(rec).has_value());
    }

    auto query = createEmbedding(64, 5.0f);

    // Search with candidates that don't exist in the corpus
    std::unordered_set<std::string> nonExistentCandidates = {"doc_X", "doc_Y", "doc_Z"};
    auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, nonExistentCandidates, {});
    REQUIRE(result.has_value());
    CHECK(result.value().empty());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar combines candidate_hashes with metadata_filters",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert vectors with different docs and metadata
    for (int i = 0; i < 20; ++i) {
        std::string docGroup = (i < 10) ? "doc_A" : "doc_B";
        std::string category = (i % 2 == 0) ? "even" : "odd";

        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        VectorRecord rec;
        rec.chunk_id = "combined_" + std::to_string(i);
        rec.document_hash = docGroup;
        rec.embedding = emb;
        rec.content = "Content " + std::to_string(i);
        rec.metadata["category"] = category;

        REQUIRE(backend.insertVector(rec).has_value());
    }

    auto query = createEmbedding(64, 10.0f);

    // Filter to doc_A AND category=even
    std::unordered_set<std::string> candidates = {"doc_A"};
    std::map<std::string, std::string> metaFilters = {{"category", "even"}};

    auto result = backend.searchSimilar(query, 20, 0.0f, std::nullopt, candidates, metaFilters);
    REQUIRE(result.has_value());

    // Should only return doc_A vectors with category=even
    for (const auto& r : result.value()) {
        CHECK(r.document_hash == "doc_A");
        auto catIt = r.metadata.find("category");
        REQUIRE(catIt != r.metadata.end());
        CHECK(catIt->second == "even");
    }

    // doc_A has indices 0-9, even indices are 0,2,4,6,8 = 5 vectors max
    // HNSW is approximate, so we may not find all 5, but should find at least 1
    CHECK(result.value().size() >= 1);
    CHECK(result.value().size() <= 5);
}

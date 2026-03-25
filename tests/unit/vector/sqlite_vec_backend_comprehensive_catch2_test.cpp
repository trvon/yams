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

#include <sqlite3.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_schema_migration.h>
#include <yams/vector/turboquant.h>
#include <yams/vector/vector_index_manager.h>

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
    auto searchResult = backend.searchSimilar(query, 5, -2.0f, std::nullopt, {});
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

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend search falls back before initial HNSW seed build",
                 "[vector][backend][hnsw][fallback][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.checkpoint_threshold = 1000;

    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(dbPath).has_value());
    REQUIRE(backend.createTables(64).has_value());

    std::vector<VectorRecord> records;
    for (int i = 0; i < 32; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        records.push_back(createVectorRecord("fallback_" + std::to_string(i), emb));
    }

    REQUIRE(backend.insertVectorsBatch(records).has_value());
    CHECK(backend.testingLastHnswMaintenanceMode() == SqliteVecBackend::HnswMaintenanceMode::None);

    auto query = createEmbedding(64, 12.0f);
    auto searchResult = backend.searchSimilar(query, 5, -2.0f, std::nullopt, {});
    REQUIRE(searchResult.has_value());
    CHECK(searchResult.value().size() == 5);
    CHECK(backend.testingLastHnswMaintenanceMode() ==
          SqliteVecBackend::HnswMaintenanceMode::BruteForceFallback);

    REQUIRE(backend.buildIndex().has_value());
    CHECK(backend.testingLastHnswMaintenanceMode() ==
          SqliteVecBackend::HnswMaintenanceMode::FullRebuild);

    auto hnswSearchResult = backend.searchSimilar(query, 5, -2.0f, std::nullopt, {});
    REQUIRE(hnswSearchResult.has_value());
    CHECK(hnswSearchResult.value().size() == 5);
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

// =============================================================================
// HNSW Optimization Tests (beads-001, beads-003, beads-005)
// Tests for Config::for_corpus(), adaptive ef_search, and zero-norm validation
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend Config::for_corpus auto-tuning produces working index",
                 "[vector][backend][hnsw][autotuning][catch2]") {
    skipIfNeeded();

    // Test that the auto-tuned HNSW parameters (via Config::for_corpus()) produce
    // a working index with good recall across different dimension sizes.
    // For 384d embeddings, Config::for_corpus() should use M=24 instead of M=16.

    SECTION("64-dimensional embeddings") {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(":memory:").has_value());
        REQUIRE(backend.createTables(64).has_value());

        // Insert corpus to trigger auto-tuning
        std::vector<VectorRecord> records;
        for (int i = 0; i < 100; ++i) {
            auto emb = createEmbedding(64, static_cast<float>(i + 1));
            records.push_back(createVectorRecord("tune64_" + std::to_string(i), emb));
        }
        REQUIRE(backend.insertVectorsBatch(records).has_value());

        // Search should work with auto-tuned parameters
        auto query = createEmbedding(64, 50.0f);
        auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(result.has_value());
        CHECK(result.value().size() == 10);
    }

    SECTION("384-dimensional embeddings (higher M value)") {
        SqliteVecBackend::Config config;
        config.embedding_dim = 384;
        SqliteVecBackend backend(config);
        REQUIRE(backend.initialize(":memory:").has_value());
        REQUIRE(backend.createTables(384).has_value());

        // Insert corpus - 384d should trigger M=24 via Config::for_corpus()
        std::vector<VectorRecord> records;
        for (int i = 0; i < 100; ++i) {
            auto emb = createEmbedding(384, static_cast<float>(i + 1));
            records.push_back(createVectorRecord("tune384_" + std::to_string(i), emb));
        }
        REQUIRE(backend.insertVectorsBatch(records).has_value());

        // Search should work with higher M value
        auto query = createEmbedding(384, 50.0f);
        auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(result.has_value());
        CHECK(result.value().size() == 10);

        // Verify results are reasonable (top result should have high similarity)
        CHECK(result.value()[0].relevance_score > 0.5f);
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend adaptive ef_search scales with k",
                 "[vector][backend][hnsw][adaptive][catch2]") {
    skipIfNeeded();

    // Test that adaptive ef_search (via recommended_ef_search()) provides
    // good recall for different k values without requiring explicit configuration.

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    // Set a low default ef_search to verify adaptive increases it when needed
    config.hnsw_ef_search = 20;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    // Insert larger corpus to exercise adaptive ef_search
    std::vector<VectorRecord> records;
    for (int i = 0; i < 500; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        records.push_back(createVectorRecord("adaptive_" + std::to_string(i), emb));
    }
    REQUIRE(backend.insertVectorsBatch(records).has_value());

    auto query = createEmbedding(64, 250.0f); // Middle of corpus

    SECTION("k=5 should return 5 results") {
        auto result = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
        REQUIRE(result.has_value());
        CHECK(result.value().size() == 5);
    }

    SECTION("k=50 should return 50 results with adaptive ef_search") {
        // With k=50, recommended_ef_search() should automatically increase
        // ef_search above the default 20 to achieve target 95% recall
        auto result = backend.searchSimilar(query, 50, 0.0f, std::nullopt, {});
        REQUIRE(result.has_value());
        CHECK(result.value().size() == 50);
    }

    SECTION("k=100 should return 100 results") {
        auto result = backend.searchSimilar(query, 100, 0.0f, std::nullopt, {});
        REQUIRE(result.has_value());
        CHECK(result.value().size() == 100);
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend zero-norm vector validation",
                 "[vector][backend][hnsw][zeronorm][catch2]") {
    skipIfNeeded();

    // Test that zero-norm vectors (all zeros) are handled gracefully.
    // Zero vectors cannot participate in cosine similarity and should be
    // either rejected or skipped during HNSW indexing.

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    SqliteVecBackend backend(config);
    REQUIRE(backend.initialize(":memory:").has_value());
    REQUIRE(backend.createTables(64).has_value());

    SECTION("Zero vector is skipped in HNSW but stored in DB") {
        // Create a zero vector (all zeros)
        std::vector<float> zeroVec(64, 0.0f);
        VectorRecord zeroRec;
        zeroRec.chunk_id = "zero_vec_chunk";
        zeroRec.document_hash = "zero_doc";
        zeroRec.embedding = zeroVec;
        zeroRec.content = "Zero vector content";

        // Insert should succeed (stored in SQLite)
        auto insertResult = backend.insertVector(zeroRec);
        REQUIRE(insertResult.has_value());

        // Vector should be retrievable from DB
        auto getResult = backend.getVector("zero_vec_chunk");
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        CHECK(getResult.value().value().chunk_id == "zero_vec_chunk");

        // Add some normal vectors
        for (int i = 0; i < 10; ++i) {
            auto emb = createEmbedding(64, static_cast<float>(i + 1));
            REQUIRE(backend.insertVector(createVectorRecord("normal_" + std::to_string(i), emb))
                        .has_value());
        }

        // Search should work and not return the zero vector
        // (zero vectors become dead-ends in HNSW and shouldn't be found)
        auto query = createEmbedding(64, 5.0f);
        auto searchResult = backend.searchSimilar(query, 20, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());

        // Verify zero vector is not in search results
        for (const auto& result : searchResult.value()) {
            CHECK(result.chunk_id != "zero_vec_chunk");
        }
    }

    SECTION("Near-zero vector (negligible magnitude) is handled") {
        // Create a near-zero vector (very small values that would normalize to essentially zero)
        std::vector<float> nearZeroVec(64, 1e-20f);
        VectorRecord nearZeroRec;
        nearZeroRec.chunk_id = "near_zero_chunk";
        nearZeroRec.document_hash = "near_zero_doc";
        nearZeroRec.embedding = nearZeroVec;
        nearZeroRec.content = "Near-zero vector content";

        // Insert should succeed
        auto insertResult = backend.insertVector(nearZeroRec);
        REQUIRE(insertResult.has_value());

        // Add normal vectors
        for (int i = 0; i < 5; ++i) {
            auto emb = createEmbedding(64, static_cast<float>(i + 1));
            REQUIRE(backend.insertVector(createVectorRecord("norm_" + std::to_string(i), emb))
                        .has_value());
        }

        // Search should work without issues
        auto query = createEmbedding(64, 2.0f);
        auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());
        CHECK(searchResult.value().size() >= 1);
    }

    SECTION("Batch insert with zero vectors skips them in HNSW") {
        std::vector<VectorRecord> records;

        // Mix of normal and zero vectors
        for (int i = 0; i < 10; ++i) {
            if (i == 3 || i == 7) {
                // Insert zero vectors at indices 3 and 7
                std::vector<float> zeroVec(64, 0.0f);
                VectorRecord rec;
                rec.chunk_id = "batch_zero_" + std::to_string(i);
                rec.document_hash = "batch_doc";
                rec.embedding = zeroVec;
                rec.content = "Zero content " + std::to_string(i);
                records.push_back(rec);
            } else {
                auto emb = createEmbedding(64, static_cast<float>(i + 1));
                records.push_back(
                    createVectorRecord("batch_normal_" + std::to_string(i), emb, "batch_doc"));
            }
        }

        // Batch insert should succeed
        auto batchResult = backend.insertVectorsBatch(records);
        REQUIRE(batchResult.has_value());

        // All 10 vectors should be in DB
        auto countResult = backend.getVectorCount();
        REQUIRE(countResult.has_value());
        CHECK(countResult.value() == 10);

        // Search should find normal vectors but not zero vectors
        auto query = createEmbedding(64, 5.0f);
        auto searchResult = backend.searchSimilar(query, 20, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());

        for (const auto& result : searchResult.value()) {
            // Zero vectors at indices 3 and 7 should not appear
            CHECK(result.chunk_id != "batch_zero_3");
            CHECK(result.chunk_id != "batch_zero_7");
        }
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend V2.2 migration adds quantized columns",
                 "[sqlite_vec_backend][migration][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value()); // Creates fresh V2.2 schema

    // V2.2 schema adds quantized sidecar columns.
    auto schema_version = VectorSchemaMigration::detectVersion(backend.getDbHandle());
    CHECK(static_cast<int>(schema_version) >= 3); // At least V2.1
    CHECK(VectorSchemaMigration::hasQuantizedColumns(backend.getDbHandle()));

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(
        backend.getDbHandle(),
        "SELECT quantized_format, quantized_bits, quantized_seed FROM vectors LIMIT 1", -1, &stmt,
        nullptr);
    CHECK(rc == SQLITE_OK);
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized sidecar round-trips through insert/get",
                 "[sqlite_vec_backend][turboquant][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value()); // Creates fresh V2.2 schema

    VectorRecord rec;
    rec.chunk_id = "quantized_test_1";
    rec.document_hash = "quant_doc_1";
    rec.embedding = createEmbedding(128, 1.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 42;
    rec.quantized.packed_codes = {1, 2, 3, 4, 5, 6, 7, 8};

    auto insert_result = backend.insertVector(rec);
    REQUIRE(insert_result.has_value());

    auto get_result = backend.getVector("quantized_test_1");
    REQUIRE(get_result.has_value());
    REQUIRE(get_result.value().has_value());
    auto retrieved = get_result.value().value();
    CHECK(retrieved.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1);
    CHECK(retrieved.quantized.bits_per_channel == 4);
    CHECK(retrieved.quantized.seed == 42);
    CHECK(retrieved.quantized.packed_codes.size() == 8);
    CHECK(std::memcmp(retrieved.quantized.packed_codes.data(), rec.quantized.packed_codes.data(),
                      8) == 0);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend mixed rows: quantized and non-quantized coexist",
                 "[sqlite_vec_backend][turboquant][mixed][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Insert a non-quantized row
    VectorRecord plain_rec;
    plain_rec.chunk_id = "plain_row_1";
    plain_rec.document_hash = "plain_doc";
    plain_rec.embedding = createEmbedding(128, 1.0f);
    REQUIRE(backend.insertVector(plain_rec).has_value());

    // Insert a quantized row
    VectorRecord quant_rec;
    quant_rec.chunk_id = "quant_row_1";
    quant_rec.document_hash = "quant_doc";
    quant_rec.embedding = createEmbedding(128, 2.0f);
    quant_rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    quant_rec.quantized.bits_per_channel = 4;
    quant_rec.quantized.seed = 99;
    quant_rec.quantized.packed_codes = {10, 20, 30, 40};
    REQUIRE(backend.insertVector(quant_rec).has_value());

    // Retrieve both
    auto plain_get = backend.getVector("plain_row_1");
    REQUIRE(plain_get.has_value());
    REQUIRE(plain_get.value().has_value());
    CHECK(plain_get.value().value().quantized.packed_codes.empty());

    auto quant_get = backend.getVector("quant_row_1");
    REQUIRE(quant_get.has_value());
    REQUIRE(quant_get.value().has_value());
    CHECK(quant_get.value().value().quantized.format ==
          VectorRecord::QuantizedFormat::TURBOquant_1);
    CHECK(quant_get.value().value().quantized.packed_codes.size() == 4);

    // Batch get both at once
    std::vector<std::string> ids = {"plain_row_1", "quant_row_1"};
    auto batch_result = backend.getVectorsBatch(ids);
    REQUIRE(batch_result.has_value());
    CHECK(batch_result.value().size() == 2);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend batch insert of quantized rows",
                 "[sqlite_vec_backend][turboquant][batch][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    std::vector<VectorRecord> batch;
    for (size_t i = 0; i < 5; ++i) {
        VectorRecord rec;
        rec.chunk_id = "batch_quant_" + std::to_string(i);
        rec.document_hash = "batch_doc_" + std::to_string(i);
        rec.embedding = createEmbedding(128, static_cast<float>(i + 1));
        rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
        rec.quantized.bits_per_channel = 4;
        rec.quantized.seed = static_cast<uint64_t>(i * 11);
        rec.quantized.packed_codes = {static_cast<uint8_t>(i), static_cast<uint8_t>(i * 2),
                                      static_cast<uint8_t>(i * 3), static_cast<uint8_t>(i * 4)};
        batch.push_back(rec);
    }

    auto batch_result = backend.insertVectorsBatch(batch);
    REQUIRE(batch_result.has_value());

    // Retrieve all 5 and verify quantized data survived
    for (size_t i = 0; i < 5; ++i) {
        auto get_result = backend.getVector("batch_quant_" + std::to_string(i));
        REQUIRE(get_result.has_value());
        REQUIRE(get_result.value().has_value());
        auto& rec = get_result.value().value();
        CHECK(rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1);
        CHECK(rec.quantized.packed_codes.size() == 4);
        CHECK(rec.quantized.packed_codes[0] == static_cast<uint8_t>(i));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend update changes quantized sidecar",
                 "[sqlite_vec_backend][turboquant][update][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    VectorRecord rec;
    rec.chunk_id = "update_quant_1";
    rec.document_hash = "update_doc";
    rec.embedding = createEmbedding(128, 3.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 7;
    rec.quantized.packed_codes = {1, 2, 3, 4};
    REQUIRE(backend.insertVector(rec).has_value());

    // Update: replace quantized data
    rec.quantized.packed_codes = {9, 8, 7, 6};
    rec.quantized.seed = 88;
    auto update_result = backend.updateVector(rec.chunk_id, rec);
    REQUIRE(update_result.has_value());

    auto get_result = backend.getVector("update_quant_1");
    REQUIRE(get_result.has_value());
    REQUIRE(get_result.value().has_value());
    auto& retrieved = get_result.value().value();
    CHECK(retrieved.quantized.seed == 88);
    CHECK(retrieved.quantized.packed_codes.size() == 4);
    CHECK(retrieved.quantized.packed_codes[0] == 9);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend loads legacy V2.1 row (NULL quantized columns) safely",
                 "[sqlite_vec_backend][turboquant][migration][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Manually insert a row that simulates a pre-V2.2 insert: quantized columns as NULL.
    // This mimics a row that existed before the quantized sidecar was added.
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(
        backend.getDbHandle(),
        "INSERT INTO vectors (chunk_id, document_hash, embedding, embedding_dim, "
        "quantized_format, quantized_bits, quantized_seed, quantized_packed_codes) "
        "VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)",
        -1, &stmt, nullptr);
    REQUIRE(rc == SQLITE_OK);

    std::vector<uint8_t> embedding_bytes(128 * sizeof(float));
    std::memcpy(embedding_bytes.data(), createEmbedding(128, 99.0f).data(), 128 * sizeof(float));

    sqlite3_bind_text(stmt, 1, "legacy_row_1", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, "legacy_doc", -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt, 3, embedding_bytes.data(), static_cast<int>(embedding_bytes.size()),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, 128);
    rc = sqlite3_step(stmt);
    REQUIRE(rc == SQLITE_DONE);
    sqlite3_finalize(stmt);

    // Load it back — should not crash and should return the row with empty quantized fields
    auto get_result = backend.getVector("legacy_row_1");
    REQUIRE(get_result.has_value());
    REQUIRE(get_result.value().has_value());
    auto& retrieved = get_result.value().value();
    CHECK(retrieved.chunk_id == "legacy_row_1");
    CHECK(retrieved.quantized.packed_codes.empty());
    CHECK(retrieved.quantized.seed == 0); // Default/zero for NULL
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized-primary storage omits float blob",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    // Configure backend with quantized-primary storage mode
    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 7;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Insert a vector with both embedding and quantized data
    VectorRecord rec;
    rec.chunk_id = "qp_test_1";
    rec.document_hash = "qp_doc";
    rec.embedding = createEmbedding(128, 3.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 7;
    rec.quantized.packed_codes = {1, 2, 3, 4, 5, 6, 7, 8};
    REQUIRE(backend.insertVector(rec).has_value());

    // Verify embedding blob is NULL directly in SQLite
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(backend.getDbHandle(),
                                "SELECT embedding, quantized_format, quantized_bits, "
                                "quantized_seed, quantized_packed_codes "
                                "FROM vectors WHERE chunk_id = ?",
                                -1, &stmt, nullptr);
    REQUIRE(rc == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, "qp_test_1", -1, SQLITE_TRANSIENT);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);

    // Float blob should be NULL in quantized-primary mode
    CHECK(sqlite3_column_type(stmt, 0) == SQLITE_NULL);

    // Quantized sidecar should be populated
    CHECK(sqlite3_column_int(stmt, 1) == 1);            // TURBOquant_1
    CHECK(sqlite3_column_int(stmt, 2) == 4);            // bits
    CHECK(sqlite3_column_int64(stmt, 3) == 7);          // seed
    CHECK(sqlite3_column_type(stmt, 4) != SQLITE_NULL); // packed_codes present
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized-primary + VectorDatabase dequantizes on getVector",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 99;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Insert quantized-primary row
    VectorRecord rec;
    rec.chunk_id = "qp_vec_1";
    rec.document_hash = "qp_doc_1";
    rec.embedding = createEmbedding(128, 7.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 99;
    rec.quantized.packed_codes = {10, 20, 30, 40};
    REQUIRE(backend.insertVector(rec).has_value());

    // getVector should return the row (backend itself returns NULL blob)
    auto get_result = backend.getVector("qp_vec_1");
    REQUIRE(get_result.has_value());
    REQUIRE(get_result.value().has_value());
    auto& retrieved = get_result.value().value();
    CHECK(retrieved.chunk_id == "qp_vec_1");
    // Backend returns NULL blob → embedding is empty (VectorDatabase layer handles dequantization)
    CHECK(retrieved.embedding.empty());
    CHECK(retrieved.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1);
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend update/delete derive dimension from embedding_dim in quantized-primary mode",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 11;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Insert a quantized-primary row
    VectorRecord rec;
    rec.chunk_id = "upd_del_1";
    rec.document_hash = "doc_upd";
    rec.embedding = createEmbedding(128, 5.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 11;
    rec.quantized.packed_codes = {5, 10, 15, 20, 25, 30, 35, 40};
    REQUIRE(backend.insertVector(rec).has_value());

    // Update: pass a record with embedding for HNSW but quantized for storage
    VectorRecord upd;
    upd.chunk_id = "upd_del_1";
    upd.document_hash = "doc_upd";
    upd.embedding = createEmbedding(128, 6.0f);
    upd.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    upd.quantized.bits_per_channel = 4;
    upd.quantized.seed = 11;
    upd.quantized.packed_codes = {6, 12, 18, 24, 30, 36, 42, 48};
    REQUIRE(backend.updateVector("upd_del_1", upd).has_value());

    // Verify the row still exists (update succeeded without dimension error)
    auto get_result = backend.getVector("upd_del_1");
    REQUIRE(get_result.has_value());
    REQUIRE(get_result.value().has_value());
    CHECK(get_result.value().value().chunk_id == "upd_del_1");

    // Delete: should not crash (dimension derived from embedding_dim, not embedding.size())
    REQUIRE(backend.deleteVector("upd_del_1").has_value());

    // Verify gone
    auto gone_result = backend.getVector("upd_del_1");
    REQUIRE(gone_result.has_value());
    CHECK(!gone_result.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend queryVectorDimsUnlocked includes quantized-primary rows",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 13;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE(init_result.has_value());
    REQUIRE(backend.createTables(128).has_value());

    // Prepare a TurboQuantMSE to produce valid packed codes for dim=128, bits=4
    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = 128;
    tq_cfg.bits_per_channel = 4;
    tq_cfg.seed = 13;
    TurboQuantMSE tq(tq_cfg);

    // Insert several quantized-primary rows using real quantized data
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(128, static_cast<float>(i));
        std::vector<uint8_t> packed = vector_utils::packedQuantizeVector(emb, &tq);

        VectorRecord rec;
        rec.chunk_id = "dim_test_" + std::to_string(i);
        rec.document_hash = "doc_dim";
        rec.embedding = emb;
        rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
        rec.quantized.bits_per_channel = 4;
        rec.quantized.seed = 13;
        rec.quantized.packed_codes = packed;
        REQUIRE(backend.insertVector(rec).has_value());
    }

    // Brute-force search should find these quantized-primary rows
    // (proves queryVectorDimsUnlocked includes them in its dimension scan)
    // Use same seed as first row for exact match (similarity = 1.0)
    auto bf_result = backend.searchSimilar(createEmbedding(128, 0.0f), 5, 0.0f);
    REQUIRE(bf_result.has_value());
    auto& results = bf_result.value();
    // Should find at least the row with matching seed 0.0f
    CHECK(results.size() >= 1);
    CHECK(results[0].chunk_id == "dim_test_0");
    CHECK(results[0].relevance_score >
          0.5f); // TurboQuant causes some distortion; just verify reasonable similarity
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend quantized-primary rows survive close+reopen and remain searchable",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    std::string db_path = createTempDbPath();

    // Phase 1: write
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 128;
        config.enable_turboquant_storage = true;
        config.quantized_primary_storage = true;
        config.turboquant_bits = 4;
        config.turboquant_seed = 17;
        SqliteVecBackend writer(config);
        REQUIRE(writer.initialize(db_path).has_value());
        REQUIRE(writer.createTables(128).has_value());

        TurboQuantConfig tq_cfg;
        tq_cfg.dimension = 128;
        tq_cfg.bits_per_channel = 4;
        tq_cfg.seed = 17;
        TurboQuantMSE tq(tq_cfg);

        // Insert two rows
        for (int i = 0; i < 2; ++i) {
            auto emb = createEmbedding(128, static_cast<float>(i));
            std::vector<uint8_t> packed = vector_utils::packedQuantizeVector(emb, &tq);
            VectorRecord rec;
            rec.chunk_id = "reopen_" + std::to_string(i);
            rec.document_hash = "doc_reopen";
            rec.embedding = emb;
            rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
            rec.quantized.bits_per_channel = 4;
            rec.quantized.seed = 17;
            rec.quantized.packed_codes = packed;
            REQUIRE(writer.insertVector(rec).has_value());
        }
    } // writer closes here

    // Phase 2: reopen fresh backend on same path and verify rows survived
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 128;
        config.enable_turboquant_storage = true;
        config.quantized_primary_storage = true;
        config.turboquant_bits = 4;
        config.turboquant_seed = 17;
        SqliteVecBackend reader(config);
        REQUIRE(reader.initialize(db_path).has_value());

        // Verify both rows exist (backend getVector returns raw record; dequantization
        // is VectorDatabase's responsibility. The proof is that search finds them.)
        for (int i = 0; i < 2; ++i) {
            auto get_result = reader.getVector("reopen_" + std::to_string(i));
            REQUIRE(get_result.has_value());
            REQUIRE(get_result.value().has_value());
            const auto& rec = get_result.value().value();
            CHECK(rec.chunk_id == "reopen_" + std::to_string(i));
            CHECK(rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1);
        }

        // Phase 3: brute-force search finds the rows after reopen (dequantizes from packed codes)
        auto bf_result = reader.searchSimilar(createEmbedding(128, 0.0f), 5, 0.0f);
        REQUIRE(bf_result.has_value());
        CHECK(bf_result.value().size() >= 1);
        CHECK(bf_result.value()[0].chunk_id == "reopen_0");
    }
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend rejects quantized_primary_storage without enable_turboquant_storage",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = false;
    config.quantized_primary_storage = true; // Invalid: requires TurboQuant
    SqliteVecBackend backend(config);

    auto result = backend.initialize(createTempDbPath());
    CHECK(!result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
}

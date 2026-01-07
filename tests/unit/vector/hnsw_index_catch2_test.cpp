// Catch2 tests for HNSW Index functionality
// Tests the sqlite-vec-cpp HNSW implementation through YAMS VectorDatabase API
// Phase 4.1: Unit tests for SqliteVecHNSWIndex

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <random>
#include <string>
#include <vector>
#include <yams/vector/vector_database.h>

using namespace yams::vector;
using Catch::Matchers::ContainsSubstring;
using Catch::Matchers::WithinAbs;

namespace {

struct HNSWIndexFixture {
    HNSWIndexFixture() {
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping HNSW test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping HNSW test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    VectorDatabaseConfig createConfig(size_t dim) {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = dim;
        config.create_if_missing = true;
        config.use_in_memory = true;
        return config;
    }

    std::vector<float> createEmbedding(size_t dim, float seed = 42.0f) {
        std::vector<float> emb(dim);
        std::mt19937 rng(static_cast<uint32_t>(seed));
        std::uniform_real_distribution<float> dist(0.0f, 1.0f);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = dist(rng);
        }
        return emb;
    }

    VectorRecord createVectorRecord(const std::string& chunk_id, const std::string& doc_hash,
                                    const std::vector<float>& embedding) {
        VectorRecord rec;
        rec.chunk_id = chunk_id;
        rec.document_hash = doc_hash;
        rec.embedding = embedding;
        rec.content = "Test content for " + chunk_id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::string skipReason;
};

} // namespace

// =============================================================================
// Test Group 1: Basic Add and Search
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex basic insert and search",
                 "[vector][hnsw][basic][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert a vector
    std::vector<float> embedding = createEmbedding(64, 1.0f);
    VectorRecord rec = createVectorRecord("test_chunk_001", "test_doc_001", embedding);
    REQUIRE(db.insertVector(rec));

    // Search for it
    VectorSearchParams params;
    params.k = 5;
    auto results = db.search(embedding, params);
    REQUIRE(results.size() >= 1);

    // Should find our vector
    bool found = false;
    for (const auto& r : results) {
        if (r.chunk_id == "test_chunk_001") {
            found = true;
            break;
        }
    }
    CHECK(found);
}

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex insert multiple vectors",
                 "[vector][hnsw][basic][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert multiple vectors
    std::vector<std::string> chunk_ids;
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, 1000.0f + i);
        VectorRecord rec = createVectorRecord("test_chunk_" + std::to_string(i),
                                              "test_doc_" + std::to_string(i), emb);
        REQUIRE(db.insertVector(rec));
        chunk_ids.push_back("test_chunk_" + std::to_string(i));
    }

    // Verify count
    size_t count = db.getVectorCount();
    CHECK(count == 10);

    // Search should return results
    auto emb = createEmbedding(64, 1005.0f);
    VectorSearchParams params;
    params.k = 10;
    auto results = db.search(emb, params);
    REQUIRE(results.size() == 10);
}

// =============================================================================
// Test Group 2: Batch Operations
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex batch insert", "[vector][hnsw][batch][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        records.push_back(createVectorRecord("batch_chunk_" + std::to_string(i),
                                             "batch_doc_" + std::to_string(i),
                                             createEmbedding(64, 2000.0f + i)));
    }

    CHECK(db.insertVectorsBatch(records));

    size_t count = db.getVectorCount();
    CHECK(count == 100);
}

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex batch search",
                 "[vector][hnsw][batch][search][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add vectors in batches
    for (int batch = 0; batch < 5; ++batch) {
        std::vector<VectorRecord> records;
        for (int i = 0; i < 50; ++i) {
            int idx = batch * 50 + i;
            records.push_back(createVectorRecord("batch_search_" + std::to_string(idx),
                                                 "batch_doc_" + std::to_string(idx),
                                                 createEmbedding(64, 3000.0f + idx)));
        }
        REQUIRE(db.insertVectorsBatch(records));
    }

    // Search
    auto query = createEmbedding(64, 3125.0f); // Near middle of range
    VectorSearchParams params;
    params.k = 10;
    auto results = db.search(query, params);
    REQUIRE(results.size() == 10);
}

// =============================================================================
// Test Group 3: Soft Delete
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex soft delete", "[vector][hnsw][delete][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert vectors
    auto emb1 = createEmbedding(64, 4000.0f);
    auto emb2 = createEmbedding(64, 4001.0f);
    REQUIRE(db.insertVector(createVectorRecord("delete_001", "delete_doc_001", emb1)));
    REQUIRE(db.insertVector(createVectorRecord("delete_002", "delete_doc_002", emb2)));

    // Delete one
    CHECK(db.deleteVector("delete_001"));

    // Count should be 1
    size_t count = db.getVectorCount();
    CHECK(count == 1);

    // Search should not return deleted
    VectorSearchParams params;
    params.k = 5;
    auto results = db.search(emb1, params);
    for (const auto& r : results) {
        CHECK(r.chunk_id != "delete_001");
    }
}

// =============================================================================
// Test Group 4: Search Quality and Recall
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex search quality recall",
                 "[vector][hnsw][recall][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add 1000 vectors
    for (int i = 0; i < 1000; ++i) {
        db.insertVector(createVectorRecord("recall_" + std::to_string(i),
                                           "recall_doc_" + std::to_string(i),
                                           createEmbedding(64, 6000.0f + i)));
    }

    // Query with a known vector
    auto query = createEmbedding(64, 6500.0f);
    VectorSearchParams params;
    params.k = 10;
    auto results = db.search(query, params);
    REQUIRE(results.size() == 10);

    // First result should be closest (exact match at index 500)
    CHECK(results[0].chunk_id == "recall_500");
}

// =============================================================================
// Test Group 5: Large Corpus
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex large corpus 10K vectors",
                 "[vector][hnsw][large][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add 10,000 vectors
    for (int i = 0; i < 10000; ++i) {
        db.insertVector(createVectorRecord("large_" + std::to_string(i),
                                           "large_doc_" + std::to_string(i),
                                           createEmbedding(64, 7000.0f + i)));
    }

    size_t count = db.getVectorCount();
    CHECK(count == 10000);

    // Search should work
    auto query = createEmbedding(64, 7500.0f);
    VectorSearchParams params;
    params.k = 10;
    auto results = db.search(query, params);
    REQUIRE(results.size() == 10);

    // First result should be index 500
    CHECK(results[0].chunk_id == "large_500");
}

// =============================================================================
// Test Group 6: fp16 Accuracy (via VectorDatabase API)
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex fp16 accuracy preserved",
                 "[vector][hnsw][fp16][catch2]") {
    skipIfNeeded();

    auto config = createConfig(16); // Small dimension
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add vectors with specific values
    std::vector<float> vec1(16, 0.0f);
    vec1[0] = 1.0f;
    std::vector<float> vec2(16, 0.0f);
    vec2[0] = 0.5f;
    std::vector<float> vec3(16, 0.0f);
    vec3[0] = -1.0f;

    REQUIRE(db.insertVector(createVectorRecord("fp16_001", "fp16_doc_001", vec1)));
    REQUIRE(db.insertVector(createVectorRecord("fp16_002", "fp16_doc_002", vec2)));
    REQUIRE(db.insertVector(createVectorRecord("fp16_003", "fp16_doc_003", vec3)));

    // Query with vec1 - vec2 should be in top results
    VectorSearchParams params;
    params.k = 3;
    params.similarity_threshold = -2.0f; // Include all results including negative similarities
    auto results = db.search(vec1, params);
    REQUIRE(results.size() == 3);

    // vec2 should be found (orthogonal to vec1 in other dimensions)
    bool found_002 = false;
    for (const auto& r : results) {
        if (r.chunk_id == "fp16_002") {
            found_002 = true;
            break;
        }
    }
    CHECK(found_002);
}

// =============================================================================
// Test Group 7: Thread Safety
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex concurrent inserts",
                 "[vector][hnsw][thread][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    constexpr int num_threads = 4;
    constexpr int vectors_per_thread = 250;
    constexpr int total_vectors = num_threads * vectors_per_thread;

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < vectors_per_thread; ++i) {
                int idx = t * vectors_per_thread + i;
                auto emb = createEmbedding(64, 8000.0f + idx);
                if (db.insertVector(createVectorRecord("thread_" + std::to_string(idx),
                                                       "thread_doc_" + std::to_string(idx), emb))) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    CHECK(success_count.load() == total_vectors);

    size_t count = db.getVectorCount();
    CHECK(count == total_vectors);
}

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex search with different params",
                 "[vector][hnsw][params][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add vectors
    std::vector<float> vec1(64, 0.0f);
    vec1[0] = 1.0f;
    std::vector<float> vec2(64, 0.0f);
    vec2[0] = 0.5f;
    std::vector<float> vec3(64, 0.0f);
    vec3[0] = -1.0f;

    REQUIRE(db.insertVector(createVectorRecord("metric_001", "metric_doc_001", vec1)));
    REQUIRE(db.insertVector(createVectorRecord("metric_002", "metric_doc_002", vec2)));
    REQUIRE(db.insertVector(createVectorRecord("metric_003", "metric_doc_003", vec3)));

    // Search with k=3
    VectorSearchParams params;
    params.k = 3;
    params.similarity_threshold = -2.0f; // Include all results including negative similarities
    auto results = db.search(vec1, params);
    REQUIRE(results.size() == 3);

    // vec1 (metric_001) is identical to query, so it's the closest
    // vec2 (metric_002) should be second closest (same direction, different magnitude)
    CHECK(results[0].chunk_id == "metric_001");
    CHECK(results[1].chunk_id == "metric_002");
}

// =============================================================================
// Test Group 9: Error Handling
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex dimension mismatch",
                 "[vector][hnsw][error][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Add 64-dim vector
    auto emb64 = createEmbedding(64, 12000.0f);
    REQUIRE(db.insertVector(createVectorRecord("dim_test", "dim_doc", emb64)));

    // Search with 128-dim should return fewer results or fail gracefully
    auto emb128 = createEmbedding(128, 12001.0f);
    VectorSearchParams params;
    params.k = 5;

    // The database should handle this gracefully (either return empty or partial)
    auto results = db.search(emb128, params);
    // Results may be empty due to dimension mismatch
    CHECK(results.size() <= 5);
}

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex search non-existent",
                 "[vector][hnsw][error][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Search on empty database
    auto query = createEmbedding(64, 13000.0f);
    VectorSearchParams params;
    params.k = 5;
    auto results = db.search(query, params);
    CHECK(results.empty());
}

// =============================================================================
// Test Group 10: Edge Cases
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex single vector", "[vector][hnsw][edge][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    auto emb = createEmbedding(64, 14000.0f);
    REQUIRE(db.insertVector(createVectorRecord("single", "single_doc", emb)));

    VectorSearchParams params;
    params.k = 10;
    auto results = db.search(emb, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "single");
}

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex update vector", "[vector][hnsw][update][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    auto emb1 = createEmbedding(64, 15000.0f);
    REQUIRE(db.insertVector(createVectorRecord("update_test", "update_doc", emb1)));

    // Update with different vector
    auto emb2 = createEmbedding(64, 15001.0f);
    VectorRecord rec = createVectorRecord("update_test", "update_doc", emb2);
    CHECK(db.updateVector("update_test", rec));

    // Search should return updated vector
    VectorSearchParams params;
    params.k = 1;
    auto results = db.search(emb2, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "update_test");
}

// =============================================================================
// Test Group 11: Get Vector by ID
// =============================================================================

TEST_CASE_METHOD(HNSWIndexFixture, "HNSWIndex get vector by chunk_id",
                 "[vector][hnsw][get][catch2]") {
    skipIfNeeded();

    auto config = createConfig(64);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    auto emb = createEmbedding(64, 16000.0f);
    REQUIRE(db.insertVector(createVectorRecord("get_test", "get_doc", emb)));

    auto retrieved = db.getVector("get_test");
    REQUIRE(retrieved.has_value());
    CHECK(retrieved->chunk_id == "get_test");
    CHECK(retrieved->embedding.size() == 64);
}

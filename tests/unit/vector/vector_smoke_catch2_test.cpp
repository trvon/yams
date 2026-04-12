// Catch2 tests for VectorDatabase smoke tests
// Migrated from GTest: vector_smoke_test.cpp
// PBI-040: Minimal vector smoke test (conditional on sqlite-vec availability)

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <yams/vector/vector_database.h>

using namespace yams::vector;

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

TEST_CASE_METHOD(VectorSmokeFixture, "VectorSmoke insert and search with quantized HNSW engine",
                 "[vector][smoke][qhnsw][catch2]") {
    skipIfNeeded();

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::HnswQuantizedL2;
    config.quantized_hnsw_mode = QuantizedHnswMode::LVQ8;
    config.quantized_hnsw_rerank_factor = 2;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorRecord rec;
    rec.chunk_id = "qhnsw_hash_001";
    rec.document_hash = "qhnsw_doc_001";
    rec.embedding = embedding;
    rec.content = "Quantized HNSW test content";
    rec.start_offset = 0;
    rec.end_offset = 27;
    REQUIRE(db.insertVector(rec));
    REQUIRE(db.prepareSearchIndex());

    VectorSearchParams params;
    params.k = 1;
    auto results = db.search(embedding, params);
    REQUIRE(results.size() == 1);
    CHECK(results[0].chunk_id == "qhnsw_hash_001");
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

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

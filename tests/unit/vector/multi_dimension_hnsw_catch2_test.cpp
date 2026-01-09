// Catch2 tests for multi-dimension HNSW index support
// Tests that SqliteVecBackend can store and search vectors of different dimensions

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct MultiDimensionFixture {
    MultiDimensionFixture() {
        // Skip if vectors disabled
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping vector test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping vector test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::vector<float> createEmbedding(size_t dim, float seed = 1.0f) {
        std::vector<float> emb(dim);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = seed * static_cast<float>(i + 1) / static_cast<float>(dim);
        }
        // Normalize for cosine similarity
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

    VectorRecord createVectorRecord(const std::string& id, const std::vector<float>& embedding) {
        VectorRecord rec;
        rec.document_hash = "doc_" + id;
        rec.chunk_id = "chunk_" + id;
        rec.embedding = embedding;
        rec.content = "Test content for " + id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::string skipReason;
};

} // namespace

// =============================================================================
// Multi-Dimension Insert and Search
// =============================================================================

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW insert vectors of different dimensions",
                 "[vector][multidim][hnsw][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.hnsw_m = 16;
    config.hnsw_ef_construction = 100;
    config.hnsw_ef_search = 50;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());

    // Create tables (use 384 as initial dim, but multi-dim support allows others)
    auto createResult = backend.createTables(384);
    REQUIRE(createResult.has_value());

    SECTION("insert 384-dim and 768-dim vectors successfully") {
        // Insert 384-dim vector
        auto emb384 = createEmbedding(384, 1.0f);
        auto rec384 = createVectorRecord("vec_384", emb384);
        auto insert384 = backend.insertVector(rec384);
        if (!insert384.has_value()) {
            INFO("insert384 failed: " << insert384.error().message);
        }
        REQUIRE(insert384.has_value());

        // Insert 768-dim vector
        auto emb768 = createEmbedding(768, 2.0f);
        auto rec768 = createVectorRecord("vec_768", emb768);
        auto insert768 = backend.insertVector(rec768);
        REQUIRE(insert768.has_value());

        // Both should be stored
        auto count = backend.getVectorCount();
        REQUIRE(count.has_value());
        CHECK(count.value() == 2);
    }

    SECTION("insert multiple dimensions in batch") {
        std::vector<VectorRecord> records;

        // Mix of 384 and 768 dimension vectors
        for (int i = 0; i < 5; ++i) {
            records.push_back(createVectorRecord("batch_384_" + std::to_string(i),
                                                 createEmbedding(384, static_cast<float>(i + 1))));
        }
        for (int i = 0; i < 5; ++i) {
            records.push_back(createVectorRecord("batch_768_" + std::to_string(i),
                                                 createEmbedding(768, static_cast<float>(i + 10))));
        }

        auto result = backend.insertVectorsBatch(records);
        REQUIRE(result.has_value());

        auto count = backend.getVectorCount();
        REQUIRE(count.has_value());
        CHECK(count.value() == 10);
    }
}

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW search routes to correct dimension index",
                 "[vector][multidim][search][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.hnsw_m = 16;
    config.hnsw_ef_construction = 100;
    config.hnsw_ef_search = 50;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());

    auto createResult = backend.createTables(384);
    REQUIRE(createResult.has_value());

    // Insert vectors of different dimensions
    auto emb384_a = createEmbedding(384, 1.0f);
    auto emb384_b = createEmbedding(384, 1.1f);
    auto emb768_a = createEmbedding(768, 2.0f);
    auto emb768_b = createEmbedding(768, 2.1f);

    REQUIRE(backend.insertVector(createVectorRecord("v384_a", emb384_a)).has_value());
    REQUIRE(backend.insertVector(createVectorRecord("v384_b", emb384_b)).has_value());
    REQUIRE(backend.insertVector(createVectorRecord("v768_a", emb768_a)).has_value());
    REQUIRE(backend.insertVector(createVectorRecord("v768_b", emb768_b)).has_value());

    SECTION("384-dim query finds only 384-dim vectors") {
        auto query = createEmbedding(384, 1.0f);
        auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());

        auto& results = searchResult.value();
        // Should find the 384-dim vectors
        CHECK(results.size() >= 1);
        CHECK(results.size() <= 2); // Only 384-dim vectors

        for (const auto& r : results) {
            CHECK(r.embedding.size() == 384);
        }
    }

    SECTION("768-dim query finds only 768-dim vectors") {
        auto query = createEmbedding(768, 2.0f);
        auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());

        auto& results = searchResult.value();
        // Should find the 768-dim vectors
        CHECK(results.size() >= 1);
        CHECK(results.size() <= 2); // Only 768-dim vectors

        for (const auto& r : results) {
            CHECK(r.embedding.size() == 768);
        }
    }

    SECTION("query for non-existent dimension returns empty") {
        auto query = createEmbedding(512, 3.0f); // No 512-dim vectors inserted
        auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());
        CHECK(searchResult.value().empty());
    }
}

// =============================================================================
// Delete Operations with Multi-Dimension
// =============================================================================

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW delete from correct dimension index",
                 "[vector][multidim][delete][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());

    auto createResult = backend.createTables(384);
    REQUIRE(createResult.has_value());

    // Insert vectors
    auto emb384 = createEmbedding(384, 1.0f);
    auto emb768 = createEmbedding(768, 2.0f);
    REQUIRE(backend.insertVector(createVectorRecord("del_384", emb384)).has_value());
    REQUIRE(backend.insertVector(createVectorRecord("del_768", emb768)).has_value());

    SECTION("delete 384-dim vector, 768-dim still searchable") {
        auto delResult = backend.deleteVector("chunk_del_384");
        REQUIRE(delResult.has_value());

        // 384-dim search should return nothing
        auto search384 = backend.searchSimilar(emb384, 10, 0.0f, std::nullopt, {});
        REQUIRE(search384.has_value());
        CHECK(search384.value().empty());

        // 768-dim search should still work
        auto search768 = backend.searchSimilar(emb768, 10, 0.0f, std::nullopt, {});
        REQUIRE(search768.has_value());
        CHECK(search768.value().size() == 1);
    }

    SECTION("delete by document clears from correct indices") {
        // Add another 384-dim vector with same document
        auto emb384_2 = createEmbedding(384, 1.5f);
        VectorRecord rec2;
        rec2.document_hash = "doc_del_384"; // Same document as del_384
        rec2.chunk_id = "chunk_del_384_2";
        rec2.embedding = emb384_2;
        rec2.content = "Second chunk";
        REQUIRE(backend.insertVector(rec2).has_value());

        // Delete by document
        auto delResult = backend.deleteVectorsByDocument("doc_del_384");
        REQUIRE(delResult.has_value());

        // 384-dim search should return nothing
        auto search384 = backend.searchSimilar(emb384, 10, 0.0f, std::nullopt, {});
        REQUIRE(search384.has_value());
        CHECK(search384.value().empty());

        // 768-dim should still be there
        auto search768 = backend.searchSimilar(emb768, 10, 0.0f, std::nullopt, {});
        REQUIRE(search768.has_value());
        CHECK(search768.value().size() == 1);
    }
}

// =============================================================================
// Update Operations with Dimension Change
// =============================================================================

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW update vector with dimension change",
                 "[vector][multidim][update][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());

    auto createResult = backend.createTables(384);
    REQUIRE(createResult.has_value());

    // Insert a 384-dim vector
    auto emb384 = createEmbedding(384, 1.0f);
    REQUIRE(backend.insertVector(createVectorRecord("upd_vec", emb384)).has_value());

    SECTION("update to same dimension") {
        auto newEmb384 = createEmbedding(384, 1.5f);
        VectorRecord newRec = createVectorRecord("upd_vec", newEmb384);

        auto updResult = backend.updateVector("chunk_upd_vec", newRec);
        REQUIRE(updResult.has_value());

        // Should be searchable with new embedding
        auto searchResult = backend.searchSimilar(newEmb384, 1, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());
        REQUIRE(searchResult.value().size() == 1);
        CHECK(searchResult.value()[0].chunk_id == "chunk_upd_vec");
    }

    SECTION("update to different dimension") {
        // Update from 384 to 768 dimensions
        auto newEmb768 = createEmbedding(768, 2.0f);
        VectorRecord newRec = createVectorRecord("upd_vec", newEmb768);

        auto updResult = backend.updateVector("chunk_upd_vec", newRec);
        REQUIRE(updResult.has_value());

        // Should NOT be found with 384-dim query
        auto search384 = backend.searchSimilar(emb384, 10, 0.0f, std::nullopt, {});
        REQUIRE(search384.has_value());
        CHECK(search384.value().empty());

        // Should be found with 768-dim query
        auto search768 = backend.searchSimilar(newEmb768, 1, 0.0f, std::nullopt, {});
        REQUIRE(search768.has_value());
        REQUIRE(search768.value().size() == 1);
        CHECK(search768.value()[0].chunk_id == "chunk_upd_vec");
        CHECK(search768.value()[0].embedding.size() == 768);
    }
}

// =============================================================================
// Persistence with Multi-Dimension
// =============================================================================

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW persistence across close/reopen",
                 "[vector][multidim][persistence][catch2]") {
    skipIfNeeded();

    // Use a temp file for persistence test
    std::string tmpPath = std::filesystem::temp_directory_path() / "multi_dim_hnsw_test.db";

    // Clean up any previous test
    std::filesystem::remove(tmpPath);

    auto emb384 = createEmbedding(384, 1.0f);
    auto emb768 = createEmbedding(768, 2.0f);

    // First session: create and populate
    {
        SqliteVecBackend::Config config;
        SqliteVecBackend backend(config);
        auto result = backend.initialize(tmpPath);
        REQUIRE(result.has_value());

        auto createResult = backend.createTables(384);
        REQUIRE(createResult.has_value());

        REQUIRE(backend.insertVector(createVectorRecord("persist_384", emb384)).has_value());
        REQUIRE(backend.insertVector(createVectorRecord("persist_768", emb768)).has_value());

        backend.close();
    }

    // Second session: reopen and verify
    {
        SqliteVecBackend::Config config;
        SqliteVecBackend backend(config);
        auto result = backend.initialize(tmpPath);
        REQUIRE(result.has_value());

        // Verify count
        auto count = backend.getVectorCount();
        REQUIRE(count.has_value());
        CHECK(count.value() == 2);

        // Verify 384-dim search
        auto search384 = backend.searchSimilar(emb384, 1, 0.0f, std::nullopt, {});
        REQUIRE(search384.has_value());
        REQUIRE(search384.value().size() == 1);
        CHECK(search384.value()[0].chunk_id == "chunk_persist_384");

        // Verify 768-dim search
        auto search768 = backend.searchSimilar(emb768, 1, 0.0f, std::nullopt, {});
        REQUIRE(search768.has_value());
        REQUIRE(search768.value().size() == 1);
        CHECK(search768.value()[0].chunk_id == "chunk_persist_768");

        backend.close();
    }

    // Cleanup
    std::filesystem::remove(tmpPath);
}

// =============================================================================
// Common Embedding Dimensions
// =============================================================================

TEST_CASE_METHOD(MultiDimensionFixture, "MultiDimHNSW supports common embedding model dimensions",
                 "[vector][multidim][models][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE(result.has_value());

    auto createResult = backend.createTables(384);
    REQUIRE(createResult.has_value());

    // Common embedding model dimensions
    std::vector<std::pair<size_t, std::string>> models = {
        {384, "MiniLM-L6"},       {768, "nomic-embed-text-v1.5"},          {1024, "E5-large"},
        {1536, "OpenAI ada-002"}, {3072, "OpenAI text-embedding-3-large"},
    };

    // Insert one vector for each model dimension
    for (const auto& [dim, model] : models) {
        auto emb = createEmbedding(dim, static_cast<float>(dim) / 100.0f);
        auto rec = createVectorRecord("model_" + model, emb);
        INFO("Inserting " << dim << "-dim vector for model: " << model);
        auto insertResult = backend.insertVector(rec);
        REQUIRE(insertResult.has_value());
    }

    // Verify count
    auto count = backend.getVectorCount();
    REQUIRE(count.has_value());
    CHECK(count.value() == models.size());

    // Verify each dimension can be searched independently
    for (const auto& [dim, model] : models) {
        INFO("Searching " << dim << "-dim vectors for model: " << model);
        auto query = createEmbedding(dim, static_cast<float>(dim) / 100.0f);
        auto searchResult = backend.searchSimilar(query, 1, 0.0f, std::nullopt, {});
        REQUIRE(searchResult.has_value());
        REQUIRE(searchResult.value().size() == 1);
        CHECK(searchResult.value()[0].embedding.size() == dim);
    }
}

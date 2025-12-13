// Catch2 tests for vector database dimension validation and error handling
// Migrated from GTest: vector_dimension_validation_test.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>
#include <yams/vector/vector_database.h>

using namespace yams::vector;
using Catch::Matchers::ContainsSubstring;

namespace {

struct VectorDimensionFixture {
    VectorDimensionFixture() {
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

    VectorDatabaseConfig createConfig(size_t dim) {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = dim;
        config.create_if_missing = true;
        config.use_in_memory = true;
        return config;
    }

    std::vector<float> createEmbedding(size_t dim, float value = 1.0f) {
        std::vector<float> emb(dim);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = value * static_cast<float>(i + 1) / static_cast<float>(dim);
        }
        return emb;
    }

    VectorRecord createVectorRecord(const std::string& hash, const std::vector<float>& embedding) {
        VectorRecord rec;
        rec.document_hash = hash;
        rec.chunk_id = hash + "_chunk_0";
        rec.embedding = embedding;
        rec.content = "Test content for " + hash;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::string skipReason;
};

} // namespace

// =============================================================================
// Single Vector Insert Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation insert correct dimensions",
                 "[vector][dimension][catch2]") {
    skipIfNeeded();

    SECTION("384-dim embedding accepted when configured for 384") {
        auto config = createConfig(384);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(384);
        CHECK(db.addVector("test_hash_001", embedding));
    }

    SECTION("768-dim embedding accepted when configured for 768") {
        auto config = createConfig(768);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(768);
        CHECK(db.addVector("test_hash_002", embedding));
    }

    SECTION("1024-dim embedding accepted when configured for 1024") {
        auto config = createConfig(1024);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(1024);
        CHECK(db.addVector("test_hash_003", embedding));
    }
}

// =============================================================================
// Dimension Mismatch Error Messages
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation dimension mismatch errors",
                 "[vector][dimension][error][catch2]") {
    skipIfNeeded();

    SECTION("768-dim rejected when configured for 384") {
        auto config = createConfig(384);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(768);
        CHECK_FALSE(db.addVector("test_hash_004", embedding));

        std::string error = db.getLastError();
        CHECK_THAT(error, ContainsSubstring("dimension") || ContainsSubstring("Invalid"));
    }

    SECTION("384-dim rejected when configured for 768") {
        auto config = createConfig(768);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(384);
        CHECK_FALSE(db.addVector("test_hash_005", embedding));

        std::string error = db.getLastError();
        CHECK_THAT(error, ContainsSubstring("dimension") || ContainsSubstring("Invalid"));
    }

    SECTION("512-dim rejected when configured for 384 with error details") {
        auto config = createConfig(384);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto embedding = createEmbedding(512);
        CHECK_FALSE(db.addVector("test_hash_006", embedding));

        std::string error = db.getLastError();
        CHECK_FALSE(error.empty());
    }
}

// =============================================================================
// Batch Insert Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation batch insert",
                 "[vector][dimension][batch][catch2]") {
    skipIfNeeded();

    auto config = createConfig(384);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    SECTION("all correct dimensions accepted") {
        std::vector<VectorRecord> records;
        for (int i = 0; i < 5; ++i) {
            auto emb = createEmbedding(384);
            records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
        }
        CHECK(db.insertVectorsBatch(records));
    }

    SECTION("one mismatch rejects batch") {
        std::vector<VectorRecord> records;
        records.push_back(createVectorRecord("hash_0", createEmbedding(384)));
        records.push_back(createVectorRecord("hash_1", createEmbedding(384)));
        records.push_back(createVectorRecord("hash_2", createEmbedding(768))); // Wrong!
        records.push_back(createVectorRecord("hash_3", createEmbedding(384)));

        CHECK_FALSE(db.insertVectorsBatch(records));

        std::string error = db.getLastError();
        CHECK_THAT(error, ContainsSubstring("dimension"));
    }

    SECTION("all wrong dimensions rejected") {
        std::vector<VectorRecord> records;
        for (int i = 0; i < 5; ++i) {
            auto emb = createEmbedding(768); // All wrong
            records.push_back(createVectorRecord("hash_wrong_" + std::to_string(i), emb));
        }
        CHECK_FALSE(db.insertVectorsBatch(records));
    }
}

// =============================================================================
// Search Query Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation search query dimension",
                 "[vector][dimension][search][catch2]") {
    skipIfNeeded();

    auto config = createConfig(384);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert a vector
    auto embedding = createEmbedding(384);
    REQUIRE(db.addVector("test_hash_007", embedding));

    SECTION("correct query dimension returns results") {
        auto results = db.search(embedding, 5);
        REQUIRE(results.has_value());
    }

    SECTION("wrong query dimension fails") {
        auto wrongEmb = createEmbedding(768);
        auto results = db.search(wrongEmb, 5);
        CHECK_FALSE(results.has_value());
    }
}

// =============================================================================
// Empty/Edge Case Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation edge cases",
                 "[vector][dimension][edge][catch2]") {
    skipIfNeeded();

    SECTION("empty vector rejected") {
        auto config = createConfig(384);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        std::vector<float> emptyVec;
        CHECK_FALSE(db.addVector("test_hash_009", emptyVec));
    }

    SECTION("zero dimension config rejects vectors") {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = 0; // Invalid
        config.create_if_missing = true;
        config.use_in_memory = true;

        VectorDatabase db(config);
        db.initialize(); // May fail or succeed

        auto embedding = createEmbedding(384);
        CHECK_FALSE(db.addVector("test_hash_010", embedding));
    }
}

// =============================================================================
// Common Dimension Combinations (replacing parameterized test)
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation common dimension combinations",
                 "[vector][dimension][combinations][catch2]") {
    skipIfNeeded();

    SECTION("matching dimensions succeed") {
        struct DimTest {
            size_t dim;
            const char* name;
        };
        std::vector<DimTest> tests = {
            {384, "MiniLM"},
            {768, "MPNet"},
            {1024, "E5-large"},
            {1536, "OpenAI ada-002"},
        };

        for (const auto& t : tests) {
            INFO("Testing dimension " << t.dim << " (" << t.name << ")");
            auto config = createConfig(t.dim);
            VectorDatabase db(config);
            REQUIRE(db.initialize());
            auto emb = createEmbedding(t.dim);
            CHECK(db.addVector("test_hash", emb));
        }
    }

    SECTION("mismatched dimensions fail") {
        struct MismatchTest {
            size_t configDim;
            size_t vectorDim;
        };
        std::vector<MismatchTest> tests = {
            {384, 768},  {768, 384},  {384, 1024},
            {1024, 384}, {768, 1024}, {1024, 768},
        };

        for (const auto& t : tests) {
            INFO("Config dim=" << t.configDim << ", vector dim=" << t.vectorDim);
            auto config = createConfig(t.configDim);
            VectorDatabase db(config);
            REQUIRE(db.initialize());
            auto emb = createEmbedding(t.vectorDim);
            CHECK_FALSE(db.addVector("test_hash", emb));
        }
    }
}

// =============================================================================
// Dimension Schema Consistency
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation schema preserved across inserts",
                 "[vector][dimension][schema][catch2]") {
    skipIfNeeded();

    auto config = createConfig(384);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert first vector with correct dimension
    auto emb1 = createEmbedding(384);
    REQUIRE(db.addVector("hash_1", emb1));

    // Schema should still enforce 384 dimensions
    auto wrongEmb = createEmbedding(768);
    CHECK_FALSE(db.addVector("hash_2", wrongEmb));

    // Correct dimension should still work
    auto emb2 = createEmbedding(384);
    CHECK(db.addVector("hash_3", emb2));
}

// =============================================================================
// Large Batch Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation large batch",
                 "[vector][dimension][batch][large][catch2]") {
    skipIfNeeded();

    auto config = createConfig(384);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    SECTION("100 correct vectors succeed") {
        std::vector<VectorRecord> records;
        for (int i = 0; i < 100; ++i) {
            auto emb = createEmbedding(384);
            records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
        }
        CHECK(db.insertVectorsBatch(records));
    }

    SECTION("100 vectors with one wrong in middle fails") {
        std::vector<VectorRecord> records;
        for (int i = 0; i < 100; ++i) {
            size_t dim = (i == 50) ? 768 : 384; // One wrong in the middle
            auto emb = createEmbedding(dim);
            records.push_back(createVectorRecord("hash_lg_" + std::to_string(i), emb));
        }
        CHECK_FALSE(db.insertVectorsBatch(records));

        std::string error = db.getLastError();
        CHECK_FALSE(error.empty());
    }
}

// =============================================================================
// Update with Dimension Validation
// =============================================================================

TEST_CASE_METHOD(VectorDimensionFixture,
                 "VectorDimensionValidation update operations",
                 "[vector][dimension][update][catch2]") {
    skipIfNeeded();

    auto config = createConfig(384);
    VectorDatabase db(config);
    REQUIRE(db.initialize());

    SECTION("update with correct dimension succeeds") {
        // Insert initial vector
        auto emb1 = createEmbedding(384, 1.0f);
        REQUIRE(db.addVector("test_hash_011", emb1));

        // Update with same dimension
        VectorRecord rec = createVectorRecord("test_hash_011", createEmbedding(384, 2.0f));
        CHECK(db.updateVector(rec));
    }

    SECTION("update with wrong dimension fails") {
        // Insert initial 384-dim vector
        auto emb384 = createEmbedding(384);
        REQUIRE(db.addVector("test_hash_012", emb384));

        // Try to update with 768-dim vector
        VectorRecord rec = createVectorRecord("test_hash_012", createEmbedding(768));
        CHECK_FALSE(db.updateVector(rec));
    }
}

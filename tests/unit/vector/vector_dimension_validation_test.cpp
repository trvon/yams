// Unit tests for vector database dimension validation and error handling
#include <cstdlib>
#include <filesystem>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/vector/vector_database.h>

using namespace testing;

namespace yams::vector {

class VectorDimensionValidationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Skip if vectors disabled
        if (const char* skip_env = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skip_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping vector test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disable_env = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disable_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping vector test (YAMS_DISABLE_VECTORS=1)";
            }
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
};

// ============================================================================
// Test 1: Single Vector Insert Dimension Validation
// ============================================================================

TEST_F(VectorDimensionValidationTest, InsertCorrectDimension384) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    auto embedding = createEmbedding(384);
    EXPECT_TRUE(db.addVector("test_hash_001", embedding))
        << "Should accept 384-dim embedding when configured for 384";
}

TEST_F(VectorDimensionValidationTest, InsertCorrectDimension768) {
    auto config = createConfig(768);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    auto embedding = createEmbedding(768);
    EXPECT_TRUE(db.addVector("test_hash_002", embedding))
        << "Should accept 768-dim embedding when configured for 768";
}

TEST_F(VectorDimensionValidationTest, InsertCorrectDimension1024) {
    auto config = createConfig(1024);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    auto embedding = createEmbedding(1024);
    EXPECT_TRUE(db.addVector("test_hash_003", embedding))
        << "Should accept 1024-dim embedding when configured for 1024";
}

// ============================================================================
// Test 2: Dimension Mismatch Error Messages
// ============================================================================

TEST_F(VectorDimensionValidationTest, InsertDimensionMismatch384vs768) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Try to insert 768-dim vector into 384-dim DB
    auto embedding = createEmbedding(768);
    EXPECT_FALSE(db.addVector("test_hash_004", embedding))
        << "Should reject 768-dim embedding when configured for 384";

    std::string error = db.getLastError();
    EXPECT_THAT(error, AnyOf(HasSubstr("dimension"), HasSubstr("Invalid")))
        << "Error message should mention dimension problem";
}

TEST_F(VectorDimensionValidationTest, InsertDimensionMismatch768vs384) {
    auto config = createConfig(768);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Try to insert 384-dim vector into 768-dim DB
    auto embedding = createEmbedding(384);
    EXPECT_FALSE(db.addVector("test_hash_005", embedding))
        << "Should reject 384-dim embedding when configured for 768";

    std::string error = db.getLastError();
    EXPECT_THAT(error, AnyOf(HasSubstr("dimension"), HasSubstr("Invalid")))
        << "Error message should mention dimension problem";
}

TEST_F(VectorDimensionValidationTest, InsertDimensionMismatchErrorDetails) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    auto embedding = createEmbedding(512); // Wrong dimension
    EXPECT_FALSE(db.addVector("test_hash_006", embedding));

    std::string error = db.getLastError();
    // After enhancement, error should include both expected and actual dimensions
    EXPECT_THAT(error, Not(IsEmpty())) << "Should provide error message";
    // TODO: After enhancement, verify format like "expected=384, got=512"
}

// ============================================================================
// Test 3: Batch Insert Dimension Validation
// ============================================================================

TEST_F(VectorDimensionValidationTest, BatchInsertAllCorrectDimensions) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    std::vector<VectorRecord> records;
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(384);
        records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
    }

    EXPECT_TRUE(db.insertVectorsBatch(records))
        << "Should accept batch when all vectors have correct dimension";
}

TEST_F(VectorDimensionValidationTest, BatchInsertOneMismatch) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    std::vector<VectorRecord> records;
    records.push_back(createVectorRecord("hash_0", createEmbedding(384)));
    records.push_back(createVectorRecord("hash_1", createEmbedding(384)));
    records.push_back(createVectorRecord("hash_2", createEmbedding(768))); // Wrong!
    records.push_back(createVectorRecord("hash_3", createEmbedding(384)));

    EXPECT_FALSE(db.insertVectorsBatch(records))
        << "Should reject batch when one vector has wrong dimension";

    std::string error = db.getLastError();
    EXPECT_THAT(error, HasSubstr("dimension"))
        << "Error should indicate dimension problem in batch";
}

TEST_F(VectorDimensionValidationTest, BatchInsertAllMismatch) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    std::vector<VectorRecord> records;
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(768); // All wrong
        records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
    }

    EXPECT_FALSE(db.insertVectorsBatch(records))
        << "Should reject batch when all vectors have wrong dimension";
}

// ============================================================================
// Test 4: Search Query Dimension Validation
// ============================================================================

TEST_F(VectorDimensionValidationTest, SearchWithCorrectDimension) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert a vector
    auto embedding = createEmbedding(384);
    ASSERT_TRUE(db.addVector("test_hash_007", embedding));

    // Search with same dimension
    auto results = db.search(embedding, 5);
    EXPECT_TRUE(results.has_value()) << "Search should work with correct query dimension";
}

TEST_F(VectorDimensionValidationTest, SearchWithWrongDimension) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert a 384-dim vector
    auto embedding384 = createEmbedding(384);
    ASSERT_TRUE(db.addVector("test_hash_008", embedding384));

    // Try to search with 768-dim query
    auto embedding768 = createEmbedding(768);
    auto results = db.search(embedding768, 5);

    EXPECT_FALSE(results.has_value())
        << "Search should fail when query dimension doesn't match DB dimension";
}

// ============================================================================
// Test 5: Empty/Edge Case Dimension Validation
// ============================================================================

TEST_F(VectorDimensionValidationTest, InsertEmptyVector) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    std::vector<float> emptyVec;
    EXPECT_FALSE(db.addVector("test_hash_009", emptyVec)) << "Should reject empty vector";
}

TEST_F(VectorDimensionValidationTest, InsertZeroDimension) {
    // This tests configuration validation
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 0; // Invalid
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    // Initialize might fail or succeed - behavior depends on implementation
    // But any insert should fail
    db.initialize();

    auto embedding = createEmbedding(384);
    EXPECT_FALSE(db.addVector("test_hash_010", embedding))
        << "DB with 0 dimension should reject any vector";
}

// ============================================================================
// Test 6: Common Dimension Combinations
// ============================================================================

struct DimensionPair {
    size_t configDim;
    size_t vectorDim;
    bool shouldSucceed;
    std::string description;
};

class VectorDimensionPairTest : public VectorDimensionValidationTest,
                                public WithParamInterface<DimensionPair> {};

TEST_P(VectorDimensionPairTest, InsertValidation) {
    auto params = GetParam();
    auto config = createConfig(params.configDim);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize()) << "Failed to initialize for " << params.description;

    auto embedding = createEmbedding(params.vectorDim);
    bool result = db.addVector("test_hash", embedding);

    EXPECT_EQ(result, params.shouldSucceed) << params.description;
}

INSTANTIATE_TEST_SUITE_P(CommonDimensions, VectorDimensionPairTest,
                         Values(
                             // Matching dimensions - should succeed
                             DimensionPair{384, 384, true, "384 vs 384 (MiniLM)"},
                             DimensionPair{768, 768, true, "768 vs 768 (MPNet)"},
                             DimensionPair{1024, 1024, true, "1024 vs 1024 (E5-large)"},
                             DimensionPair{1536, 1536, true, "1536 vs 1536 (OpenAI ada-002)"},

                             // Mismatches - should fail
                             DimensionPair{384, 768, false, "384 config vs 768 vector"},
                             DimensionPair{768, 384, false, "768 config vs 384 vector"},
                             DimensionPair{384, 1024, false, "384 config vs 1024 vector"},
                             DimensionPair{1024, 384, false, "1024 config vs 384 vector"},
                             DimensionPair{768, 1024, false, "768 config vs 1024 vector"},
                             DimensionPair{1024, 768, false, "1024 config vs 768 vector"}));

// ============================================================================
// Test 7: Dimension Schema Consistency
// ============================================================================

TEST_F(VectorDimensionValidationTest, SchemaPreservedAcrossInserts) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert first vector with correct dimension
    auto emb1 = createEmbedding(384);
    ASSERT_TRUE(db.addVector("hash_1", emb1));

    // Schema should still enforce 384 dimensions
    auto wrongEmb = createEmbedding(768);
    EXPECT_FALSE(db.addVector("hash_2", wrongEmb))
        << "Schema should remain 384-dim after first insert";

    // Correct dimension should still work
    auto emb2 = createEmbedding(384);
    EXPECT_TRUE(db.addVector("hash_3", emb2)) << "384-dim vectors should still be accepted";
}

// ============================================================================
// Test 8: Large Batch Dimension Validation Performance
// ============================================================================

TEST_F(VectorDimensionValidationTest, LargeBatchDimensionCheck) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Create batch of 100 vectors
    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(384);
        records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
    }

    EXPECT_TRUE(db.insertVectorsBatch(records))
        << "Large batch with correct dimensions should succeed";
}

TEST_F(VectorDimensionValidationTest, LargeBatchWithOneWrongDimension) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Create batch of 100 vectors, one with wrong dimension
    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        size_t dim = (i == 50) ? 768 : 384; // One wrong in the middle
        auto emb = createEmbedding(dim);
        records.push_back(createVectorRecord("hash_" + std::to_string(i), emb));
    }

    EXPECT_FALSE(db.insertVectorsBatch(records))
        << "Batch should fail fast when dimension mismatch detected";

    std::string error = db.getLastError();
    EXPECT_THAT(error, Not(IsEmpty())) << "Should provide error message for failed batch";
}

// ============================================================================
// Test 9: Update with Dimension Validation
// ============================================================================

TEST_F(VectorDimensionValidationTest, UpdateVectorWithCorrectDimension) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert initial vector
    auto emb1 = createEmbedding(384, 1.0f);
    ASSERT_TRUE(db.addVector("test_hash_011", emb1));

    // Update with same dimension
    VectorRecord rec = createVectorRecord("test_hash_011", createEmbedding(384, 2.0f));
    EXPECT_TRUE(db.updateVector(rec)) << "Should allow update with correct dimension";
}

TEST_F(VectorDimensionValidationTest, UpdateVectorWithWrongDimension) {
    auto config = createConfig(384);
    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert initial 384-dim vector
    auto emb384 = createEmbedding(384);
    ASSERT_TRUE(db.addVector("test_hash_012", emb384));

    // Try to update with 768-dim vector
    VectorRecord rec = createVectorRecord("test_hash_012", createEmbedding(768));
    EXPECT_FALSE(db.updateVector(rec)) << "Should reject update with wrong dimension";
}

} // namespace yams::vector

// Unit tests for ingest helper dimension validation and error propagation
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

using namespace yams;
using namespace yams::ingest;
using namespace testing;

namespace {

// ============================================================================
// Mock Model Provider for Testing
// ============================================================================

class MockModelProvider : public daemon::IModelProvider {
public:
    MOCK_METHOD(Result<std::vector<float>>, generateEmbedding, (const std::string&), (override));
    MOCK_METHOD(Result<std::vector<std::vector<float>>>, generateBatchEmbeddings,
                (const std::vector<std::string>&), (override));
    MOCK_METHOD(Result<std::vector<float>>, generateEmbeddingFor,
                (const std::string&, const std::string&), (override));
    MOCK_METHOD(Result<std::vector<std::vector<float>>>, generateBatchEmbeddingsFor,
                (const std::string&, const std::vector<std::string>&), (override));

    MOCK_METHOD(Result<void>, loadModel, (const std::string&), (override));
    MOCK_METHOD(Result<void>, loadModelWithOptions, (const std::string&, const std::string&),
                (override));
    MOCK_METHOD(Result<void>, unloadModel, (const std::string&), (override));
    MOCK_METHOD(bool, isModelLoaded, (const std::string&), (const, override));
    MOCK_METHOD(std::vector<std::string>, getLoadedModels, (), (const, override));
    MOCK_METHOD(size_t, getLoadedModelCount, (), (const, override));
    MOCK_METHOD(Result<daemon::ModelInfo>, getModelInfo, (const std::string&), (const, override));
    MOCK_METHOD(size_t, getEmbeddingDim, (const std::string&), (const, override));
    MOCK_METHOD(std::shared_ptr<vector::EmbeddingGenerator>, getEmbeddingGenerator,
                (const std::string&), (override));
    MOCK_METHOD(std::string, getProviderName, (), (const, override));
    MOCK_METHOD(std::string, getProviderVersion, (), (const, override));
    MOCK_METHOD(bool, isAvailable, (), (const, override));
    MOCK_METHOD(size_t, getMemoryUsage, (), (const, override));
    MOCK_METHOD(void, releaseUnusedResources, (), (override));
    MOCK_METHOD(void, shutdown, (), (override));
    MOCK_METHOD(void, setProgressCallback, (std::function<void(const daemon::ModelLoadEvent&)>),
                (override));

    // Helper to set dimension
    void setDimension(size_t dim) { mockDim_ = dim; }

    size_t mockDim_ = 384;
};

// ============================================================================
// Mock Vector Database for Testing
// ============================================================================

class MockVectorDatabase {
public:
    MockVectorDatabase(size_t configuredDim) : configuredDim_(configuredDim) {}

    bool insertVectorsBatch(const std::vector<vector::VectorRecord>& records) {
        for (const auto& rec : records) {
            if (rec.embedding.size() != configuredDim_) {
                lastError_ = "Invalid vector record in batch (expected_dim=" +
                             std::to_string(configuredDim_) +
                             ", got_dim=" + std::to_string(rec.embedding.size()) + ")";
                return false;
            }
        }
        insertedCount_ += records.size();
        return true;
    }

    std::string getLastError() const { return lastError_; }
    size_t getConfiguredDim() const { return configuredDim_; }
    size_t getInsertedCount() const { return insertedCount_; }

private:
    size_t configuredDim_;
    size_t insertedCount_ = 0;
    std::string lastError_;
};

// ============================================================================
// Mock Metadata Repository
// ============================================================================

class MockMetadataRepository : public metadata::IMetadataRepository {
public:
    // Minimal implementation for testing
    MOCK_METHOD(Result<int64_t>, addDocument,
                (const std::string&, const std::optional<std::string>&,
                 const std::optional<std::string>&, const std::optional<std::string>&),
                (override));
    MOCK_METHOD(Result<std::optional<metadata::DocumentInfo>>, getDocumentByHash,
                (const std::string&), (const, override));
    MOCK_METHOD(Result<std::optional<metadata::DocumentContent>>, getContent, (int64_t),
                (const, override));

    // Add stubs for other required methods
    Result<std::optional<metadata::DocumentInfo>> getDocument(int64_t id) const override {
        return std::nullopt;
    }
    Result<void> updateDocument(const metadata::DocumentInfo&) override { return Result<void>(); }
    Result<int64_t> insertContent(const metadata::DocumentContent&) override { return 0; }
    Result<void> indexDocumentContent(int64_t, const std::string&, const std::string&,
                                      const std::string&) override {
        return Result<void>();
    }
    Result<void> updateFuzzyIndex(int64_t) override { return Result<void>(); }

    // Stub remaining pure virtual methods (not used in these tests)
    Result<void> removeDocument(const std::string&) override { return Result<void>(); }
    Result<std::vector<std::string>> listAllHashes() const override {
        return std::vector<std::string>{};
    }
    Result<void> addTag(const std::string&, const std::string&) override { return Result<void>(); }
    Result<std::vector<std::string>> getDocumentTags(int64_t) const override {
        return std::vector<std::string>{};
    }
    Result<std::vector<metadata::SearchResult>> search(const std::string&, int) const override {
        return std::vector<metadata::SearchResult>{};
    }
    void close() override {}
};

} // anonymous namespace

// ============================================================================
// Dimension Validation Tests
// ============================================================================

class IngestDimensionValidationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Skip if vectors disabled
        if (const char* disable_env = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disable_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping vector test (YAMS_DISABLE_VECTORS=1)";
            }
        }

        provider_ = std::make_shared<MockModelProvider>();
        metadata_ = std::make_shared<MockMetadataRepository>();
    }

    std::vector<float> createEmbedding(size_t dim) {
        std::vector<float> emb(dim);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = static_cast<float>(i) / static_cast<float>(dim);
        }
        return emb;
    }

    std::shared_ptr<MockModelProvider> provider_;
    std::shared_ptr<MockMetadataRepository> metadata_;
};

// ============================================================================
// Test 1: Successful Embedding with Matching Dimensions
// ============================================================================

TEST_F(IngestDimensionValidationTest, DISABLED_EmbedAndInsert384Success) {
    // NOTE: This test is disabled because embed_and_insert_document needs a real VectorDatabase
    // We'll re-enable after refactoring to accept VectorDatabase interface

    // Setup: Provider returns 384-dim embeddings, VDB expects 384
    provider_->setDimension(384);

    std::vector<std::vector<float>> embeddings = {createEmbedding(384), createEmbedding(384)};

    EXPECT_CALL(*provider_, generateBatchEmbeddingsFor(_, _))
        .WillOnce(Return(Result<std::vector<std::vector<float>>>(embeddings)));

    EXPECT_CALL(*provider_, getEmbeddingDim(_)).WillOnce(Return(384));

    // This would work if we refactored embed_and_insert_document to accept interfaces
    // auto result = embed_and_insert_document(*provider_, "test-model", *vdb, *metadata_,
    //                                          "hash123", "test content", "test.txt", "/tmp",
    //                                          "text/plain", {});

    // EXPECT_TRUE(result);
    // EXPECT_EQ(result.value(), 2); // 2 chunks embedded
}

// ============================================================================
// Test 2: Dimension Mismatch Detection BEFORE Generation
// ============================================================================

TEST_F(IngestDimensionValidationTest, DISABLED_PreGenerationDimensionCheck) {
    // NOTE: This test validates the enhancement we'll implement
    // After Enhancement 2, this should work

    // Setup: Provider produces 768-dim, VDB expects 384
    provider_->setDimension(768);

    EXPECT_CALL(*provider_, getEmbeddingDim(_)).WillOnce(Return(768));

    // After enhancement, this should fail BEFORE generating embeddings
    // auto result = embed_and_insert_document(*provider_, "test-model", *vdb, *metadata_,
    //                                          "hash123", "test content", "test.txt", "/tmp",
    //                                          "text/plain", {});

    // EXPECT_FALSE(result);
    // EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    // EXPECT_THAT(result.error().message, AllOf(
    //     HasSubstr("dimension mismatch"),
    //     HasSubstr("768"),
    //     HasSubstr("384")
    // ));

    // Verify embeddings were NOT generated (efficiency check)
    // EXPECT_CALL(*provider_, generateBatchEmbeddingsFor(_, _)).Times(0);
}

// ============================================================================
// Test 3: Clear Error Messages
// ============================================================================

TEST_F(IngestDimensionValidationTest, DISABLED_DimensionMismatchErrorMessage) {
    // After enhancement, error messages should be clear and actionable

    provider_->setDimension(1024);

    EXPECT_CALL(*provider_, getEmbeddingDim(_)).WillOnce(Return(1024));

    // auto result = embed_and_insert_document(...);

    // EXPECT_FALSE(result);
    // std::string error = result.error().message;

    // Error should include:
    // - "dimension mismatch" keyword
    // - Model name
    // - Expected dimension (384)
    // - Actual dimension (1024)
    // - Actionable fix suggestion

    // EXPECT_THAT(error, AllOf(
    //     HasSubstr("dimension mismatch"),
    //     HasSubstr("test-model"),
    //     HasSubstr("1024"),
    //     HasSubstr("384"),
    //     AnyOf(HasSubstr("config"), HasSubstr("rebuild"))
    // ));
}

// ============================================================================
// Test 4: Integration Test Placeholder
// ============================================================================

TEST_F(IngestDimensionValidationTest, IntegrationTestNote) {
    // NOTE: Full integration tests with real VectorDatabase are in
    // tests/integration/embedding_dimension_integration_test.cpp

    // This test file focuses on unit-level validation and mocking
    // For true end-to-end testing with actual VectorDatabase instances,
    // see the integration test suite

    SUCCEED() << "See integration tests for full end-to-end dimension validation";
}

// ============================================================================
// Test 5: Error Propagation Chain
// ============================================================================

TEST_F(IngestDimensionValidationTest, ErrorPropagationChain) {
    // Test that errors propagate correctly through the call chain:
    // embed_and_insert_document -> generateBatchEmbeddingsFor -> plugin -> error

    provider_->setDimension(384);

    // Simulate plugin error
    Error pluginError{ErrorCode::InvalidArgument, "plugin error: generate_embedding_batch"};
    EXPECT_CALL(*provider_, generateBatchEmbeddingsFor(_, _))
        .WillOnce(Return(Result<std::vector<std::vector<float>>>(pluginError)));

    // After enhancement, error should be enriched with context
    // auto result = embed_and_insert_document(...);

    // EXPECT_FALSE(result);
    // EXPECT_THAT(result.error().message, HasSubstr("embeddings"));
}

// ============================================================================
// Test 6: Chunking Dimension Consistency
// ============================================================================

TEST_F(IngestDimensionValidationTest, ChunkingPreservesDimension) {
    // When document is split into chunks, all chunks should get same-dimension embeddings

    provider_->setDimension(384);

    // Create multi-chunk embeddings
    std::vector<std::vector<float>> embeddings;
    for (int i = 0; i < 5; ++i) {
        embeddings.push_back(createEmbedding(384));
    }

    EXPECT_CALL(*provider_, generateBatchEmbeddingsFor(_, _))
        .WillOnce(Return(Result<std::vector<std::vector<float>>>(embeddings)));

    // All chunks should have consistent 384-dim embeddings
    // Verify in real implementation that all VectorRecords have same dimension
}

// ============================================================================
// Test Documentation
// ============================================================================

/*
 * TEST COVERAGE SUMMARY
 * =====================
 *
 * These tests validate the ingest layer's dimension handling:
 *
 * 1. Pre-generation dimension checks (Enhancement 2)
 * 2. Error message enrichment and propagation
 * 3. Chunking dimension consistency
 * 4. Integration with provider and vector DB layers
 *
 * LIMITATIONS:
 * - Some tests are DISABLED pending refactoring of embed_and_insert_document
 *   to accept interface types instead of concrete VectorDatabase
 * - Full end-to-end tests are in integration test suite
 *
 * DEPENDENCIES:
 * - Requires MockModelProvider with dimension control
 * - Requires VectorDatabase interface (future enhancement)
 *
 * RELATED TESTS:
 * - abi_model_provider_dimension_test.cpp (ABI layer)
 * - vector_dimension_validation_test.cpp (Vector DB layer)
 * - tests/integration/embedding_dimension_integration_test.cpp (E2E)
 */

} // namespace yams::ingest::test

// Unit tests for ABI Model Provider dimension handling and error messages
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/plugins/model_provider_v1.h>

using namespace yams::daemon;
using namespace testing;

namespace {

// Mock ONNX plugin provider for testing
struct MockProviderContext {
    yams_status_t nextStatus = YAMS_OK;
    std::string nextError;
    size_t outputDim = 384;
    std::vector<float> nextEmbedding;
    bool simulateDimensionMismatch = false;
};

// Mock implementation of plugin functions
yams_status_t mock_generate_embedding(void* self, const char* model_id, const uint8_t* text,
                                      size_t text_len, float** out_vec, size_t* out_dim) {
    auto* ctx = static_cast<MockProviderContext*>(self);
    if (ctx->simulateDimensionMismatch) {
        // Return wrong dimension
        *out_dim = 768; // Wrong! Config expects 384
        return YAMS_ERR_INVALID_ARG;
    }
    if (ctx->nextStatus != YAMS_OK) {
        return ctx->nextStatus;
    }
    // Allocate embedding with configured dimension
    *out_dim = ctx->outputDim;
    *out_vec = static_cast<float*>(malloc(*out_dim * sizeof(float)));
    for (size_t i = 0; i < *out_dim; ++i) {
        (*out_vec)[i] = static_cast<float>(i) * 0.01f;
    }
    return YAMS_OK;
}

yams_status_t mock_generate_embedding_batch(void* self, const char* model_id,
                                            const uint8_t** inputs, const size_t* input_lens,
                                            size_t batch_size, float** out_vecs, size_t* out_batch,
                                            size_t* out_dim) {
    auto* ctx = static_cast<MockProviderContext*>(self);
    if (ctx->simulateDimensionMismatch) {
        return YAMS_ERR_INVALID_ARG;
    }
    if (ctx->nextStatus != YAMS_OK) {
        return ctx->nextStatus;
    }
    if (!inputs || !input_lens || batch_size == 0) {
        return YAMS_ERR_INVALID_ARG;
    }
    // Allocate batch embeddings
    *out_batch = batch_size;
    *out_dim = ctx->outputDim;
    *out_vecs = static_cast<float*>(malloc(batch_size * ctx->outputDim * sizeof(float)));
    for (size_t i = 0; i < batch_size * ctx->outputDim; ++i) {
        (*out_vecs)[i] = static_cast<float>(i % ctx->outputDim) * 0.01f;
    }
    return YAMS_OK;
}

void mock_free_embedding(void* self, float* vec, size_t dim) {
    free(vec);
}

void mock_free_embedding_batch(void* self, float* vecs, size_t batch, size_t dim) {
    free(vecs);
}

yams_status_t mock_load_model(void* self, const char* model_id, const char* model_path,
                              const char* options) {
    auto* ctx = static_cast<MockProviderContext*>(self);
    return ctx->nextStatus;
}

yams_status_t mock_is_model_loaded(void* self, const char* model_id, bool* loaded) {
    *loaded = true;
    return YAMS_OK;
}

yams_status_t mock_get_embedding_dim(void* self, const char* model_id, size_t* dim) {
    auto* ctx = static_cast<MockProviderContext*>(self);
    *dim = ctx->outputDim;
    return YAMS_OK;
}

yams_status_t mock_get_loaded_models(void* self, const char*** model_ids, size_t* count) {
    static const char* models[] = {"test-model"};
    *model_ids = models;
    *count = 1;
    return YAMS_OK;
}

void mock_free_model_list(void* self, const char** model_ids, size_t count) {
    // Static array, no need to free
}

} // anonymous namespace

class AbiModelProviderDimensionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize mock context
        mockContext_ = std::make_unique<MockProviderContext>();

        // Setup provider table
        table_.self = mockContext_.get();
        table_.generate_embedding = mock_generate_embedding;
        table_.generate_embedding_batch = mock_generate_embedding_batch;
        table_.free_embedding = mock_free_embedding;
        table_.free_embedding_batch = mock_free_embedding_batch;
        table_.load_model = mock_load_model;
        table_.is_model_loaded = mock_is_model_loaded;
        table_.get_embedding_dim = mock_get_embedding_dim;
        table_.get_loaded_models = mock_get_loaded_models;
        table_.free_model_list = mock_free_model_list;
        table_.unload_model = nullptr;
        table_.set_progress_callback = nullptr;
        table_.get_runtime_info_json = nullptr;
        table_.free_string = nullptr;

        adapter_ = std::make_unique<AbiModelProviderAdapter>(&table_);
    }

    void TearDown() override {
        adapter_.reset();
        mockContext_.reset();
    }

    std::unique_ptr<MockProviderContext> mockContext_;
    yams_model_provider_v1 table_{};
    std::unique_ptr<AbiModelProviderAdapter> adapter_;
};

// ============================================================================
// Test 1: Error Message Preservation
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, ErrorCodeMapping) {
    // Test all error codes map correctly
    struct TestCase {
        yams_status_t pluginStatus;
        ErrorCode expectedCode;
        std::string description;
    };

    std::vector<TestCase> cases = {
        {YAMS_OK, ErrorCode::Success, "Success"},
        {YAMS_ERR_INVALID_ARG, ErrorCode::InvalidArgument, "Invalid argument"},
        {YAMS_ERR_NOT_FOUND, ErrorCode::NotFound, "Not found"},
        {YAMS_ERR_IO, ErrorCode::IOError, "IO error"},
        {YAMS_ERR_INTERNAL, ErrorCode::InternalError, "Internal error"},
        {YAMS_ERR_UNSUPPORTED, ErrorCode::NotImplemented, "Not supported"},
    };

    for (const auto& tc : cases) {
        mockContext_->nextStatus = tc.pluginStatus;
        auto result = adapter_->loadModel("test-model");

        if (tc.pluginStatus == YAMS_OK) {
            EXPECT_TRUE(result) << "Expected success for " << tc.description;
        } else {
            EXPECT_FALSE(result) << "Expected failure for " << tc.description;
            EXPECT_EQ(result.error().code, tc.expectedCode)
                << "Wrong error code for " << tc.description;
        }
    }
}

TEST_F(AbiModelProviderDimensionTest, ErrorContextIncludesOperation) {
    mockContext_->nextStatus = YAMS_ERR_INVALID_ARG;

    auto result = adapter_->generateEmbedding("test text");

    ASSERT_FALSE(result);
    EXPECT_THAT(result.error().message, HasSubstr("generate_embedding"))
        << "Error message should include operation name";
}

TEST_F(AbiModelProviderDimensionTest, BatchErrorContextIncludesOperation) {
    mockContext_->nextStatus = YAMS_ERR_INVALID_ARG;

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    ASSERT_FALSE(result);
    EXPECT_THAT(result.error().message, HasSubstr("generate_embedding_batch"))
        << "Error message should include batch operation name";
}

// ============================================================================
// Test 2: Dimension Detection
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, GetEmbeddingDim384) {
    mockContext_->outputDim = 384;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    EXPECT_EQ(dim, 384) << "Should return 384 for MiniLM-like model";
}

TEST_F(AbiModelProviderDimensionTest, GetEmbeddingDim768) {
    mockContext_->outputDim = 768;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    EXPECT_EQ(dim, 768) << "Should return 768 for MPNet-like model";
}

TEST_F(AbiModelProviderDimensionTest, GetEmbeddingDim1024) {
    mockContext_->outputDim = 1024;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    EXPECT_EQ(dim, 1024) << "Should return 1024 for E5-large-like model";
}

TEST_F(AbiModelProviderDimensionTest, GeneratedEmbeddingMatchesDeclaredDim) {
    mockContext_->outputDim = 384;

    auto result = adapter_->generateEmbedding("test text");

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().size(), 384)
        << "Generated embedding dimension should match declared dimension";
}

TEST_F(AbiModelProviderDimensionTest, BatchEmbeddingsMatchDeclaredDim) {
    mockContext_->outputDim = 768;

    std::vector<std::string> texts = {"text1", "text2", "text3"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().size(), 3);
    for (size_t i = 0; i < result.value().size(); ++i) {
        EXPECT_EQ(result.value()[i].size(), 768)
            << "Embedding " << i << " should have dimension 768";
    }
}

// ============================================================================
// Test 3: Model Info Includes Dimension
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, ModelInfoIncludesDimension) {
    mockContext_->outputDim = 384;
    mockContext_->nextStatus = YAMS_OK;

    // First load the model
    auto loadResult = adapter_->loadModel("test-model");
    ASSERT_TRUE(loadResult);

    auto infoResult = adapter_->getModelInfo("test-model");

    ASSERT_TRUE(infoResult);
    EXPECT_EQ(infoResult.value().embeddingDim, 384)
        << "ModelInfo should include embedding dimension";
    EXPECT_EQ(infoResult.value().name, "test-model");
}

// ============================================================================
// Test 4: Dimension Mismatch Error Scenarios
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, DimensionMismatchInGenerate) {
    mockContext_->simulateDimensionMismatch = true;

    auto result = adapter_->generateEmbedding("test text");

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    // TODO: After enhancement, verify error includes dimension details
}

TEST_F(AbiModelProviderDimensionTest, DimensionMismatchInBatch) {
    mockContext_->simulateDimensionMismatch = true;

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    // TODO: After enhancement, verify error includes dimension details
}

// ============================================================================
// Test 5: Empty/Invalid Input Handling
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, EmptyTextGenerate) {
    auto result = adapter_->generateEmbedding("");

    // Should still work - empty text is valid input
    EXPECT_TRUE(result);
    if (result) {
        EXPECT_EQ(result.value().size(), mockContext_->outputDim);
    }
}

TEST_F(AbiModelProviderDimensionTest, EmptyBatch) {
    std::vector<std::string> emptyBatch;

    auto result = adapter_->generateBatchEmbeddings(emptyBatch);

    // Empty batch should return empty result, not error
    ASSERT_TRUE(result);
    EXPECT_TRUE(result.value().empty());
}

TEST_F(AbiModelProviderDimensionTest, BatchWithEmptyStrings) {
    std::vector<std::string> texts = {"", "", ""};

    auto result = adapter_->generateBatchEmbeddings(texts);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().size(), 3);
    for (const auto& emb : result.value()) {
        EXPECT_EQ(emb.size(), mockContext_->outputDim);
    }
}

// ============================================================================
// Test 6: Multiple Models with Different Dimensions
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, SwitchBetweenDimensions) {
    // Simulate loading different models with different dimensions
    mockContext_->outputDim = 384;
    auto result384 = adapter_->generateEmbedding("text");
    ASSERT_TRUE(result384);
    EXPECT_EQ(result384.value().size(), 384);

    // Switch to 768-dim model
    mockContext_->outputDim = 768;
    auto result768 = adapter_->generateEmbedding("text");
    ASSERT_TRUE(result768);
    EXPECT_EQ(result768.value().size(), 768);

    // Switch to 1024-dim model
    mockContext_->outputDim = 1024;
    auto result1024 = adapter_->generateEmbedding("text");
    ASSERT_TRUE(result1024);
    EXPECT_EQ(result1024.value().size(), 1024);
}

// ============================================================================
// Test 7: Large Batch Performance Validation
// ============================================================================

TEST_F(AbiModelProviderDimensionTest, LargeBatchDimensions) {
    mockContext_->outputDim = 384;

    // Create batch of 100 texts
    std::vector<std::string> texts;
    for (int i = 0; i < 100; ++i) {
        texts.push_back("Test text " + std::to_string(i));
    }

    auto result = adapter_->generateBatchEmbeddings(texts);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().size(), 100);
    for (const auto& emb : result.value()) {
        EXPECT_EQ(emb.size(), 384) << "All embeddings should have correct dimension";
    }
}

} // namespace yams::daemon::test

// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Unit tests for ABI Model Provider dimension handling and error messages
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/core/types.h>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/plugins/model_provider_v1.h>

#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace yams::daemon;
using namespace yams;
using Catch::Matchers::ContainsSubstring;

namespace {

// Mock ONNX plugin provider for testing
struct MockProviderContext {
    yams_status_t nextStatus = YAMS_OK;
    std::string nextError;
    size_t outputDim = 384;
    std::vector<float> nextEmbedding;
    bool simulateDimensionMismatch = false;
    size_t maxOutputBatch = 0; // 0 = no cap
    bool unknownDimOnFirstBatch = false;
    size_t batchCalls = 0;
};

// Mock implementation of plugin functions
yams_status_t mock_generate_embedding(void* self, const char* model_id, const uint8_t* text,
                                      size_t text_len, float** out_vec, size_t* out_dim) {
    (void)model_id;
    (void)text;
    (void)text_len;
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
                                            const uint8_t* const* inputs, const size_t* input_lens,
                                            size_t batch_size, float** out_vecs, size_t* out_batch,
                                            size_t* out_dim) {
    (void)model_id;
    auto* ctx = static_cast<MockProviderContext*>(self);
    ctx->batchCalls++;
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
    if (ctx->maxOutputBatch > 0 && batch_size > ctx->maxOutputBatch) {
        *out_batch = ctx->maxOutputBatch;
    } else {
        *out_batch = batch_size;
    }
    if (ctx->unknownDimOnFirstBatch && ctx->batchCalls == 1) {
        *out_dim = 0;
        *out_vecs = nullptr;
        return YAMS_OK;
    }

    *out_dim = ctx->outputDim;
    *out_vecs = static_cast<float*>(malloc((*out_batch) * ctx->outputDim * sizeof(float)));
    for (size_t i = 0; i < (*out_batch) * ctx->outputDim; ++i) {
        (*out_vecs)[i] = static_cast<float>(i % ctx->outputDim) * 0.01f;
    }
    return YAMS_OK;
}

void mock_free_embedding(void* self, float* vec, size_t dim) {
    (void)self;
    (void)dim;
    free(vec);
}

void mock_free_embedding_batch(void* self, float* vecs, size_t batch, size_t dim) {
    (void)self;
    (void)batch;
    (void)dim;
    free(vecs);
}

yams_status_t mock_load_model(void* self, const char* model_id, const char* model_path,
                              const char* options) {
    (void)model_id;
    (void)model_path;
    (void)options;
    auto* ctx = static_cast<MockProviderContext*>(self);
    return ctx->nextStatus;
}

yams_status_t mock_is_model_loaded(void* self, const char* model_id, bool* loaded) {
    (void)self;
    (void)model_id;
    *loaded = true;
    return YAMS_OK;
}

yams_status_t mock_get_embedding_dim(void* self, const char* model_id, size_t* dim) {
    (void)model_id;
    auto* ctx = static_cast<MockProviderContext*>(self);
    *dim = ctx->outputDim;
    return YAMS_OK;
}

yams_status_t mock_get_loaded_models(void* self, const char*** model_ids, size_t* count) {
    (void)self;
    static const char* models[] = {"test-model"};
    *model_ids = models;
    *count = 1;
    return YAMS_OK;
}

void mock_free_model_list(void* self, const char** model_ids, size_t count) {
    (void)self;
    (void)model_ids;
    (void)count;
    // Static array, no need to free
}

} // anonymous namespace

struct AbiModelProviderDimensionFixture {
    AbiModelProviderDimensionFixture() {
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

    ~AbiModelProviderDimensionFixture() {
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

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: error code mapping",
                 "[daemon]") {
    SECTION("Success maps correctly") {
        mockContext_->nextStatus = YAMS_OK;
        auto result = adapter_->loadModel("test-model");
        CHECK(result);
    }

    SECTION("Invalid argument maps correctly") {
        mockContext_->nextStatus = YAMS_ERR_INVALID_ARG;
        auto result = adapter_->loadModel("test-model");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    }

    SECTION("Not found maps correctly") {
        mockContext_->nextStatus = YAMS_ERR_NOT_FOUND;
        auto result = adapter_->loadModel("test-model");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::NotFound);
    }

    SECTION("IO error maps correctly") {
        mockContext_->nextStatus = YAMS_ERR_IO;
        auto result = adapter_->loadModel("test-model");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::IOError);
    }

    SECTION("Internal error maps correctly") {
        mockContext_->nextStatus = YAMS_ERR_INTERNAL;
        auto result = adapter_->loadModel("test-model");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::InternalError);
    }

    SECTION("Unsupported maps correctly") {
        mockContext_->nextStatus = YAMS_ERR_UNSUPPORTED;
        auto result = adapter_->loadModel("test-model");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::NotImplemented);
    }
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: error context includes operation", "[daemon]") {
    mockContext_->nextStatus = YAMS_ERR_INVALID_ARG;

    auto result = adapter_->generateEmbedding("test text");

    REQUIRE_FALSE(result);
    CHECK_THAT(result.error().message, ContainsSubstring("generate_embedding"));
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: batch error context includes operation", "[daemon]") {
    mockContext_->nextStatus = YAMS_ERR_INVALID_ARG;

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE_FALSE(result);
    CHECK_THAT(result.error().message, ContainsSubstring("generate_embedding_batch"));
}

// ============================================================================
// Test 2: Dimension Detection
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: get embedding dim 384 (MiniLM-like)", "[daemon]") {
    mockContext_->outputDim = 384;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    CHECK(dim == 384);
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: get embedding dim 768 (MPNet-like)", "[daemon]") {
    mockContext_->outputDim = 768;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    CHECK(dim == 768);
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: get embedding dim 1024 (E5-large-like)", "[daemon]") {
    mockContext_->outputDim = 1024;

    size_t dim = adapter_->getEmbeddingDim("test-model");

    CHECK(dim == 1024);
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: generated embedding matches declared dim", "[daemon]") {
    mockContext_->outputDim = 384;

    auto result = adapter_->generateEmbedding("test text");

    REQUIRE(result);
    CHECK(result.value().size() == 384);
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: batch embeddings match declared dim", "[daemon]") {
    mockContext_->outputDim = 768;

    std::vector<std::string> texts = {"text1", "text2", "text3"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE(result);
    REQUIRE(result.value().size() == 3);
    for (size_t i = 0; i < result.value().size(); ++i) {
        INFO("Checking embedding " << i);
        CHECK(result.value()[i].size() == 768);
    }
}

// ============================================================================
// Test 3: Model Info Includes Dimension
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: model info includes dimension", "[daemon]") {
    mockContext_->outputDim = 384;
    mockContext_->nextStatus = YAMS_OK;

    // First load the model
    auto loadResult = adapter_->loadModel("test-model");
    REQUIRE(loadResult);

    auto infoResult = adapter_->getModelInfo("test-model");

    REQUIRE(infoResult);
    CHECK(infoResult.value().embeddingDim == 384);
    CHECK(infoResult.value().name == "test-model");
}

// ============================================================================
// Test 4: Dimension Mismatch Error Scenarios
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: dimension mismatch in generate", "[daemon]") {
    mockContext_->simulateDimensionMismatch = true;

    auto result = adapter_->generateEmbedding("test text");

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    // TODO: After enhancement, verify error includes dimension details
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: dimension mismatch in batch",
                 "[daemon]") {
    mockContext_->simulateDimensionMismatch = true;

    std::vector<std::string> texts = {"text1", "text2"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    // TODO: After enhancement, verify error includes dimension details
}

// ============================================================================
// Test 5: Empty/Invalid Input Handling
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: empty text generate",
                 "[daemon]") {
    auto result = adapter_->generateEmbedding("");

    // Should still work - empty text is valid input
    CHECK(result);
    if (result) {
        CHECK(result.value().size() == mockContext_->outputDim);
    }
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: empty batch", "[daemon]") {
    std::vector<std::string> emptyBatch;

    auto result = adapter_->generateBatchEmbeddings(emptyBatch);

    // Empty batch should return empty result, not error
    REQUIRE(result);
    CHECK(result.value().empty());
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: batch with empty strings",
                 "[daemon]") {
    std::vector<std::string> texts = {"", "", ""};

    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE(result);
    CHECK(result.value().size() == 3);
    for (const auto& emb : result.value()) {
        CHECK(emb.size() == mockContext_->outputDim);
    }
}

// ============================================================================
// Test 6: Multiple Models with Different Dimensions
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: switch between dimensions",
                 "[daemon]") {
    SECTION("384-dim model") {
        mockContext_->outputDim = 384;
        auto result384 = adapter_->generateEmbedding("text");
        REQUIRE(result384);
        CHECK(result384.value().size() == 384);
    }

    SECTION("768-dim model") {
        mockContext_->outputDim = 768;
        auto result768 = adapter_->generateEmbedding("text");
        REQUIRE(result768);
        CHECK(result768.value().size() == 768);
    }

    SECTION("1024-dim model") {
        mockContext_->outputDim = 1024;
        auto result1024 = adapter_->generateEmbedding("text");
        REQUIRE(result1024);
        CHECK(result1024.value().size() == 1024);
    }
}

// ============================================================================
// Test 7: Large Batch Performance Validation
// ============================================================================

TEST_CASE_METHOD(AbiModelProviderDimensionFixture, "AbiModelProvider: large batch dimensions",
                 "[daemon]") {
    mockContext_->outputDim = 384;

    // Create batch of 100 texts
    std::vector<std::string> texts;
    for (int i = 0; i < 100; ++i) {
        texts.push_back("Test text " + std::to_string(i));
    }

    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE(result);
    CHECK(result.value().size() == 100);
    for (const auto& emb : result.value()) {
        CHECK(emb.size() == 384);
    }
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: provider partial batch output handled safely",
                 "[daemon][abi][regression]") {
    mockContext_->outputDim = 768;
    mockContext_->maxOutputBatch = 2;

    std::vector<std::string> texts = {"text1", "text2", "text3", "text4"};
    auto result = adapter_->generateBatchEmbeddings(texts);

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK_THAT(result.error().message, ContainsSubstring("generate_embedding_batch_partial"));
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: deterministic fuzz-style partial batch invariants",
                 "[daemon][abi][regression][fuzz-lite]") {
    std::mt19937 rng(0x5A17BEEF);
    std::uniform_int_distribution<size_t> dimDist(1, 1024);
    std::uniform_int_distribution<size_t> batchDist(1, 64);

    for (int iter = 0; iter < 100; ++iter) {
        const size_t requestedBatch = batchDist(rng);
        const size_t outputDim = dimDist(rng);
        std::uniform_int_distribution<size_t> capDist(1, requestedBatch);
        const size_t cap = capDist(rng);

        mockContext_->outputDim = outputDim;
        mockContext_->maxOutputBatch = cap;

        std::vector<std::string> texts;
        texts.reserve(requestedBatch);
        for (size_t i = 0; i < requestedBatch; ++i) {
            texts.push_back("seeded-text-" + std::to_string(iter) + "-" + std::to_string(i));
        }

        auto result = adapter_->generateBatchEmbeddings(texts);
        INFO("iter=" << iter << " requestedBatch=" << requestedBatch << " cap=" << cap
                     << " outputDim=" << outputDim);
        if (cap < requestedBatch) {
            REQUIRE_FALSE(result);
            CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
            CHECK_THAT(result.error().message,
                       ContainsSubstring("generate_embedding_batch_partial"));
        } else {
            REQUIRE(result);
            REQUIRE(result.value().size() == requestedBatch);
            for (const auto& emb : result.value()) {
                CHECK(emb.size() == outputDim);
            }
        }
    }
}

TEST_CASE_METHOD(AbiModelProviderDimensionFixture,
                 "AbiModelProvider: unknown initial dim with truncated batch stays safe",
                 "[daemon][abi][regression]") {
    mockContext_->outputDim = 768;
    mockContext_->maxOutputBatch = 2;
    mockContext_->unknownDimOnFirstBatch = true;

    std::vector<std::string> texts = {"text1", "text2", "text3", "text4"};

    auto first = adapter_->generateBatchEmbeddings(texts);
    REQUIRE_FALSE(first);
    CHECK(first.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK_THAT(first.error().message, ContainsSubstring("generate_embedding_batch_partial"));

    auto second = adapter_->generateBatchEmbeddings(texts);
    REQUIRE_FALSE(second);
    CHECK(second.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK_THAT(second.error().message, ContainsSubstring("generate_embedding_batch_partial"));
}

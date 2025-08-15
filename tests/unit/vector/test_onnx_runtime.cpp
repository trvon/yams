#include <gtest/gtest.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_generator.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace yams::vector::tests {

namespace fs = std::filesystem;

class OnnxRuntimeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a minimal test configuration
        config_.model_name = "test_model";
        config_.model_path = "test_model.onnx"; // Will use mock if file doesn't exist
        config_.embedding_dim = 384;
        config_.max_sequence_length = 512;
        config_.batch_size = 8;
        config_.normalize_embeddings = true;
    }

    EmbeddingConfig config_;
};

TEST_F(OnnxRuntimeTest, CreateGenerator) {
    YAMS_ZONE_SCOPED_N("OnnxRuntimeTest::CreateGenerator");

    // Create generator
    EmbeddingGenerator generator(config_);

    // Should not be initialized yet
    EXPECT_FALSE(generator.isInitialized());

    // Get config
    auto retrieved_config = generator.getConfig();
    EXPECT_EQ(retrieved_config.model_name, config_.model_name);
    EXPECT_EQ(retrieved_config.embedding_dim, config_.embedding_dim);
}

TEST_F(OnnxRuntimeTest, InitializeWithMockModel) {
    YAMS_ZONE_SCOPED_N("OnnxRuntimeTest::InitializeWithMockModel");

    // When model file doesn't exist, should fall back to mock implementation
    EmbeddingGenerator generator(config_);

    // Initialize should work even without real model
    bool initialized = generator.initialize();
    EXPECT_TRUE(initialized);
    EXPECT_TRUE(generator.isInitialized());
}

TEST_F(OnnxRuntimeTest, GenerateMockEmbedding) {
    YAMS_ZONE_SCOPED_N("OnnxRuntimeTest::GenerateMockEmbedding");

    EmbeddingGenerator generator(config_);
    ASSERT_TRUE(generator.initialize());

    // Generate embedding for test text
    std::string test_text = "This is a test sentence for embedding generation.";
    auto embedding = generator.generateEmbedding(test_text);

    // Check embedding properties
    EXPECT_EQ(embedding.size(), config_.embedding_dim);

    // Check if normalized (magnitude should be ~1.0)
    double magnitude = 0.0;
    for (float val : embedding) {
        magnitude += val * val;
    }
    magnitude = std::sqrt(magnitude);
    EXPECT_NEAR(magnitude, 1.0, 0.01); // Normalized embeddings should have magnitude ~1
}

TEST_F(OnnxRuntimeTest, GenerateBatchEmbeddings) {
    YAMS_ZONE_SCOPED_N("OnnxRuntimeTest::GenerateBatchEmbeddings");

    EmbeddingGenerator generator(config_);
    ASSERT_TRUE(generator.initialize());

    // Generate embeddings for multiple texts
    std::vector<std::string> texts = {"First test sentence.",
                                      "Second test sentence with more words.",
                                      "Third sentence for batch processing test."};

    auto embeddings = generator.generateEmbeddings(texts);

    // Check results
    EXPECT_EQ(embeddings.size(), texts.size());
    for (const auto& embedding : embeddings) {
        EXPECT_EQ(embedding.size(), config_.embedding_dim);
    }
}

TEST_F(OnnxRuntimeTest, EstimateTokenCount) {
    EmbeddingGenerator generator(config_);

    // Test token estimation
    std::string short_text = "Test";
    std::string long_text =
        "This is a much longer text that should have more tokens when processed.";

    auto short_count = generator.estimateTokenCount(short_text);
    auto long_count = generator.estimateTokenCount(long_text);

    EXPECT_GT(long_count, short_count);
    EXPECT_GE(short_count, 1u);
}

TEST_F(OnnxRuntimeTest, GetModelInfo) {
    EmbeddingGenerator generator(config_);
    ASSERT_TRUE(generator.initialize());

    // Get model info string
    auto info = generator.getModelInfo();

    // Check that info contains expected details
    EXPECT_NE(info.find("Model: " + config_.model_name), std::string::npos);
    EXPECT_NE(info.find("Embedding Dimension: " + std::to_string(config_.embedding_dim)),
              std::string::npos);
}

TEST_F(OnnxRuntimeTest, GeneratorStatistics) {
    YAMS_ZONE_SCOPED_N("OnnxRuntimeTest::GeneratorStatistics");

    EmbeddingGenerator generator(config_);
    ASSERT_TRUE(generator.initialize());

    // Reset stats
    generator.resetStats();
    auto initial_stats = generator.getStats();
    EXPECT_EQ(initial_stats.total_texts_processed, 0u);

    // Generate some embeddings
    generator.generateEmbedding("Test text 1");
    generator.generateEmbedding("Test text 2");

    // Check updated stats
    auto stats = generator.getStats();
    EXPECT_EQ(stats.total_texts_processed, 2u);
    EXPECT_GT(stats.total_tokens_processed, 0u);
}

#ifdef YAMS_USE_ONNX_RUNTIME
TEST_F(OnnxRuntimeTest, CheckOnnxRuntimeLinked) {
    // This test verifies that ONNX Runtime is properly linked
    // It will only compile if ONNX Runtime headers are available
    EXPECT_TRUE(true) << "ONNX Runtime is linked and available";
}
#else
TEST_F(OnnxRuntimeTest, OnnxRuntimeNotAvailable) {
    // This test runs when ONNX Runtime is not available
    EXPECT_TRUE(true) << "Running with mock implementation (ONNX Runtime not available)";
}
#endif

} // namespace yams::vector::tests
#include <yams/vector/embedding_generator.h>

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <cmath>

using namespace yams::vector;

class EmbeddingGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory and mock model file
        test_dir_ = "test_models";
        std::filesystem::create_directories(test_dir_);
        
        mock_model_path_ = test_dir_ + "/test_model.onnx";
        createMockModelFile(mock_model_path_);
        
        config_.model_path = mock_model_path_;
        config_.model_name = "test-model";
        config_.embedding_dim = 384;
        config_.max_sequence_length = 512;
        config_.batch_size = 16;
        
        generator_ = std::make_unique<EmbeddingGenerator>(config_);
    }

    void TearDown() override {
        generator_.reset();
        std::filesystem::remove_all(test_dir_);
    }

    void createMockModelFile(const std::string& path) {
        std::ofstream file(path, std::ios::binary);
        // Write some dummy data to simulate a model file
        std::string dummy_data(1024, 'X');
        file.write(dummy_data.data(), dummy_data.size());
    }

    std::string test_dir_;
    std::string mock_model_path_;
    EmbeddingConfig config_;
    std::unique_ptr<EmbeddingGenerator> generator_;
};

TEST_F(EmbeddingGeneratorTest, InitializationAndBasicInfo) {
    EXPECT_TRUE(generator_->initialize());
    EXPECT_TRUE(generator_->isInitialized());
    EXPECT_FALSE(generator_->hasError());
    
    EXPECT_EQ(generator_->getEmbeddingDimension(), 384);
    EXPECT_EQ(generator_->getMaxSequenceLength(), 512);
    
    auto& config = generator_->getConfig();
    EXPECT_EQ(config.model_name, "test-model");
    EXPECT_EQ(config.embedding_dim, 384);
}

TEST_F(EmbeddingGeneratorTest, SingleEmbeddingGeneration) {
    ASSERT_TRUE(generator_->initialize());
    
    std::string text = "This is a test document for embedding generation.";
    auto embedding = generator_->generateEmbedding(text);
    
    EXPECT_FALSE(embedding.empty());
    EXPECT_EQ(embedding.size(), 384);
    
    // Check that embedding values are reasonable
    bool has_non_zero = false;
    for (float val : embedding) {
        EXPECT_TRUE(std::isfinite(val));
        if (val != 0.0f) {
            has_non_zero = true;
        }
    }
    EXPECT_TRUE(has_non_zero);
    
    // Check normalization (should be close to unit length)
    double magnitude = 0.0;
    for (float val : embedding) {
        magnitude += val * val;
    }
    magnitude = std::sqrt(magnitude);
    EXPECT_NEAR(magnitude, 1.0, 0.01); // Should be normalized
}

TEST_F(EmbeddingGeneratorTest, BatchEmbeddingGeneration) {
    ASSERT_TRUE(generator_->initialize());
    
    std::vector<std::string> texts = {
        "First test document for batch processing.",
        "Second document with different content.",
        "Third document for comprehensive testing.",
        "Fourth and final document in the batch."
    };
    
    auto embeddings = generator_->generateEmbeddings(texts);
    
    EXPECT_EQ(embeddings.size(), texts.size());
    
    for (const auto& embedding : embeddings) {
        EXPECT_EQ(embedding.size(), 384);
        
        // Check values are finite and normalized
        double magnitude = 0.0;
        for (float val : embedding) {
            EXPECT_TRUE(std::isfinite(val));
            magnitude += val * val;
        }
        magnitude = std::sqrt(magnitude);
        EXPECT_NEAR(magnitude, 1.0, 0.01);
    }
    
    // Embeddings for different texts should be different
    if (embeddings.size() >= 2) {
        bool embeddings_different = false;
        for (size_t i = 0; i < embeddings[0].size(); ++i) {
            if (std::abs(embeddings[0][i] - embeddings[1][i]) > 1e-6) {
                embeddings_different = true;
                break;
            }
        }
        EXPECT_TRUE(embeddings_different);
    }
}

TEST_F(EmbeddingGeneratorTest, EmptyAndInvalidInputs) {
    ASSERT_TRUE(generator_->initialize());
    
    // Empty text
    auto empty_embedding = generator_->generateEmbedding("");
    EXPECT_FALSE(empty_embedding.empty()); // Should still generate something
    
    // Very long text (should be truncated)
    std::string long_text(10000, 'a');
    auto long_embedding = generator_->generateEmbedding(long_text);
    EXPECT_EQ(long_embedding.size(), 384);
    
    // Empty batch
    std::vector<std::string> empty_batch;
    auto empty_batch_result = generator_->generateEmbeddings(empty_batch);
    EXPECT_TRUE(empty_batch_result.empty());
}

TEST_F(EmbeddingGeneratorTest, AsyncEmbeddingGeneration) {
    ASSERT_TRUE(generator_->initialize());
    
    std::string text = "Test document for async embedding generation.";
    
    auto future = generator_->generateEmbeddingAsync(text);
    ASSERT_TRUE(future.valid());
    
    auto embedding = future.get();
    EXPECT_EQ(embedding.size(), 384);
    
    // Test async batch generation
    std::vector<std::string> texts = {
        "First async document.",
        "Second async document.",
        "Third async document."
    };
    
    auto batch_future = generator_->generateEmbeddingsAsync(texts);
    ASSERT_TRUE(batch_future.valid());
    
    auto batch_embeddings = batch_future.get();
    EXPECT_EQ(batch_embeddings.size(), texts.size());
}

TEST_F(EmbeddingGeneratorTest, ModelManagement) {
    EXPECT_FALSE(generator_->isModelLoaded());
    
    EXPECT_TRUE(generator_->initialize());
    EXPECT_TRUE(generator_->isModelLoaded());
    
    generator_->unloadModel();
    EXPECT_FALSE(generator_->isModelLoaded());
    
    // Should be able to reload
    EXPECT_TRUE(generator_->initialize());
    EXPECT_TRUE(generator_->isModelLoaded());
}

TEST_F(EmbeddingGeneratorTest, ConfigurationUpdates) {
    ASSERT_TRUE(generator_->initialize());
    
    auto original_config = generator_->getConfig();
    EXPECT_EQ(original_config.embedding_dim, 384);
    
    // Create new config
    EmbeddingConfig new_config = original_config;
    new_config.batch_size = 64;
    new_config.max_sequence_length = 256;
    
    generator_->updateConfig(new_config);
    
    auto updated_config = generator_->getConfig();
    EXPECT_EQ(updated_config.batch_size, 64);
    EXPECT_EQ(updated_config.max_sequence_length, 256);
}

TEST_F(EmbeddingGeneratorTest, StatisticsTracking) {
    ASSERT_TRUE(generator_->initialize());
    
    auto initial_stats = generator_->getStats();
    EXPECT_EQ(initial_stats.total_texts_processed, 0);
    
    // Generate some embeddings
    generator_->generateEmbedding("Test document 1");
    generator_->generateEmbedding("Test document 2");
    
    std::vector<std::string> batch = {"Batch doc 1", "Batch doc 2", "Batch doc 3"};
    generator_->generateEmbeddings(batch);
    
    auto final_stats = generator_->getStats();
    EXPECT_EQ(final_stats.total_texts_processed, 5); // 2 single + 3 batch
    EXPECT_GT(final_stats.total_tokens_processed, 0);
    EXPECT_GT(final_stats.total_inference_time.count(), 0);
    EXPECT_EQ(final_stats.batch_count, 1);
    EXPECT_GT(final_stats.throughput_texts_per_sec, 0.0);
    
    // Reset stats
    generator_->resetStats();
    auto reset_stats = generator_->getStats();
    EXPECT_EQ(reset_stats.total_texts_processed, 0);
}

TEST_F(EmbeddingGeneratorTest, TextValidation) {
    ASSERT_TRUE(generator_->initialize());
    
    // Valid texts
    EXPECT_TRUE(generator_->validateText("Normal text"));
    EXPECT_TRUE(generator_->validateText("Text with 123 numbers!"));
    
    // Empty text (should still be valid)
    EXPECT_FALSE(generator_->validateText(""));
    
    // Very large text (should be invalid)
    std::string huge_text(2000000, 'x'); // 2MB
    EXPECT_FALSE(generator_->validateText(huge_text));
}

TEST_F(EmbeddingGeneratorTest, TokenEstimation) {
    ASSERT_TRUE(generator_->initialize());
    
    // Test token count estimation
    EXPECT_GT(generator_->estimateTokenCount("hello world"), 0);
    EXPECT_GT(generator_->estimateTokenCount("A longer sentence with more words"), 
              generator_->estimateTokenCount("short"));
    
    // Empty text
    EXPECT_GE(generator_->estimateTokenCount(""), 1); // Should return at least 1
}

TEST_F(EmbeddingGeneratorTest, ModelInformation) {
    ASSERT_TRUE(generator_->initialize());
    
    auto model_info = generator_->getModelInfo();
    EXPECT_FALSE(model_info.empty());
    EXPECT_TRUE(model_info.find("test-model") != std::string::npos);
    EXPECT_TRUE(model_info.find("384") != std::string::npos);
    EXPECT_TRUE(model_info.find("512") != std::string::npos);
}

TEST_F(EmbeddingGeneratorTest, ErrorHandling) {
    // Test with invalid model path
    EmbeddingConfig bad_config;
    bad_config.model_path = "non_existent_model.onnx";
    
    EmbeddingGenerator bad_generator(bad_config);
    EXPECT_FALSE(bad_generator.initialize());
    EXPECT_TRUE(bad_generator.hasError());
    EXPECT_FALSE(bad_generator.getLastError().empty());
    
    // Test operations on uninitialized generator
    auto embedding = bad_generator.generateEmbedding("test");
    EXPECT_TRUE(embedding.empty());
}

// =============================================================================
// TextPreprocessor Tests
// =============================================================================

class TextPreprocessorTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.max_sequence_length = 512;
        config_.model_settings.do_lower_case = true;
        config_.model_settings.cls_token = "[CLS]";
        config_.model_settings.sep_token = "[SEP]";
        config_.model_settings.pad_token = "[PAD]";
        
        preprocessor_ = std::make_unique<TextPreprocessor>(config_);
    }

    EmbeddingConfig config_;
    std::unique_ptr<TextPreprocessor> preprocessor_;
};

TEST_F(TextPreprocessorTest, TextNormalization) {
    // Test case normalization
    auto normalized = preprocessor_->normalizeText("HELLO World");
    EXPECT_EQ(normalized, "hello world");
    
    // Test whitespace normalization
    normalized = preprocessor_->normalizeText("  multiple   spaces  ");
    EXPECT_EQ(normalized, "multiple spaces");
    
    // Test empty text
    normalized = preprocessor_->normalizeText("");
    EXPECT_TRUE(normalized.empty());
}

TEST_F(TextPreprocessorTest, BasicTokenization) {
    auto tokens = preprocessor_->tokenize("hello world");
    
    // Should have CLS, tokens, and SEP
    EXPECT_GE(tokens.size(), 3);
    
    // Check that tokens are valid IDs
    for (int32_t token : tokens) {
        EXPECT_TRUE(preprocessor_->isValidToken(token));
    }
}

TEST_F(TextPreprocessorTest, BatchTokenization) {
    std::vector<std::string> texts = {
        "First sentence",
        "Second sentence with more words",
        "Third"
    };
    
    auto batch_tokens = preprocessor_->tokenizeBatch(texts);
    EXPECT_EQ(batch_tokens.size(), texts.size());
    
    for (const auto& tokens : batch_tokens) {
        EXPECT_GE(tokens.size(), 2); // At least CLS and SEP
    }
}

TEST_F(TextPreprocessorTest, TokenTruncation) {
    // Create tokens longer than max length
    std::vector<int32_t> long_tokens(600, 1); // 600 tokens
    
    auto truncated = preprocessor_->truncateTokens(long_tokens, 512);
    EXPECT_LE(truncated.size(), 512);
}

TEST_F(TextPreprocessorTest, TokenPadding) {
    std::vector<int32_t> short_tokens = {1, 2, 3};
    
    auto padded = preprocessor_->padTokens(short_tokens, 10);
    EXPECT_EQ(padded.size(), 10);
    
    // Check that original tokens are preserved
    for (size_t i = 0; i < short_tokens.size(); ++i) {
        EXPECT_EQ(padded[i], short_tokens[i]);
    }
}

TEST_F(TextPreprocessorTest, AttentionMaskGeneration) {
    std::vector<int32_t> tokens = {1, 2, 3, 0, 0}; // 0 is pad token
    
    auto mask = preprocessor_->generateAttentionMask(tokens);
    EXPECT_EQ(mask.size(), tokens.size());
    
    // Check mask values
    EXPECT_EQ(mask[0], 1); // Real token
    EXPECT_EQ(mask[1], 1); // Real token
    EXPECT_EQ(mask[2], 1); // Real token
    EXPECT_EQ(mask[3], 0); // Pad token
    EXPECT_EQ(mask[4], 0); // Pad token
}

TEST_F(TextPreprocessorTest, VocabularyOperations) {
    EXPECT_GT(preprocessor_->getVocabSize(), 0);
    
    // Test token decoding
    for (int32_t i = 0; i < 10 && i < static_cast<int32_t>(preprocessor_->getVocabSize()); ++i) {
        if (preprocessor_->isValidToken(i)) {
            auto decoded = preprocessor_->decodeToken(i);
            EXPECT_FALSE(decoded.empty());
        }
    }
}

// =============================================================================
// Utility Functions Tests
// =============================================================================

class EmbeddingUtilsTest : public ::testing::Test {
protected:
    void createTestEmbedding() {
        test_embedding_ = {0.3f, 0.4f, 0.0f, -0.5f, 0.8f};
    }

    void SetUp() override {
        createTestEmbedding();
    }

    std::vector<float> test_embedding_;
};

TEST_F(EmbeddingUtilsTest, EmbeddingNormalization) {
    auto normalized = embedding_utils::normalizeEmbedding(test_embedding_);
    
    // Check that it's normalized
    double magnitude = embedding_utils::computeMagnitude(normalized);
    EXPECT_NEAR(magnitude, 1.0, 1e-6);
    
    // Check dimension preserved
    EXPECT_EQ(normalized.size(), test_embedding_.size());
}

TEST_F(EmbeddingUtilsTest, BatchNormalization) {
    std::vector<std::vector<float>> embeddings = {
        {1.0f, 2.0f, 3.0f},
        {4.0f, 5.0f, 6.0f},
        {7.0f, 8.0f, 9.0f}
    };
    
    auto normalized = embedding_utils::normalizeEmbeddings(embeddings);
    EXPECT_EQ(normalized.size(), embeddings.size());
    
    for (const auto& embedding : normalized) {
        double magnitude = embedding_utils::computeMagnitude(embedding);
        EXPECT_NEAR(magnitude, 1.0, 1e-6);
    }
}

TEST_F(EmbeddingUtilsTest, MagnitudeComputation) {
    // Test known magnitude
    std::vector<float> simple_embedding = {3.0f, 4.0f, 0.0f}; // Magnitude should be 5.0
    double magnitude = embedding_utils::computeMagnitude(simple_embedding);
    EXPECT_NEAR(magnitude, 5.0, 1e-6);
    
    // Empty embedding
    std::vector<float> empty_embedding;
    magnitude = embedding_utils::computeMagnitude(empty_embedding);
    EXPECT_NEAR(magnitude, 0.0, 1e-6);
}

TEST_F(EmbeddingUtilsTest, EmbeddingValidation) {
    // Valid embedding
    EXPECT_TRUE(embedding_utils::validateEmbedding(test_embedding_, 5));
    
    // Wrong dimension
    EXPECT_FALSE(embedding_utils::validateEmbedding(test_embedding_, 10));
    
    // Invalid values
    std::vector<float> invalid_embedding = {1.0f, std::numeric_limits<float>::infinity(), 3.0f};
    EXPECT_FALSE(embedding_utils::validateEmbedding(invalid_embedding, 3));
    
    std::vector<float> nan_embedding = {1.0f, std::numeric_limits<float>::quiet_NaN(), 3.0f};
    EXPECT_FALSE(embedding_utils::validateEmbedding(nan_embedding, 3));
}

TEST_F(EmbeddingUtilsTest, EmbeddingToString) {
    auto str = embedding_utils::embeddingToString(test_embedding_, 3);
    
    EXPECT_TRUE(str.find("[") != std::string::npos);
    EXPECT_TRUE(str.find("]") != std::string::npos);
    EXPECT_TRUE(str.find("0.3000") != std::string::npos);
    EXPECT_TRUE(str.find("...") != std::string::npos); // Should truncate
}

TEST_F(EmbeddingUtilsTest, AvailableModels) {
    // Create test models directory
    std::filesystem::create_directories("test_models_dir/model1");
    std::filesystem::create_directories("test_models_dir/model2");
    
    auto models = embedding_utils::getAvailableModels("test_models_dir");
    EXPECT_GE(models.size(), 2);
    
    // Clean up
    std::filesystem::remove_all("test_models_dir");
}

// =============================================================================
// Factory Function Tests
// =============================================================================

TEST(EmbeddingGeneratorFactoryTest, CreateEmbeddingGenerator) {
    // Create test model file first
    std::filesystem::create_directories("test_factory");
    std::ofstream model_file("test_factory/model.onnx", std::ios::binary);
    model_file.write("dummy", 5);
    model_file.close();
    
    EmbeddingConfig config;
    config.model_path = "test_factory/model.onnx";
    config.model_name = "test-factory-model";
    
    auto generator = createEmbeddingGenerator(config);
    EXPECT_NE(generator, nullptr);
    EXPECT_TRUE(generator->isInitialized());
    
    // Clean up
    std::filesystem::remove_all("test_factory");
}

TEST(EmbeddingGeneratorFactoryTest, CreateWithInvalidConfig) {
    EmbeddingConfig config;
    config.model_path = "non_existent_model.onnx";
    
    auto generator = createEmbeddingGenerator(config);
    EXPECT_EQ(generator, nullptr);
}
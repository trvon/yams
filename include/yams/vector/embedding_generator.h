#pragma once

#include <yams/core/types.h>

#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <future>

namespace yams::vector {

/**
 * Configuration for embedding generation
 */
struct EmbeddingConfig {
    std::string model_path = "models/all-MiniLM-L6-v2.onnx";
    std::string tokenizer_path = "models/tokenizer.json";
    size_t max_sequence_length = 512;
    size_t embedding_dim = 384;
    size_t batch_size = 32;
    bool normalize_embeddings = true;
    std::string model_name = "all-MiniLM-L6-v2";
    float padding_token_id = 0.0f;
    float unk_token_id = 1.0f;
    bool enable_gpu = false;
    int num_threads = -1;  // -1 for auto-detect
    
    // Version tracking
    std::string model_version = "1.0.0";      // Semantic version
    uint32_t embedding_schema_version = 1;     // Schema version
    bool track_content_hash = true;            // Track content changes
    
    // Model-specific settings
    struct ModelSettings {
        std::string vocab_file;
        bool do_lower_case = true;
        std::string cls_token = "[CLS]";
        std::string sep_token = "[SEP]";
        std::string unk_token = "[UNK]";
        std::string pad_token = "[PAD]";
        std::string mask_token = "[MASK]";
    } model_settings;
};

/**
 * Statistics for embedding generation performance
 */
struct GenerationStats {
    size_t total_texts_processed = 0;
    size_t total_tokens_processed = 0;
    std::chrono::milliseconds total_inference_time{0};
    std::chrono::milliseconds avg_inference_time{0};
    size_t batch_count = 0;
    size_t total_batches = 0;
    double throughput_texts_per_sec = 0.0;
    double throughput_tokens_per_sec = 0.0;

    void updateThroughput() {
        if (total_inference_time.count() > 0) {
            double seconds = total_inference_time.count() / 1000.0;
            throughput_texts_per_sec = total_texts_processed / seconds;
            throughput_tokens_per_sec = total_tokens_processed / seconds;
        }
    }
};

/**
 * Text preprocessing utilities
 */
class TextPreprocessor {
public:
    explicit TextPreprocessor(const EmbeddingConfig& config);
    ~TextPreprocessor();

    // Text normalization
    std::string normalizeText(const std::string& text);
    
    // Tokenization (basic implementation - can be extended with proper tokenizers)
    std::vector<int32_t> tokenize(const std::string& text);
    std::vector<std::vector<int32_t>> tokenizeBatch(const std::vector<std::string>& texts);
    
    // Token processing
    std::vector<int32_t> truncateTokens(const std::vector<int32_t>& tokens, size_t max_length);
    std::vector<int32_t> padTokens(const std::vector<int32_t>& tokens, size_t target_length);
    
    // Attention mask generation
    std::vector<int32_t> generateAttentionMask(const std::vector<int32_t>& tokens);
    
    // Utility functions
    size_t getVocabSize() const;
    bool isValidToken(int32_t token_id) const;
    std::string decodeToken(int32_t token_id) const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * ONNX model management for embeddings
 */
class ModelManager {
public:
    ModelManager();
    ~ModelManager();

    // Non-copyable but movable
    ModelManager(const ModelManager&) = delete;
    ModelManager& operator=(const ModelManager&) = delete;
    ModelManager(ModelManager&&) noexcept;
    ModelManager& operator=(ModelManager&&) noexcept;

    // Model loading and management
    bool loadModel(const std::string& model_name, const std::string& model_path);
    bool isModelLoaded(const std::string& model_name) const;
    void unloadModel(const std::string& model_name);
    void clearCache();
    
    // Model inference
    std::vector<std::vector<float>> runInference(
        const std::string& model_name,
        const std::vector<std::vector<int32_t>>& input_tokens,
        const std::vector<std::vector<int32_t>>& attention_masks = {});
    
    // Model information
    size_t getModelEmbeddingDim(const std::string& model_name) const;
    size_t getModelMaxLength(const std::string& model_name) const;
    
    // Performance and monitoring
    struct ModelStats {
        size_t inference_count = 0;
        std::chrono::milliseconds total_inference_time{0};
        size_t model_size_bytes = 0;
        size_t memory_usage_bytes = 0;
    };
    
    ModelStats getModelStats(const std::string& model_name) const;
    std::vector<std::string> getLoadedModels() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Main embedding generator class
 */
class EmbeddingGenerator {
public:
    explicit EmbeddingGenerator(const EmbeddingConfig& config = {});
    ~EmbeddingGenerator();

    // Non-copyable but movable
    EmbeddingGenerator(const EmbeddingGenerator&) = delete;
    EmbeddingGenerator& operator=(const EmbeddingGenerator&) = delete;
    EmbeddingGenerator(EmbeddingGenerator&&) noexcept;
    EmbeddingGenerator& operator=(EmbeddingGenerator&&) noexcept;

    // Initialization
    bool initialize();
    bool isInitialized() const;
    void shutdown();

    // Single text embedding
    std::vector<float> generateEmbedding(const std::string& text);
    
    // Batch embedding generation (synchronous)
    std::vector<std::vector<float>> generateEmbeddings(const std::vector<std::string>& texts);
    
    // Asynchronous embedding generation
    std::future<std::vector<float>> generateEmbeddingAsync(const std::string& text);
    std::future<std::vector<std::vector<float>>> generateEmbeddingsAsync(const std::vector<std::string>& texts);
    
    // Model management
    bool loadModel(const std::string& model_path);
    bool switchModel(const std::string& model_name, const EmbeddingConfig& new_config);
    bool isModelLoaded() const;
    void unloadModel();
    
    // Configuration and information
    size_t getEmbeddingDimension() const;
    size_t getMaxSequenceLength() const;
    const EmbeddingConfig& getConfig() const;
    void updateConfig(const EmbeddingConfig& new_config);
    
    // Statistics and monitoring
    GenerationStats getStats() const;
    void resetStats();
    
    // Validation and utility
    bool validateText(const std::string& text) const;
    size_t estimateTokenCount(const std::string& text) const;
    std::string getModelInfo() const;
    
    // Error handling
    std::string getLastError() const;
    bool hasError() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Factory function for creating embedding generators
 */
std::unique_ptr<EmbeddingGenerator> createEmbeddingGenerator(const EmbeddingConfig& config = {});

/**
 * Utility functions for embedding operations
 */
namespace embedding_utils {
    /**
     * Normalize embeddings to unit length
     */
    std::vector<float> normalizeEmbedding(const std::vector<float>& embedding);
    
    /**
     * Normalize batch of embeddings
     */
    std::vector<std::vector<float>> normalizeEmbeddings(const std::vector<std::vector<float>>& embeddings);
    
    /**
     * Compute embedding magnitude
     */
    double computeMagnitude(const std::vector<float>& embedding);
    
    /**
     * Validate embedding dimensions and values
     */
    bool validateEmbedding(const std::vector<float>& embedding, size_t expected_dim);
    
    /**
     * Convert embedding to string representation (for debugging)
     */
    std::string embeddingToString(const std::vector<float>& embedding, size_t max_values = 10);
    
    /**
     * Load model configuration from JSON file
     */
    EmbeddingConfig loadConfigFromFile(const std::string& config_path);
    
    /**
     * Save model configuration to JSON file
     */
    bool saveConfigToFile(const EmbeddingConfig& config, const std::string& config_path);
    
    /**
     * Get available models in models directory
     */
    std::vector<std::string> getAvailableModels(const std::string& models_dir = "models");
    
    /**
     * Download model files (placeholder for future implementation)
     */
    bool downloadModel(const std::string& model_name, const std::string& target_dir);
}

} // namespace yams::vector
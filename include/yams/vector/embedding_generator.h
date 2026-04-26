#pragma once

#include <yams/core/types.h>

#include <atomic>
#include <chrono>
#include <concepts>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace yams::vector {

/**
 * Configuration for embedding generation
 */
struct EmbeddingConfig {
    // Backend selection
    enum class Backend {
        Daemon,      // Use daemon service / daemon-selected model provider
        Hybrid,      // Legacy alias; treated as daemon-only
        Simeon,      // In-process SIMD model-free (third_party/simeon)
        OnnxRuntime, // ONNX Runtime embeddings via the daemon/plugin model provider path
    };
    Backend backend = Backend::Daemon; // Daemon-only embedding path

    // Model configuration (used by daemon and fallback/mock providers)
    std::string model_name = "all-MiniLM-L6-v2";
    size_t max_sequence_length = 512;
    size_t embedding_dim = 1024;
    size_t batch_size = 32;
    bool normalize_embeddings = true;
    float padding_token_id = 0.0f;
    float unk_token_id = 1.0f;

    // Model/runtime settings retained for compatibility with provider adapters
    std::string model_path = "models/all-MiniLM-L6-v2.onnx";
    std::string tokenizer_path = "models/tokenizer.json";
    bool enable_gpu = false;
    int num_threads = -1;      // -1 for auto-detect
    int inter_op_threads = -1; // -1 for auto-detect

    // When true, force deterministic ONNX runtime: sequential execution,
    // IntraOpNumThreads=1. Throughput cost is absorbed by bench harnesses; set
    // from [embeddings].deterministic TOML / bench config — never from env.
    bool deterministic = false;

    // Daemon backend settings
    std::string daemon_socket; // Empty = auto-resolve based on runtime environment
    std::chrono::milliseconds daemon_timeout{5000};
    size_t daemon_max_retries = 3;
    bool daemon_auto_start = true;

    // Version tracking
    std::string model_version = "1.0.0";   // Semantic version
    uint32_t embedding_schema_version = 1; // Schema version
    bool track_content_hash = true;        // Track content changes

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
 * Using atomic counters for lock-free access
 */
struct GenerationStats {
    std::atomic<size_t> total_texts_processed{0};
    std::atomic<size_t> total_tokens_processed{0};
    std::atomic<std::chrono::milliseconds::rep> total_inference_time{0};
    std::atomic<std::chrono::milliseconds::rep> avg_inference_time{0};
    std::atomic<size_t> batch_count{0};
    std::atomic<size_t> total_batches{0};
    std::atomic<double> throughput_texts_per_sec{0.0};
    std::atomic<double> throughput_tokens_per_sec{0.0};

    // Default constructor
    GenerationStats() = default;

    // Copy constructor for non-atomic interface compatibility
    GenerationStats(const GenerationStats& other)
        : total_texts_processed(other.total_texts_processed.load()),
          total_tokens_processed(other.total_tokens_processed.load()),
          total_inference_time(other.total_inference_time.load()),
          avg_inference_time(other.avg_inference_time.load()),
          batch_count(other.batch_count.load()), total_batches(other.total_batches.load()),
          throughput_texts_per_sec(other.throughput_texts_per_sec.load()),
          throughput_tokens_per_sec(other.throughput_tokens_per_sec.load()) {}

    // Assignment operator for non-atomic interface compatibility
    GenerationStats& operator=(const GenerationStats& other) {
        if (this != &other) {
            total_texts_processed.store(other.total_texts_processed.load());
            total_tokens_processed.store(other.total_tokens_processed.load());
            total_inference_time.store(other.total_inference_time.load());
            avg_inference_time.store(other.avg_inference_time.load());
            batch_count.store(other.batch_count.load());
            total_batches.store(other.total_batches.load());
            throughput_texts_per_sec.store(other.throughput_texts_per_sec.load());
            throughput_tokens_per_sec.store(other.throughput_tokens_per_sec.load());
        }
        return *this;
    }

    void updateThroughput() {
        auto time_ms = total_inference_time.load();
        if (time_ms > 0) {
            double seconds = time_ms / 1000.0;
            auto texts = total_texts_processed.load();
            auto tokens = total_tokens_processed.load();
            throughput_texts_per_sec.store(texts / seconds);
            throughput_tokens_per_sec.store(tokens / seconds);
        }
    }
};

// Forward declarations for backend implementations
class IEmbeddingBackend;
class DaemonBackend;
class HybridBackend;

/**
 * C++20 Concept for embedding backends
 */
template <typename T>
concept EmbeddingBackend =
    requires(T& t, const std::string& text, std::span<const std::string> texts) {
        { t.initialize() } -> std::convertible_to<bool>;
        { t.shutdown() } -> std::same_as<void>;
        { t.isInitialized() } -> std::convertible_to<bool>;
        { t.generateEmbedding(text) } -> std::same_as<Result<std::vector<float>>>;
        { t.generateEmbeddings(texts) } -> std::same_as<Result<std::vector<std::vector<float>>>>;
        { t.getEmbeddingDimension() } -> std::convertible_to<size_t>;
        { t.getMaxSequenceLength() } -> std::convertible_to<size_t>;
        { t.getBackendName() } -> std::convertible_to<std::string>;
        { t.isAvailable() } -> std::convertible_to<bool>;
    };

/**
 * Abstract interface for embedding backends
 * This allows runtime polymorphism when needed
 */
class IEmbeddingBackend {
public:
    virtual ~IEmbeddingBackend() = default;

    virtual bool initialize() = 0;
    virtual void shutdown() = 0;
    virtual bool isInitialized() const = 0;

    virtual Result<std::vector<float>> generateEmbedding(const std::string& text) = 0;
    virtual Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) = 0;

    virtual size_t getEmbeddingDimension() const = 0;
    virtual size_t getMaxSequenceLength() const = 0;

    virtual std::string getBackendName() const = 0;
    virtual bool isAvailable() const = 0;
    virtual GenerationStats getStats() const = 0;
    virtual void resetStats() = 0;
};

/**
 * Main embedding generator class
 * Uses strategy pattern with runtime backend selection
 */
class EmbeddingGenerator {
public:
    explicit EmbeddingGenerator(const EmbeddingConfig& config = {});

    /**
     * Constructor with custom backend injection
     * Allows using a pre-configured IEmbeddingBackend instead of auto-selecting
     * based on config. The backend should already be initialized.
     */
    explicit EmbeddingGenerator(std::unique_ptr<IEmbeddingBackend> backend,
                                const EmbeddingConfig& config = {});

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
    std::future<std::vector<std::vector<float>>>
    generateEmbeddingsAsync(const std::vector<std::string>& texts);

    // Model management
    bool loadModel(const std::string& model_path);
    bool switchModel(const std::string& model_name, const EmbeddingConfig& new_config);
    bool isModelLoaded() const;
    void unloadModel();

    // Configuration and information
    size_t getEmbeddingDimension() const;
    size_t getMaxSequenceLength() const;
    const EmbeddingConfig& getConfig() const;
    // Backend identity for diagnostics
    std::string getBackendName() const;
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
std::vector<std::vector<float>>
normalizeEmbeddings(const std::vector<std::vector<float>>& embeddings);

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
} // namespace embedding_utils

} // namespace yams::vector

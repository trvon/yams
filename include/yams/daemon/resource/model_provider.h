#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {
// Forward declaration
struct ModelPoolConfig;

// ============================================================================
// Model Information
// ============================================================================

struct ModelInfo {
    std::string name;
    std::string path;
    size_t embeddingDim = 0;
    size_t maxSequenceLength = 0;
    size_t memoryUsageBytes = 0;
    std::chrono::system_clock::time_point loadTime;
    size_t requestCount = 0;
    size_t errorCount = 0;
};

// ============================================================================
// Abstract Model Provider Interface
// ============================================================================

/**
 * Abstract interface for model providers (ONNX, TensorFlow, etc.)
 * This allows the daemon to work with different ML backends without
 * directly depending on their libraries.
 */
class IModelProvider {
public:
    virtual ~IModelProvider() = default;

    // Optional: progress callback for long-running model operations (e.g., preload)
    // Default no-op; providers may invoke with phases like
    // started/downloading/initializing/warming/completed
    virtual void setProgressCallback(std::function<void(const ModelLoadEvent&)> /*cb*/) {}

    // ========================================================================
    // Model Operations
    // ========================================================================

    /**
     * Generate embedding for a single text
     * @param text Input text to embed
     * @return Vector of float embeddings or error
     */
    virtual Result<std::vector<float>> generateEmbedding(const std::string& text) = 0;

    /**
     * Generate embeddings for a batch of texts
     * @param texts Input texts to embed
     * @return Vector of embedding vectors or error
     */
    virtual Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) = 0;

    // Generate embedding(s) using a specific model name when provided by the caller
    // Implementations should prefer this over the default model selection logic.
    virtual Result<std::vector<float>> generateEmbeddingFor(const std::string& modelName,
                                                            const std::string& text) = 0;
    virtual Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& modelName,
                               const std::vector<std::string>& texts) = 0;

    // ========================================================================
    // Model Management
    // ========================================================================

    /**
     * Load a model by name
     * @param modelName Name of the model to load
     * @return Success or error
     */
    virtual Result<void> loadModel(const std::string& modelName) = 0;
    // Optional: load model with provider-specific options (JSON). Default forwards to loadModel.
    virtual Result<void> loadModelWithOptions(const std::string& modelName,
                                              const std::string& optionsJson) {
        (void)optionsJson;
        return loadModel(modelName);
    }

    /**
     * Unload a model from memory
     * @param modelName Name of the model to unload
     * @return Success or error
     */
    virtual Result<void> unloadModel(const std::string& modelName) = 0;

    /**
     * Check if a model is loaded
     * @param modelName Name of the model to check
     * @return true if loaded, false otherwise
     */
    virtual bool isModelLoaded(const std::string& modelName) const = 0;

    /**
     * Get list of loaded models
     * @return Vector of model names
     */
    virtual std::vector<std::string> getLoadedModels() const = 0;

    // ========================================================================
    // Model Information
    // ========================================================================

    /**
     * Get information about a model
     * @param modelName Name of the model
     * @return Model information or error if not found
     */
    virtual Result<ModelInfo> getModelInfo(const std::string& modelName) const = 0;

    /**
     * Get embedding dimension for a model
     * @param modelName Name of the model
     * @return Embedding dimension or 0 if not loaded
     */
    virtual size_t getEmbeddingDim(const std::string& modelName) const = 0;

    // ========================================================================
    // Provider Information
    // ========================================================================

    /**
     * Get the name of this provider (e.g., "ONNX", "TensorFlow")
     * @return Provider name
     */
    virtual std::string getProviderName() const = 0;

    /**
     * Get provider version information
     * @return Version string
     */
    virtual std::string getProviderVersion() const = 0;

    /**
     * Check if the provider is available and functional
     * @return true if provider can be used
     */
    virtual bool isAvailable() const = 0;

    // ========================================================================
    // Resource Management
    // ========================================================================

    /**
     * Get current memory usage in bytes
     * @return Memory usage or 0 if unknown
     */
    virtual size_t getMemoryUsage() const = 0;

    /**
     * Release unused resources to free memory
     */
    virtual void releaseUnusedResources() = 0;

    /**
     * Shutdown the provider and release all resources
     */
    virtual void shutdown() = 0;
};

// ============================================================================
// Model Provider Factory
// ============================================================================

/**
 * Factory function type for creating model providers
 * Using std::function to support lambdas and captured state from plugins
 */
using ModelProviderFactory = std::function<std::unique_ptr<IModelProvider>()>;

/**
 * Create a model provider based on available backends
 * This function will attempt to load providers in order of preference:
 * 1. ONNX Runtime (if available)
 * 2. Mock provider (for testing)
 * 3. Null provider (no-op)
 *
 * @param preferredProvider Optional name of preferred provider
 * @return Unique pointer to model provider or nullptr if none available
 */
std::unique_ptr<IModelProvider> createModelProvider(const std::string& preferredProvider = "");

/**
 * Create a model provider with configuration
 * @param config Model pool configuration
 * @param preferredProvider Optional name of preferred provider
 * @return Unique pointer to model provider or nullptr if none available
 */
std::unique_ptr<IModelProvider> createModelProvider(const ModelPoolConfig& config,
                                                    const std::string& preferredProvider = "");

/**
 * Register a model provider factory
 * @param name Name of the provider
 * @param factory Factory function to create the provider
 */
void registerModelProvider(const std::string& name, ModelProviderFactory factory);

/**
 * Get list of registered model providers
 * @return Vector of provider names
 */
std::vector<std::string> getRegisteredProviders();

} // namespace yams::daemon

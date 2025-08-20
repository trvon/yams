#pragma once

#include <memory>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::ml {

// ============================================================================
// Abstract Embedding Provider Interface
// ============================================================================

/**
 * Abstract interface for embedding providers
 * This allows the vector library to work with different embedding backends
 * without directly depending on their libraries.
 */
class IEmbeddingProvider {
public:
    virtual ~IEmbeddingProvider() = default;

    // ========================================================================
    // Core Operations
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

    // ========================================================================
    // Provider Information
    // ========================================================================

    /**
     * Check if the provider is available and functional
     * @return true if provider can be used
     */
    virtual bool isAvailable() const = 0;

    /**
     * Get the name of this provider (e.g., "Mock", "Daemon", "ONNX")
     * @return Provider name
     */
    virtual std::string getProviderName() const = 0;

    /**
     * Get embedding dimension
     * @return Embedding dimension or 0 if not available
     */
    virtual size_t getEmbeddingDimension() const = 0;

    /**
     * Get maximum sequence length
     * @return Max sequence length or 0 if unlimited
     */
    virtual size_t getMaxSequenceLength() const = 0;

    // ========================================================================
    // Resource Management
    // ========================================================================

    /**
     * Initialize the provider
     * @return Success or error
     */
    virtual Result<void> initialize() = 0;

    /**
     * Shutdown the provider and release resources
     */
    virtual void shutdown() = 0;
};

// ============================================================================
// Embedding Provider Factory
// ============================================================================

/**
 * Factory function type for creating embedding providers
 */
using EmbeddingProviderFactory = std::unique_ptr<IEmbeddingProvider> (*)();

/**
 * Create an embedding provider based on available backends
 * This function will attempt to load providers in order of preference:
 * 1. Daemon client (if daemon is running)
 * 2. ONNX Runtime (if available and enabled)
 * 3. Mock provider (for testing)
 *
 * @param preferredProvider Optional name of preferred provider
 * @return Unique pointer to embedding provider or nullptr if none available
 */
std::unique_ptr<IEmbeddingProvider>
createEmbeddingProvider(const std::string& preferredProvider = "");

/**
 * Register an embedding provider factory
 * @param name Name of the provider
 * @param factory Factory function to create the provider
 */
void registerEmbeddingProvider(const std::string& name, EmbeddingProviderFactory factory);

/**
 * Get list of registered embedding providers
 * @return Vector of provider names
 */
std::vector<std::string> getRegisteredEmbeddingProviders();

} // namespace yams::ml
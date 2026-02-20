#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/IComponent.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>

namespace yams::vector {
class VectorDatabase;
} // namespace yams::vector

namespace yams::daemon {

struct StateComponent;
class ServiceManagerFsm;
class IModelProvider;

/**
 * @brief Manages vector database lifecycle.
 *
 * Extracted from ServiceManager (PBI-088) to centralize vector system concerns.
 *
 * ## Responsibilities
 * - Vector database initialization (with cross-process locking)
 * - Embedding dimension resolution
 * - Sentinel file management
 *
 * ## Thread Safety
 * - initializeOnce() uses atomic guard for single-attempt semantics
 * - All accessors are thread-safe
 *
 * Note: VectorIndexManager was removed - SearchEngine uses VectorDatabase directly.
 */
class VectorSystemManager : public IComponent {
public:
    /**
     * @brief Dependency injection for VectorSystemManager.
     */
    struct Dependencies {
        /// State component for readiness tracking
        StateComponent* state{nullptr};

        /// Service manager FSM for event dispatch (optional)
        ServiceManagerFsm* serviceFsm{nullptr};

        /// Model provider for dimension resolution (optional)
        std::weak_ptr<IModelProvider> modelProvider;

        /// Function to resolve preferred model name
        std::function<std::string()> resolvePreferredModel;

        /// Function to get embedding dimension from generator
        std::function<size_t()> getEmbeddingDimension;
    };

    explicit VectorSystemManager(Dependencies deps);
    ~VectorSystemManager() override;

    // IComponent interface
    const char* getName() const override { return "VectorSystemManager"; }
    Result<void> initialize() override;
    void shutdown() override;

    /**
     * @brief Initialize vector database (single-attempt, idempotent).
     *
     * Safe to call multiple times; only the first invocation performs work.
     * Uses cross-process advisory locking to prevent concurrent initialization.
     *
     * @param dataDir Data directory containing vectors.db
     * @return Result<bool> - true if this call performed init, false if skipped/deferred
     */
    Result<bool> initializeOnce(const std::filesystem::path& dataDir);

    // Accessors
    std::shared_ptr<vector::VectorDatabase> getVectorDatabase() const {
        return std::atomic_load_explicit(&vectorDatabase_, std::memory_order_acquire);
    }

    /**
     * @brief Get embedding dimension from database config.
     * @return Dimension or 0 if not initialized
     */
    size_t getEmbeddingDimension() const;

    /**
     * @brief Check if vector database init was attempted.
     */
    bool wasInitAttempted() const { return initAttempted_.load(std::memory_order_acquire); }

    /**
     * @brief Reset init attempt flag (for retry scenarios).
     */
    void resetInitAttempt() { initAttempted_.store(false, std::memory_order_release); }

private:
    Dependencies deps_;

    std::shared_ptr<vector::VectorDatabase> vectorDatabase_;

    std::atomic<bool> initAttempted_{false};
};

} // namespace yams::daemon

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include "SearchEngineFsm.h"
#include <boost/asio/awaitable.hpp>
#include <yams/core/types.h> // For Result type

namespace yams::app::services {
class IGraphQueryService;
}

namespace yams::search {
class SearchEngine;
class SearchEngineBuilder;
} // namespace yams::search

namespace yams::metadata {
class MetadataRepository;
} // namespace yams::metadata

namespace yams::vector {
class VectorDatabase;
class EmbeddingGenerator;
} // namespace yams::vector

namespace yams::embedding {
class IEmbeddingGenerator;
} // namespace yams::embedding

namespace yams::daemon {

/**
 * Phase 2.4: SearchEngineManager - Encapsulates all search engine lifecycle concerns
 *
 * Owns:
 * - SearchEngineFsm (state machine)
 * - SearchEngine pointer (actual engine)
 * - Build coordination logic (async build coroutine)
 * - Mutexes for safe concurrent access
 * - Cached snapshot for non-blocking reads
 *
 * ServiceManager delegates to this manager instead of managing engine directly.
 */
class SearchEngineManager {
public:
    SearchEngineManager() = default;
    ~SearchEngineManager() = default;

    // Non-copyable, non-movable
    SearchEngineManager(const SearchEngineManager&) = delete;
    SearchEngineManager& operator=(const SearchEngineManager&) = delete;

    /**
     * Build or rebuild the search engine asynchronously.
     *
     * @param metadataRepo Metadata repository dependency
     * @param vectorDatabase Vector database (required for SearchEngine)
     * @param embeddingGen Optional embedding generator (for vector search)
     * @param reason Human-readable reason for build (e.g., "user_initiated", "auto_repair")
     * @param timeoutMs Build timeout in milliseconds
     * @param workerExecutor Executor for blocking build operations
     * @return Awaitable that resolves to Result containing engine pointer or error
     */
    boost::asio::awaitable<Result<std::shared_ptr<yams::search::SearchEngine>>>
    buildEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                std::shared_ptr<yams::vector::VectorDatabase> vectorDatabase,
                std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGen,
                const std::string& reason, int timeoutMs,
                boost::asio::any_io_executor workerExecutor);

    /**
     * Get cached engine pointer for non-blocking reads.
     * Returns nullptr if no engine is available or engine is building.
     * Thread-safe via snapshot mutex (does NOT block on build operations).
     */
    yams::search::SearchEngine* getCachedEngine() const;

    /**
     * Get current FSM snapshot (state, build reason, vector enabled, etc.)
     * Non-blocking, always succeeds.
     */
    SearchEngineSnapshot getSnapshot() const;

    /**
     * Get shared pointer to current engine (may block if build in progress).
     * Prefer getCachedEngine() for status/diagnostic reads.
     */
    std::shared_ptr<yams::search::SearchEngine> getEngine() const;

    /**
     * Set search engine from external source (e.g., during initialization).
     * Updates FSM state and cached snapshot.
     */
    void setEngine(const std::shared_ptr<yams::search::SearchEngine>& engine, bool vectorEnabled);

    /**
     * Check if engine is currently building.
     */
    bool isBuilding() const { return fsm_.isBuilding(); }

    /**
     * Check if engine is ready for queries.
     */
    bool isReady() const { return fsm_.isReady(); }

private:
    void refreshSnapshot();

    // FSM for state tracking
    SearchEngineFsm fsm_;

    // Actual search engine (guarded by engineMutex_)
    std::shared_ptr<yams::search::SearchEngine> engine_;
    mutable std::shared_mutex engineMutex_;

    // Cached snapshot for non-blocking reads (guarded by snapshotMutex_)
    mutable std::shared_mutex snapshotMutex_;
    yams::search::SearchEngine* cachedEngine_{nullptr};
};

} // namespace yams::daemon

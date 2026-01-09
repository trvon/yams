// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <yams/core/types.h>

namespace yams::search {
class SearchEngine;
} // namespace yams::search

namespace yams::metadata {
class MetadataRepository;
} // namespace yams::metadata

namespace yams::vector {
class VectorDatabase;
class EmbeddingGenerator;
} // namespace yams::vector

namespace yams::daemon {

class ServiceManager;
struct StateComponent;
struct SearchEngineSnapshot;

/**
 * SearchComponent - Manages search engine lifecycle and auto-tuning
 *
 * Responsibilities:
 * - Monitor corpus growth and trigger rebuilds when significant changes occur
 * - Ensure search engine tuning state matches corpus characteristics
 * - Provide thread-safe access to rebuild triggering
 *
 * The component runs periodic checks and triggers rebuilds when:
 * - Corpus grows by 10x since last build, OR
 * - Corpus grows by +1000 documents since last build
 *
 * Rebuild is done asynchronously and doesn't block the calling thread.
 */
class SearchComponent {
public:
    struct Config {
        double growthRatioThreshold;               // Growth ratio to trigger rebuild (default: 10x)
        uint64_t growthAbsoluteThreshold;          // Absolute doc count increase (default: +1000)
        std::chrono::milliseconds checkIntervalMs; // Min interval between checks (default: 5s)

        Config()
            : growthRatioThreshold(10.0), growthAbsoluteThreshold(1000), checkIntervalMs(5000) {}
    };

    SearchComponent(ServiceManager& serviceManager, StateComponent& state,
                    const Config& config = Config());
    ~SearchComponent();

    // Non-copyable
    SearchComponent(const SearchComponent&) = delete;
    SearchComponent& operator=(const SearchComponent&) = delete;

    /**
     * Check if search engine needs rebuild due to corpus growth.
     * Safe to call frequently - uses atomic guards and respects check interval.
     * Returns true if a rebuild was triggered (async, not awaited).
     */
    bool checkAndTriggerRebuildIfNeeded();

    /**
     * Force immediate rebuild regardless of corpus growth.
     * Returns true if rebuild was triggered.
     */
    bool forceRebuild();

    /**
     * Get the document count at last successful build.
     */
    uint64_t getLastBuildDocCount() const { return lastBuildDocCount_.load(); }

    /**
     * Record a successful build with current corpus size.
     * Called by ServiceManager after search engine build completes.
     */
    void recordSuccessfulBuild(uint64_t docCount);

    /**
     * Get current corpus document count from metadata repo.
     * Returns 0 if count cannot be determined.
     */
    uint64_t getCurrentDocCount() const;

    /**
     * Check if corpus has grown significantly since last build.
     */
    bool hasSignificantGrowth() const;

    /**
     * Get current configuration.
     */
    const Config& getConfig() const { return config_; }

private:
    ServiceManager& serviceManager_;
    StateComponent& state_;
    Config config_;

    // Document count at last successful build
    std::atomic<uint64_t> lastBuildDocCount_{0};

    // Prevent concurrent rebuild triggers
    std::atomic<bool> rebuildInProgress_{false};

    // Last check time for rate limiting
    mutable std::shared_mutex checkMutex_;
    std::chrono::steady_clock::time_point lastCheckTime_;
};

} // namespace yams::daemon

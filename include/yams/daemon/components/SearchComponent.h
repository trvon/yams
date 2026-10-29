// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <shared_mutex>
#include <yams/core/types.h>
#include <yams/search/search_execution_context.h>

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
        bool automaticRebuildsEnabled;             // Lifecycle-resolved maintenance policy

        Config()
            : growthRatioThreshold(10.0), growthAbsoluteThreshold(1000), checkIntervalMs(5000),
              automaticRebuildsEnabled(true) {}
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
    bool shouldTriggerHeavyRebuild() const;

    /**
     * Get current configuration.
     */
    const Config& getConfig() const { return config_; }

#ifdef YAMS_TESTING
    /// Replace the corpus probes (document count, insert counter, index freshness) so the
    /// growth evaluation can be driven without a metadata repository or search engine.
    void testing_setProbes(std::function<uint64_t()> docCount,
                           std::function<uint64_t()> insertCounter,
                           std::function<search::IndexFreshnessSnapshot()> freshness) {
        docCountProbe_ = std::move(docCount);
        insertCounterProbe_ = std::move(insertCounter);
        freshnessProbe_ = std::move(freshness);
    }
    uint64_t testing_lastCoveredEvaluationDocCount() const {
        return lastCoveredDocCount_.load(std::memory_order_acquire);
    }
#endif

private:
    uint64_t currentInsertCounter() const;
    search::IndexFreshnessSnapshot currentIndexFreshness() const;
    bool coveredSinceLastEvaluation() const;
    void recordCoveredEvaluation(uint64_t docCount) const;

    // Optional probe overrides (tests); production reads the ServiceManager.
    std::function<uint64_t()> docCountProbe_;
    std::function<uint64_t()> insertCounterProbe_;
    std::function<search::IndexFreshnessSnapshot()> freshnessProbe_;

    // Last evaluation that skipped a heavy rebuild because the lexical overlay covered the
    // growth: the document count seen and the repository insert counter at that time. While
    // the insert counter has not moved, nothing new can have arrived, so the evaluation (a DB
    // count, a freshness snapshot, a log line) is not repeated. Cleared by a successful build.
    mutable std::atomic<uint64_t> lastCoveredDocCount_{0};
    mutable std::atomic<uint64_t> lastCoveredInsertCounter_{0};
    mutable std::atomic<bool> coveredEvaluationValid_{false};

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

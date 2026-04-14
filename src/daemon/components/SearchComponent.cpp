// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstdlib>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/daemon/components/SearchComponent.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/corpus_stats.h>

namespace yams::daemon {

namespace {
constexpr auto kOverlayRebuildMinAge = std::chrono::minutes(5);
constexpr std::uint64_t kOverlayHeavyRebuildMinDocs = 256;
constexpr std::uint64_t kTopologyDirtyFastThreshold = 64;
} // namespace

SearchComponent::SearchComponent(ServiceManager& serviceManager, StateComponent& state,
                                 const Config& config)
    : serviceManager_(serviceManager), state_(state), config_(config) {
    // Initialize lastBuildDocCount from state if available
    uint64_t stateDocCount = state_.readiness.searchEngineDocCount.load();
    if (stateDocCount > 0) {
        lastBuildDocCount_.store(stateDocCount);
    }
}

SearchComponent::~SearchComponent() = default;

uint64_t SearchComponent::getCurrentDocCount() const {
    try {
        auto metadataRepo = serviceManager_.getMetadataRepo();
        if (metadataRepo) {
            auto countRes = metadataRepo->getDocumentCount();
            if (countRes) {
                return countRes.value();
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("[SearchComponent] Failed to get doc count: {}", e.what());
    }
    return 0;
}

bool SearchComponent::hasSignificantGrowth() const {
    uint64_t currentCount = getCurrentDocCount();
    uint64_t lastBuildCount = lastBuildDocCount_.load();

    if (currentCount == 0 || currentCount <= lastBuildCount) {
        return false;
    }

    // Check growth ratio threshold (e.g., 10x growth)
    bool ratioExceeded =
        (currentCount >=
         static_cast<uint64_t>(static_cast<double>(lastBuildCount) * config_.growthRatioThreshold));

    // Check absolute growth threshold (e.g., +1000 docs)
    bool absoluteExceeded = (currentCount >= lastBuildCount + config_.growthAbsoluteThreshold);

    return ratioExceeded || absoluteExceeded;
}

bool SearchComponent::shouldTriggerHeavyRebuild() const {
    if (!hasSignificantGrowth()) {
        return false;
    }

    auto metadataRepo = serviceManager_.getMetadataRepo();
    if (!metadataRepo) {
        return true;
    }

    const auto statsResult = metadataRepo->getCorpusStats();
    if (!statsResult) {
        return true;
    }

    const auto& stats = statsResult.value();
    if (!stats.usedOnlineOverlay) {
        return true;
    }

    const auto freshness = serviceManager_.getIndexFreshnessSnapshot();
    const auto topology = serviceManager_.getTopologyTelemetrySnapshot();
    const auto currentCount = getCurrentDocCount();
    const auto lastBuildCount = lastBuildDocCount_.load();
    const auto growth = currentCount > lastBuildCount ? currentCount - lastBuildCount : 0;
    const auto minGrowth =
        std::max<std::uint64_t>(config_.growthAbsoluteThreshold * 2, kOverlayHeavyRebuildMinDocs);
    const bool overlayLarge = freshness.lexicalDeltaRecentDocs >= minGrowth || growth >= minGrowth;
    const bool topologyPressure = topology.dirtyDocumentCount >= kTopologyDirtyFastThreshold;

    if (stats.reconciledComputedAtMs <= 0) {
        return overlayLarge && topologyPressure;
    }

    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    const auto overlayAgeMs = nowMs > stats.reconciledComputedAtMs
                                  ? static_cast<std::uint64_t>(nowMs - stats.reconciledComputedAtMs)
                                  : 0;
    const bool overlayAged =
        overlayAgeMs >=
        static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(kOverlayRebuildMinAge).count());
    return overlayLarge && (overlayAged || topologyPressure);
}

void SearchComponent::recordSuccessfulBuild(uint64_t docCount) {
    lastBuildDocCount_.store(docCount);
    state_.readiness.searchEngineDocCount.store(docCount);
    rebuildInProgress_.store(false);
    spdlog::info("[SearchComponent] Recorded successful build with {} docs", docCount);
}

bool SearchComponent::checkAndTriggerRebuildIfNeeded() {
    // Rate limit checks
    {
        std::shared_lock lock(checkMutex_);
        auto now = std::chrono::steady_clock::now();
        if (lastCheckTime_.time_since_epoch().count() != 0) {
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - lastCheckTime_);
            if (elapsed < config_.checkIntervalMs) {
                return false;
            }
        }
    }

    // Update check time
    {
        std::unique_lock lock(checkMutex_);
        lastCheckTime_ = std::chrono::steady_clock::now();
    }

    // Check if search engine is ready (don't trigger if still initializing)
    if (!state_.readiness.searchEngineReady.load()) {
        return false;
    }

    // Allow disabling rebuilds (bench + ops control).
    if (const char* env = std::getenv("YAMS_DISABLE_SEARCH_REBUILDS")) {
        try {
            std::string v(env);
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            if (v == "1" || v == "true" || v == "yes" || v == "on") {
                return false;
            }
        } catch (...) {
        }
    }

    // Avoid rebuilds while ingestion is active (even if post-ingest is briefly drained).
    // Ingest can run ahead of post-ingest; rebuilds here create oscillations and noise.
    try {
        auto ingest = serviceManager_.getIngestMetricsSnapshot();
        if (ingest.queued > 0 || ingest.active > 0) {
            return false;
        }
    } catch (...) {
    }

    // Rebuild only when ingest/post-ingest/embedding pipelines are drained.
    // Rebuilding during heavy ingest competes with extraction+embedding and reduces throughput.
    if (const auto postIngest = serviceManager_.getPostIngestQueue()) {
        if (postIngest->size() > 0 || postIngest->totalInFlight() > 0) {
            return false;
        }
    }
    if (serviceManager_.getEmbeddingQueuedJobs() > 0 ||
        serviceManager_.getEmbeddingInFlightJobs() > 0) {
        return false;
    }

    // Rebuilds are also expensive for active interactive search traffic because the engine swap
    // and build work contend with query execution. Defer until the UX search lane is quiet.
    try {
        const auto searchLoad = serviceManager_.getSearchLoadMetrics();
        if (searchLoad.active > 0 || searchLoad.queued > 0) {
            return false;
        }
    } catch (...) {
    }

    // Check for concurrent rebuild
    bool expected = false;
    if (!rebuildInProgress_.compare_exchange_strong(expected, true)) {
        spdlog::debug("[SearchComponent] Rebuild already in progress, skipping");
        return false;
    }

    // Heavy rebuilds should follow reconciled shifts or large/aged overlay deltas.
    if (!shouldTriggerHeavyRebuild()) {
        rebuildInProgress_.store(false);
        return false;
    }

    uint64_t currentCount = getCurrentDocCount();
    uint64_t lastCount = lastBuildDocCount_.load();

    spdlog::info("[SearchComponent] Corpus grew significantly ({} -> {}), triggering rebuild",
                 lastCount, currentCount);

    // Spawn the rebuild coroutine
    return forceRebuild();
}

bool SearchComponent::forceRebuild() {
    // Mark rebuild in progress if not already
    bool expected = false;
    rebuildInProgress_.compare_exchange_strong(expected, true);

    try {
        auto executor = serviceManager_.getWorkerExecutor();
        auto weakSm = serviceManager_.weak_from_this();
        boost::asio::co_spawn(
            executor,
            [weakSm]() -> boost::asio::awaitable<void> {
                auto smPtr = weakSm.lock();
                if (!smPtr) {
                    co_return;
                }
                co_await smPtr->co_enableEmbeddingsAndRebuild();
            },
            boost::asio::detached);

        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[SearchComponent] Failed to spawn rebuild: {}", e.what());
        rebuildInProgress_.store(false);
        return false;
    }
}

} // namespace yams::daemon

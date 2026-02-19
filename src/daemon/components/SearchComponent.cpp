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

namespace yams::daemon {

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

    // Check for concurrent rebuild
    bool expected = false;
    if (!rebuildInProgress_.compare_exchange_strong(expected, true)) {
        spdlog::debug("[SearchComponent] Rebuild already in progress, skipping");
        return false;
    }

    // Check for significant corpus growth
    if (!hasSignificantGrowth()) {
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

        // IMPORTANT: Use shared_from_this() for ServiceManager to ensure proper lifetime.
        // ServiceManager inherits from std::enable_shared_from_this<ServiceManager>.
        auto smPtr = serviceManager_.shared_from_this();
        auto* sc = this;

        boost::asio::co_spawn(
            executor,
            [smPtr, sc]() -> boost::asio::awaitable<void> {
                co_await smPtr->co_enableEmbeddingsAndRebuild();
                // Record the build after completion
                uint64_t docCount = sc->getCurrentDocCount();
                sc->recordSuccessfulBuild(docCount);
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

// PBI-062: Prune request handler
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/integrity/repair_manager.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handlePruneRequest(const PruneRequest& req) {
    spdlog::info("Prune request: categories={} extensions={} dry_run={}", req.categories.size(),
                 req.extensions.size(), req.dryRun);

    // Get metadata repository for querying candidates
    auto metaRepo = serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
    if (!metaRepo) {
        co_return ErrorResponse{.code = ErrorCode::InternalError,
                                .message = "Metadata repository unavailable"};
    }

    // Build config from request
    integrity::PruneConfig config;
    config.dryRun = req.dryRun;
    config.categories = req.categories;
    config.extensions = req.extensions;

    // Parse age filter (e.g., "30d", "2w")
    if (!req.olderThan.empty()) {
        const auto& ageStr = req.olderThan;
        try {
            int64_t value = 0;
            char unit = 'd';
            if (std::sscanf(ageStr.c_str(), "%lld%c", &value, &unit) >= 1) {
                switch (unit) {
                    case 'd':
                        config.minAge = std::chrono::hours(value * 24);
                        break;
                    case 'w':
                        config.minAge = std::chrono::hours(value * 24 * 7);
                        break;
                    case 'm':
                        config.minAge = std::chrono::hours(value * 24 * 30);
                        break;
                    case 'y':
                        config.minAge = std::chrono::hours(value * 24 * 365);
                        break;
                    default:
                        spdlog::warn("Unknown age unit '{}', ignoring", unit);
                }
            }
        } catch (...) {
            spdlog::warn("Failed to parse older_than '{}', ignoring", ageStr);
        }
    }

    // Parse size filters (e.g., "10MB", "1GB")
    auto parseSize = [](const std::string& sizeStr) -> int64_t {
        if (sizeStr.empty())
            return 0;
        try {
            int64_t value = 0;
            char unit[8] = {0};
            if (std::sscanf(sizeStr.c_str(), "%lld%7s", &value, unit) >= 1) {
                std::string unitStr(unit);
                std::transform(unitStr.begin(), unitStr.end(), unitStr.begin(), ::toupper);

                if (unitStr == "KB")
                    return value * 1024;
                if (unitStr == "MB")
                    return value * 1024 * 1024;
                if (unitStr == "GB")
                    return value * 1024 * 1024 * 1024;
                return value;
            }
        } catch (...) {
        }
        return 0;
    };

    if (!req.largerThan.empty()) {
        config.minSize = parseSize(req.largerThan);
    }

    if (!req.smallerThan.empty()) {
        config.maxSize = parseSize(req.smallerThan);
    }

    // Query metadata DB for matching candidates (synchronous preview)
    PruneResponse response;
    response.filesDeleted = 0;
    response.filesFailed = 0;
    response.totalBytesFreed = 0;

    try {
        auto candidates = integrity::RepairManager::queryCandidatesForPrune(*metaRepo, config);

        // Aggregate by category for preview
        for (const auto& candidate : candidates) {
            response.categoryCounts[candidate.category]++;
            response.categorySizes[candidate.category] += candidate.fileSize;
            response.totalBytesFreed += candidate.fileSize;
        }

        spdlog::info("Prune preview: {} candidates, {} bytes total", candidates.size(),
                     response.totalBytesFreed);

        // If not dry-run and user wants to apply, queue async job
        if (!req.dryRun && !candidates.empty()) {
            static uint64_t pruneRequestCounter = 0;
            uint64_t requestId = ++pruneRequestCounter;

            InternalEventBus::PruneJob job{.requestId = requestId, .config = std::move(config)};

            static auto pruneQueue =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::PruneJob>(
                    "prune_jobs", 128);

            if (!pruneQueue->try_push(std::move(job))) {
                spdlog::error("Prune queue full, request dropped");
                InternalEventBus::instance().incPostDropped();
                response.errorMessage = "Prune queue full, try again later";
                co_return response;
            }

            InternalEventBus::instance().incPostQueued();
            spdlog::info("Prune job {} queued for async execution ({} files)", requestId,
                         candidates.size());

            response.statusMessage =
                fmt::format("Prune job {} started (processing {} files in background)", requestId,
                            candidates.size());
        }

    } catch (const std::exception& e) {
        spdlog::error("Prune query failed: {}", e.what());
        response.errorMessage = fmt::format("Failed to query candidates: {}", e.what());
    }

    co_return response;
}

} // namespace yams::daemon

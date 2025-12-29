// PBI-062: Prune request handler
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <system_error>
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

        // If not dry-run and user wants to apply, execute prune synchronously
        if (!req.dryRun && !candidates.empty()) {
            spdlog::info("Executing prune operation for {} candidates", candidates.size());

            uint64_t deleted = 0;
            uint64_t failed = 0;
            uint64_t bytesFreed = 0;

            for (size_t i = 0; i < candidates.size(); ++i) {
                const auto& candidate = candidates[i];

                if (i > 0 && i % 100 == 0) {
                    co_await boost::asio::post(boost::asio::use_awaitable);
                }
                try {
                    // Remove from metadata database only (do not delete filesystem files)
                    auto docResult = metaRepo->getDocumentByHash(candidate.hash);
                    if (docResult && docResult.value().has_value()) {
                        auto delResult = metaRepo->deleteDocument(docResult.value()->id);
                        if (delResult) {
                            deleted++;
                            bytesFreed += candidate.fileSize;
                        } else {
                            spdlog::warn("Failed to remove {} from metadata: {}",
                                         candidate.path, delResult.error().message);
                            failed++;
                        }
                    } else {
                        // Nothing to delete in metadata (already gone)
                        deleted++;
                    }
                } catch (const std::exception& e) {
                    spdlog::error("Exception while pruning {}: {}", candidate.path, e.what());
                    failed++;
                }
            }

            response.filesDeleted = deleted;
            response.filesFailed = failed;
            response.totalBytesFreed = bytesFreed;

            spdlog::info("Prune complete: deleted={} failed={} bytes_freed={}", deleted, failed,
                         bytesFreed);
        }

    } catch (const std::exception& e) {
        spdlog::error("Prune query failed: {}", e.what());
        response.errorMessage = fmt::format("Failed to query candidates: {}", e.what());
    }

    co_return response;
}

} // namespace yams::daemon

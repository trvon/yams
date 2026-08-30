#include <yams/daemon/components/ServiceManager.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/ipc/retrieval_session.h>

namespace yams::daemon {

bool ServiceManager::shouldStartSessionWatcher(std::string_view disableValue) {
    const std::string value(disableValue);
    return !ConfigResolver::envTruthy(value.c_str());
}

std::chrono::milliseconds ServiceManager::sessionWatcherDelay(bool watchEnabled,
                                                              std::uint32_t intervalMs) {
    constexpr auto kIdleDelay = std::chrono::milliseconds(2000);
    constexpr std::uint32_t kMinimumIntervalMs = 100;
    if (!watchEnabled) {
        return kIdleDelay;
    }
    return std::chrono::milliseconds(std::max(kMinimumIntervalMs, intervalMs));
}

app::services::AddDirectoryRequest
ServiceManager::makeSessionWatchRequest(std::string_view session,
                                        const std::filesystem::path& directory,
                                        std::vector<std::string> changed) {
    app::services::AddDirectoryRequest request;
    request.directoryPath = directory.string();
    request.includePatterns = std::move(changed);
    request.recursive = true;
    request.sessionId = std::string(session);
    request.noGitignore = false;
    return request;
}

bool ServiceManager::scanSessionWatchDirectory(app::services::IIndexingService& indexingService,
                                               app::services::IDocumentService* documentService,
                                               std::string_view session,
                                               const std::filesystem::path& directory) {
    std::error_code error;
    if (directory.empty() || !std::filesystem::is_directory(directory, error)) {
        return false;
    }

    auto& previousFiles = sessionWatch_.dirFiles[directory.string()];
    std::unordered_map<std::string, std::pair<std::uint64_t, std::uint64_t>> currentFiles;
    std::vector<std::string> changed;
    auto it = std::filesystem::recursive_directory_iterator(directory, error);
    const auto end = std::filesystem::recursive_directory_iterator();
    for (; !error && it != end; it.increment(error)) {
        std::error_code metadataError;
        if (!it->is_regular_file(metadataError)) {
            if (metadataError) {
                error = metadataError;
            }
            continue;
        }

        const auto filePath = it->path().string();
        const auto fileSize = static_cast<std::uint64_t>(it->file_size(metadataError));
        if (metadataError) {
            error = metadataError;
            break;
        }
        const auto modifiedTime = it->last_write_time(metadataError);
        if (metadataError) {
            error = metadataError;
            break;
        }
        const auto modifiedAt = static_cast<std::uint64_t>(modifiedTime.time_since_epoch().count());
        currentFiles[filePath] = {modifiedAt, fileSize};
        const auto previous = previousFiles.find(filePath);
        if (previous == previousFiles.end() || previous->second != currentFiles[filePath]) {
            std::error_code relativeError;
            const auto relativePath =
                std::filesystem::relative(it->path(), directory, relativeError);
            auto relative =
                relativeError ? it->path().filename().string() : relativePath.generic_string();
            if (!relative.empty()) {
                changed.emplace_back(std::move(relative));
            }
        }
    }
    if (error) {
        spdlog::warn("[ServiceManager] session watcher scan failed for '{}': {}",
                     directory.string(), error.message());
        return false;
    }

    std::vector<std::string> removed;
    removed.reserve(previousFiles.size());
    for (const auto& [filePath, fingerprint] : previousFiles) {
        (void)fingerprint;
        if (!currentFiles.contains(filePath)) {
            removed.push_back(filePath);
        }
    }

    if (!changed.empty()) {
        auto request = makeSessionWatchRequest(session, directory, std::move(changed));
        auto indexed = indexingService.addDirectory(request);
        if (!indexed || indexed.value().filesFailed != 0) {
            const auto detail =
                indexed ? std::to_string(indexed.value().filesFailed) + " changed file(s) failed"
                        : indexed.error().message;
            spdlog::warn("[ServiceManager] session watcher indexing failed for '{}': {}",
                         directory.string(), detail);
            return false;
        }
    }

    if (!removed.empty() && documentService == nullptr) {
        spdlog::warn("[ServiceManager] session watcher cannot remove {} stale path(s) for '{}': "
                     "document service unavailable",
                     removed.size(), directory.string());
        return false;
    }
    for (const auto& filePath : removed) {
        app::services::DeleteByNameRequest request;
        request.name = filePath;
        request.force = false;
        auto deleted = documentService->deleteByName(request);
        if (!deleted) {
            if (deleted.error().code == ErrorCode::NotFound) {
                continue;
            }
            spdlog::warn("[ServiceManager] session watcher removal failed for '{}': {}", filePath,
                         deleted.error().message);
            return false;
        }
        if (!deleted.value().errors.empty()) {
            spdlog::warn("[ServiceManager] session watcher removal failed for '{}': {}", filePath,
                         deleted.value().errors.front().error.value_or("unknown error"));
            return false;
        }
    }

    previousFiles.swap(currentFiles);
    return true;
}

std::chrono::milliseconds ServiceManager::runSessionWatcherIteration() {
    yams::app::services::AppContext appCtx = getAppContext();
    auto sessionService = yams::app::services::makeSessionService(&appCtx);
    const auto current = sessionService->current();
    if (!current) {
        return sessionWatcherDelay(false, 0);
    }

    const bool watchEnabled = sessionService->watchEnabled(current);
    const auto delay = sessionWatcherDelay(watchEnabled, sessionService->watchIntervalMs(current));
    if (!watchEnabled) {
        return delay;
    }

    auto indexingService = yams::app::services::makeIndexingService(appCtx);
    if (!indexingService) {
        return delay;
    }
    auto documentService = yams::app::services::makeDocumentService(appCtx);

    for (const auto& pattern : sessionService->getPinnedPatterns(current)) {
        scanSessionWatchDirectory(*indexingService, documentService.get(), *current, pattern);
    }
    return delay;
}

boost::asio::awaitable<void>
ServiceManager::co_runSessionWatcher(const yams::compat::stop_token& token) {
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);

    while (!token.stop_requested()) {
        auto waitDuration = sessionWatcherDelay(false, 0);
        try {
            waitDuration = runSessionWatcherIteration();
        } catch (const std::exception& e) {
            spdlog::debug("[ServiceManager] session watcher iteration failed: {}", e.what());
        } catch (...) {
            spdlog::debug(
                "[ServiceManager] session watcher iteration failed with unknown exception");
        }

        try {
            if (retrievalSessions_) {
                retrievalSessions_->cleanupExpired(std::chrono::seconds(60));
            }
        } catch (const std::exception& e) {
            spdlog::debug("[ServiceManager] retrieval-session cleanup failed: {}", e.what());
        } catch (...) {
            spdlog::debug(
                "[ServiceManager] retrieval-session cleanup failed with unknown exception");
        }

        boost::system::error_code error;
        timer.expires_after(waitDuration);
        co_await timer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if (token.stop_requested() || error == boost::asio::error::operation_aborted) {
            break;
        }
    }

    co_return;
}

} // namespace yams::daemon

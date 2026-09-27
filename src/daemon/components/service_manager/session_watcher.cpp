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
#include <unordered_set>
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
#include <yams/common/gitignore.h>
#include <yams/common/log_rate_limiter.h>
#include <yams/compat/thread_stop_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/ipc/retrieval_session.h>

namespace yams::daemon {

namespace {
constexpr auto kSessionWatcherWarnInterval = std::chrono::minutes(10);
} // namespace

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
    using Fingerprint = std::pair<std::uint64_t, std::uint64_t>;
    std::error_code error;
    if (directory.empty() || !std::filesystem::is_directory(directory, error)) {
        return false;
    }

    // Honor the directory's .gitignore the same way addDirectory does, so ignored build
    // output is never tracked here: it cannot produce removals, and it is not re-statted
    // on every pass. Patterns reload when .gitignore changes.
    const auto dirKey = directory.string();
    {
        std::error_code mtimeError;
        const auto mtime = std::filesystem::last_write_time(directory / ".gitignore", mtimeError);
        const auto stamp = mtimeError
                               ? std::uint64_t{0}
                               : static_cast<std::uint64_t>(mtime.time_since_epoch().count());
        auto known = sessionWatch_.gitignoreMtime.find(dirKey);
        if (known == sessionWatch_.gitignoreMtime.end() || known->second != stamp) {
            sessionWatch_.gitignorePatterns[dirKey] =
                yams::common::loadGitignorePatterns(directory);
            sessionWatch_.gitignoreMtime[dirKey] = stamp;
        }
    }
    const auto& ignorePatterns = sessionWatch_.gitignorePatterns[dirKey];
    const auto relativeTo = [&](const std::filesystem::path& path) {
        std::error_code relativeError;
        const auto relative = std::filesystem::relative(path, directory, relativeError);
        return relativeError ? path.filename().generic_string() : relative.generic_string();
    };

    auto& previousFiles = sessionWatch_.dirFiles[dirKey];
    std::unordered_map<std::string, Fingerprint> currentFiles;
    std::vector<std::string> changed;         // relative, for the indexing request
    std::vector<std::string> changedAbsolute; // same order, to hold back failures
    auto it = std::filesystem::recursive_directory_iterator(directory, error);
    const auto end = std::filesystem::recursive_directory_iterator();
    for (; !error && it != end; it.increment(error)) {
        std::error_code metadataError;
        const auto relative = relativeTo(it->path());
        if (it->is_directory(metadataError)) {
            if (relative == ".git" || (!ignorePatterns.empty() &&
                                       yams::common::matchesGitignore(relative, ignorePatterns))) {
                it.disable_recursion_pending();
            }
            continue;
        }
        if (!it->is_regular_file(metadataError)) {
            if (metadataError) {
                error = metadataError;
            }
            continue;
        }
        if (!ignorePatterns.empty() && yams::common::matchesGitignore(relative, ignorePatterns)) {
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
        const Fingerprint fingerprint{
            static_cast<std::uint64_t>(modifiedTime.time_since_epoch().count()), fileSize};
        currentFiles[filePath] = fingerprint;
        const auto previous = previousFiles.find(filePath);
        if ((previous == previousFiles.end() || previous->second != fingerprint) &&
            !relative.empty()) {
            changed.push_back(relative);
            changedAbsolute.push_back(filePath);
        }
    }
    if (error) {
        // Retried every pass (seconds apart) until the directory is readable again.
        static yams::common::LogRateLimiter scanFailureLog{kSessionWatcherWarnInterval};
        if (auto held = scanFailureLog.admit()) {
            spdlog::warn("[ServiceManager] session watcher scan failed for '{}': {}{}",
                         directory.string(), error.message(),
                         yams::common::LogRateLimiter::suppressedSuffix(*held));
        }
        return false;
    }

    // Each path that fails is held back on its own, so one bad path is retried alone and
    // never blocks the snapshot from advancing for everything else. (A stale snapshot made
    // every later file look "changed" again and re-indexed it on every pass.)
    bool allSucceeded = true;
    const auto noteFailure = [&](const std::string& path, std::string_view what,
                                 std::string_view detail) {
        allSucceeded = false;
        // A directory full of failing files reports each once, but not thousands of lines.
        static yams::common::LogRateLimiter firstFailureLog{kSessionWatcherWarnInterval};
        if (sessionWatch_.failingPaths.insert(path).second) {
            if (auto held = firstFailureLog.admit()) {
                spdlog::warn("[ServiceManager] session watcher {} failed for '{}': {} (retrying "
                             "quietly){}",
                             what, path, detail,
                             yams::common::LogRateLimiter::suppressedSuffix(*held));
            } else {
                spdlog::debug("[ServiceManager] session watcher {} failed for '{}': {}", what, path,
                              detail);
            }
        } else {
            spdlog::debug("[ServiceManager] session watcher {} still failing for '{}': {}", what,
                          path, detail);
        }
    };
    std::unordered_set<std::string> heldBack;
    const auto holdBackChanged = [&](const std::string& filePath) {
        heldBack.insert(filePath);
        // Keep the previous fingerprint (or none) so the file is still "changed" next pass
        // without looking removed now.
        const auto previous = previousFiles.find(filePath);
        if (previous != previousFiles.end()) {
            currentFiles[filePath] = previous->second;
        } else {
            currentFiles.erase(filePath);
        }
    };

    if (!changed.empty()) {
        auto request = makeSessionWatchRequest(session, directory, changed);
        auto indexed = indexingService.addDirectory(request);
        if (!indexed) {
            for (const auto& filePath : changedAbsolute) {
                holdBackChanged(filePath);
            }
            allSucceeded = false;
            static yams::common::LogRateLimiter indexFailureLog{kSessionWatcherWarnInterval};
            if (auto held = indexFailureLog.admit()) {
                spdlog::warn("[ServiceManager] session watcher indexing failed for '{}': {}{}",
                             directory.string(), indexed.error().message,
                             yams::common::LogRateLimiter::suppressedSuffix(*held));
            }
        } else if (indexed.value().filesFailed != 0) {
            std::unordered_set<std::string> failed;
            for (const auto& result : indexed.value().results) {
                if (!result.success) {
                    failed.insert(result.path);
                    noteFailure(result.path, "indexing", result.error.value_or("unknown error"));
                }
            }
            // Without per-file results, retry every changed file.
            for (const auto& filePath : changedAbsolute) {
                if (failed.empty() || failed.contains(filePath)) {
                    holdBackChanged(filePath);
                }
            }
            allSucceeded = false;
        }
        for (const auto& filePath : changedAbsolute) {
            if (!heldBack.contains(filePath)) {
                sessionWatch_.failingPaths.erase(filePath);
            }
        }
    }

    for (const auto& [filePath, fingerprint] : previousFiles) {
        if (currentFiles.contains(filePath)) {
            continue;
        }
        std::error_code existsError;
        if (std::filesystem::exists(filePath, existsError)) {
            continue; // held back above, or now gitignored; nothing to remove
        }
        if (documentService == nullptr) {
            currentFiles[filePath] = fingerprint;
            noteFailure(filePath, "removal", "document service unavailable");
            continue;
        }
        app::services::DeleteByNameRequest request;
        request.name = filePath;
        // The file is gone, so every document stored at this path is stale. Without force,
        // a path indexed more than once ("Multiple documents match") could never be removed.
        request.force = true;
        auto deleted = documentService->deleteByName(request);
        std::string failure;
        if (!deleted) {
            if (deleted.error().code != ErrorCode::NotFound) {
                failure = deleted.error().message;
            }
        } else if (!deleted.value().errors.empty()) {
            failure = deleted.value().errors.front().error.value_or("unknown error");
        }
        if (!failure.empty()) {
            currentFiles[filePath] = fingerprint; // keep it tracked so the removal is retried
            noteFailure(filePath, "removal", failure);
        } else {
            sessionWatch_.failingPaths.erase(filePath);
        }
    }

    previousFiles.swap(currentFiles);
    return allSucceeded;
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

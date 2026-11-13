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
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/ipc/retrieval_session.h>

namespace yams::daemon {

bool ServiceManager::shouldStartSessionWatcher(std::string_view disableValue) {
    const std::string value(disableValue);
    return !ConfigResolver::envTruthy(value.c_str());
}

std::chrono::milliseconds ServiceManager::sessionWatcherDelay(bool watchEnabled,
                                                              std::uint32_t intervalMs) {
    return SessionWatcher::delay(watchEnabled, intervalMs);
}

app::services::AddDirectoryRequest
ServiceManager::makeSessionWatchRequest(std::string_view session,
                                        const std::filesystem::path& directory,
                                        std::vector<std::string> changed) {
    return SessionWatcher::makeRequest(session, directory, std::move(changed));
}

void ServiceManager::configureSessionWatcher(bool forDaemonLoop) {
    auto options = sessionWatcher_.options();
    if (!options.stopRequested) {
        options.stopRequested = [this] { return sessionWatchStopSource_.stop_requested(); };
    }
    if (forDaemonLoop && !sessionWatcherConfigured_) {
        // The snapshot describes what this store has indexed, so it lives in the data dir.
        if (const auto dataDir = getResolvedDataDir(); !dataDir.empty()) {
            options.stateDir = dataDir / "watch";
            sessionWatcherConfigured_ = true;
        }
        // The walk and the indexing it queues are background work: they wait while this
        // daemon is under memory or CPU pressure, and while the rest of the host is busy.
        options.admit = [] {
            auto& governor = ResourceGovernor::instance();
            return governor.canAdmitWork() &&
                   governor.admitDeferrable(DeferrableWork::SessionWatch);
        };
    }
    sessionWatcher_.setOptions(std::move(options));
}

bool ServiceManager::scanSessionWatchDirectory(app::services::IIndexingService& indexingService,
                                               app::services::IDocumentService* documentService,
                                               std::string_view session,
                                               const std::filesystem::path& directory) {
    configureSessionWatcher(false);
    const SessionWatcher::Target target{std::string(session), directory, 0};
    return sessionWatcher_.scan(indexingService, documentService, target,
                                SessionWatcher::WallClock::now()) ==
           SessionWatcher::ScanOutcome::Clean;
}

std::chrono::milliseconds ServiceManager::runSessionWatcherIteration() {
    configureSessionWatcher(true);
    yams::app::services::AppContext appCtx = getAppContext();
    auto sessionService = yams::app::services::makeSessionService(&appCtx);
    auto indexingService = yams::app::services::makeIndexingService(appCtx);
    if (!sessionService || !indexingService) {
        return SessionWatcher::delay(false, 0);
    }
    auto documentService = yams::app::services::makeDocumentService(appCtx);
    return sessionWatcher_.runPass(*sessionService, *indexingService, documentService.get(),
                                   SessionWatcher::WallClock::now());
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

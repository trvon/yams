// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
//
// Background sweep that fills the grep trigram index (see GrepTrigramIndexer).

#include <spdlog/spdlog.h>

#include <yams/api/content_store.h>
#include <yams/daemon/components/GrepTrigramIndexer.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <memory>

namespace yams::daemon {

namespace {
// Between passes while documents are waiting: short, so a backfill makes steady progress, but a
// pause, so admission is re-evaluated against the rest of the host's work.
constexpr std::chrono::milliseconds kGrepTrigramBusyDelay{250};
// When nothing is waiting, or the work was not admitted.
constexpr std::chrono::milliseconds kGrepTrigramIdleDelay{60'000};
} // namespace

std::chrono::milliseconds ServiceManager::runGrepTrigramIteration() {
    if (!grepTrigramIndexer_) {
        auto repository = getMetadataRepo();
        auto store = getContentStore();
        if (!repository || !store) {
            return kGrepTrigramIdleDelay;
        }
        GrepTrigramIndexer::Options options;
        options.fetchBytes = [store](const std::string& hash) {
            return store->retrieveBytes(hash);
        };
        // Re-checked before every document: a backfill runs for minutes, so it yields as soon
        // as this daemon or the host comes under pressure.
        options.admit = [] { return ResourceGovernor::instance().canAdmitWork(); };
        options.stopRequested = [this] {
            return grepTrigramStopRequested_.load(std::memory_order_acquire);
        };
        grepTrigramIndexer_ =
            std::make_unique<GrepTrigramIndexer>(std::move(repository), std::move(options));
    }
    if (!grepTrigramIndexer_->available()) {
        return kGrepTrigramIdleDelay;
    }
    auto& governor = ResourceGovernor::instance();
    if (!governor.canAdmitWork() || !governor.admitDeferrable(DeferrableWork::GrepTrigramIndex)) {
        return kGrepTrigramIdleDelay;
    }
    const auto pass = grepTrigramIndexer_->runPass();
    if (pass.indexed > 0 || pass.failed > 0) {
        spdlog::debug("[grep-trigram] indexed {} document(s), {} failed{}", pass.indexed,
                      pass.failed, pass.morePending ? ", more pending" : "");
    }
    return (pass.morePending && !pass.interrupted) ? kGrepTrigramBusyDelay : kGrepTrigramIdleDelay;
}

boost::asio::awaitable<void> ServiceManager::co_runGrepTrigramIndexer() {
    auto executor = co_await boost::asio::this_coro::executor;
    // Shared with stopGrepTrigramIndexerForShutdown(), which cancels it from the strand.
    const auto timer = std::make_shared<boost::asio::steady_timer>(executor);
    {
        std::lock_guard lock(grepTrigramTimerMutex_);
        grepTrigramTimer_ = timer;
    }
    const auto stopping = [&] { return grepTrigramStopRequested_.load(std::memory_order_acquire); };

    // Let start-up work (index loads, the lexical build) go first.
    auto wait = kGrepTrigramIdleDelay;
    while (!stopping()) {
        boost::system::error_code error;
        timer->expires_after(wait);
        co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if (stopping() || error == boost::asio::error::operation_aborted) {
            break;
        }
        try {
            wait = runGrepTrigramIteration();
        } catch (const std::exception& e) {
            spdlog::debug("[grep-trigram] iteration failed: {}", e.what());
            wait = kGrepTrigramIdleDelay;
        } catch (...) {
            spdlog::debug("[grep-trigram] iteration failed with an unknown exception");
            wait = kGrepTrigramIdleDelay;
        }
    }

    {
        std::lock_guard lock(grepTrigramTimerMutex_);
        grepTrigramTimer_.reset();
    }
    co_return;
}

void ServiceManager::startGrepTrigramIndexerOn(const boost::asio::any_io_executor& executor) {
    grepTrigramStopRequested_.store(false, std::memory_order_release);
    // On a strand, so the cancel posted at shutdown cannot run alongside the coroutine's own use
    // of the timer.
    grepTrigramFuture_ = boost::asio::co_spawn(
        boost::asio::make_strand(executor),
        [this]() -> boost::asio::awaitable<void> { co_await co_runGrepTrigramIndexer(); },
        boost::asio::use_future);
}

void ServiceManager::stopGrepTrigramIndexerForShutdown() {
    grepTrigramStopRequested_.store(true, std::memory_order_release);
    try {
        std::shared_ptr<boost::asio::steady_timer> timer;
        {
            std::lock_guard lock(grepTrigramTimerMutex_);
            timer = grepTrigramTimer_;
        }
        if (timer) {
            boost::asio::post(timer->get_executor(), [timer] { timer->cancel(); });
        }
        if (grepTrigramFuture_.valid()) {
            // A pass stops before its next document once the flag is set; one document's insert
            // is the longest it can still take.
            if (grepTrigramFuture_.wait_for(std::chrono::seconds(10)) ==
                std::future_status::timeout) {
                spdlog::warn("[ServiceManager] grep trigram indexer did not stop within 10s");
            } else {
                grepTrigramFuture_.get();
            }
            grepTrigramFuture_ = std::future<void>();
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] grep trigram indexer stop failed: {}", e.what());
    } catch (...) {
        spdlog::warn("[ServiceManager] grep trigram indexer stop failed");
    }
}

} // namespace yams::daemon

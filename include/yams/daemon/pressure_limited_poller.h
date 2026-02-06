#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/daemon/components/GradientLimiter.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/TuneAdvisor.h>

namespace yams::daemon {

namespace detail {

inline bool applyPressureToLimit(std::size_t& maxConcurrent) {
    auto pressureLevel = ResourceGovernor::instance().getPressureLevel();
    switch (pressureLevel) {
        case ResourcePressureLevel::Emergency:
            maxConcurrent = 0;
            return true;
        case ResourcePressureLevel::Critical:
            maxConcurrent = 1;
            return false;
        case ResourcePressureLevel::Warning:
            maxConcurrent = std::max<std::size_t>(1, (maxConcurrent * 3) / 4);
            return false;
        default:
            return false;
    }
}

inline bool applyCpuThrottlingForPoller(boost::asio::steady_timer& timer) {
    if (!TuneAdvisor::enableResourceGovernor())
        return false;
    auto snap = ResourceGovernor::instance().getSnapshot();
    int32_t delayMs = TuneAdvisor::computeCpuThrottleDelayMs(snap.cpuUsagePercent);
    if (delayMs > 0) {
        timer.expires_after(std::chrono::milliseconds(delayMs));
        return true;
    }
    return false;
}

} // namespace detail

/// Configuration for a pressure-limited poller coroutine.
/// Template parameter Task is the channel job type.
template <typename Task> struct PressureLimitedPollerConfig {
    std::string stageName;
    std::atomic<bool>* stopFlag = nullptr;
    std::atomic<bool>* startedFlag = nullptr;
    std::atomic<bool>* pauseFlag = nullptr;
    std::atomic<bool>* wasActiveFlag = nullptr;
    std::atomic<std::size_t>* inFlightCounter = nullptr;
    std::function<GradientLimiter*()> getLimiterFn;
    std::function<std::size_t()> maxConcurrentFn;
    std::function<bool(GradientLimiter*, const std::string&, const std::string&)> tryAcquireFn;
    std::function<void(const std::string&, bool)> completeJobFn;
    std::function<void()> checkDrainFn;
    boost::asio::any_io_executor executor;

    // Single-item processing
    std::function<void(Task&)> processFn;
    std::function<std::string(const Task&)> getHashFn;

    // Batch mode (channelPoller only)
    bool batchMode = false;
    std::function<std::size_t()> batchSizeFn;
    std::function<void(std::vector<Task>&&)> batchProcessFn;

    // Capability check (titlePoller)
    std::function<bool()> isCapableFn;

    bool enableCpuThrottling = true;
};

/// Generic pressure-limited poller coroutine.
/// Replaces channelPoller, kgPoller, symbolPoller, entityPoller, titlePoller.
template <typename Task>
boost::asio::awaitable<void> pressureLimitedPoll(std::shared_ptr<SpscQueue<Task>> channel,
                                                 PressureLimitedPollerConfig<Task> cfg) {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    cfg.startedFlag->store(true);
    spdlog::info("[PostIngestQueue] {} poller started", cfg.stageName);

    constexpr auto kMinIdleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);
    auto idleDelay = kMinIdleDelay;

    while (!cfg.stopFlag->load()) {
        try {
            // Capability check (e.g., titlePoller requires titleExtractor_)
            if (cfg.isCapableFn && !cfg.isCapableFn()) {
                timer.expires_after(std::chrono::milliseconds(250));
                co_await timer.async_wait(boost::asio::use_awaitable);
                continue;
            }

            std::size_t maxConcurrent = cfg.maxConcurrentFn();
            detail::applyPressureToLimit(maxConcurrent);

            if (cfg.pauseFlag->load(std::memory_order_acquire) || maxConcurrent == 0) {
                timer.expires_after(kMinIdleDelay);
                co_await timer.async_wait(boost::asio::use_awaitable);
                continue;
            }

            bool didWork = false;

            if (cfg.batchMode) {
                // Batch collection (channelPoller)
                const std::size_t batchSize = cfg.batchSizeFn ? cfg.batchSizeFn() : std::size_t(1);
                std::vector<Task> batch;
                batch.reserve(batchSize);
                Task task;

                while (cfg.inFlightCounter->load() < maxConcurrent && batch.size() < batchSize &&
                       channel->try_pop(task)) {
                    GradientLimiter* lim = cfg.getLimiterFn ? cfg.getLimiterFn() : nullptr;
                    if (!cfg.tryAcquireFn(lim, cfg.getHashFn(task), cfg.stageName)) {
                        channel->try_push(std::move(task));
                        break;
                    }
                    didWork = true;
                    cfg.inFlightCounter->fetch_add(1);
                    batch.push_back(std::move(task));
                }

                if (didWork && !batch.empty()) {
                    cfg.wasActiveFlag->store(true, std::memory_order_release);
                    const std::size_t batchCount = batch.size();
                    std::vector<std::string> hashes;
                    GradientLimiter* lim = cfg.getLimiterFn ? cfg.getLimiterFn() : nullptr;
                    if (lim) {
                        hashes.reserve(batchCount);
                        for (const auto& t : batch) {
                            hashes.push_back(cfg.getHashFn(t));
                        }
                    }
                    boost::asio::post(cfg.executor, [cfg, batch = std::move(batch), batchCount,
                                                     hashes = std::move(hashes)]() mutable {
                        cfg.batchProcessFn(std::move(batch));
                        for (const auto& h : hashes) {
                            cfg.completeJobFn(h, true);
                        }
                        cfg.inFlightCounter->fetch_sub(batchCount);
                        cfg.checkDrainFn();
                    });
                }
            } else {
                // Single-item processing (kg, symbol, entity, title pollers)
                Task job;
                while (cfg.inFlightCounter->load() < maxConcurrent && channel->try_pop(job)) {
                    GradientLimiter* lim = cfg.getLimiterFn ? cfg.getLimiterFn() : nullptr;
                    std::string hash = cfg.getHashFn(job);
                    if (!cfg.tryAcquireFn(lim, hash, cfg.stageName)) {
                        channel->try_push(std::move(job));
                        break;
                    }
                    didWork = true;
                    cfg.wasActiveFlag->store(true, std::memory_order_release);
                    cfg.inFlightCounter->fetch_add(1);
                    boost::asio::post(cfg.executor, [cfg, job = std::move(job),
                                                     hash = std::move(hash)]() mutable {
                        cfg.processFn(job);
                        cfg.completeJobFn(hash, true);
                        cfg.inFlightCounter->fetch_sub(1);
                        cfg.checkDrainFn();
                    });
                }
            }

            if (didWork) {
                if (cfg.enableCpuThrottling) {
                    if (detail::applyCpuThrottlingForPoller(timer)) {
                        co_await timer.async_wait(boost::asio::use_awaitable);
                    }
                }
                idleDelay = kMinIdleDelay;
                continue;
            }

            // Adaptive backoff when idle
            timer.expires_after(idleDelay);
            co_await timer.async_wait(boost::asio::use_awaitable);
            if (idleDelay < kMaxIdleDelay) {
                idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
            }
        } catch (const std::exception& e) {
            spdlog::error("[PostIngestQueue] {} poller exception: {}", cfg.stageName, e.what());
            idleDelay = std::chrono::milliseconds(100);
        }
    }

    spdlog::info("[PostIngestQueue] {} poller exited", cfg.stageName);
}

} // namespace yams::daemon

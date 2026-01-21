#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <utility>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <yams/daemon/components/InternalEventBus.h>

namespace yams::daemon {

template <typename Task> class ChannelPoller {
public:
    struct Config {
        std::size_t maxConcurrent = 4;
        std::size_t batchSize = 1;
        std::chrono::milliseconds minBackoff{1};
        std::chrono::milliseconds maxBackoff{50};
        bool singleItemMode = false;
    };

    using ProcessFn = std::function<void(const Task&)>;
    using BatchProcessFn = std::function<void(std::vector<Task>&&)>;

    ChannelPoller(std::shared_ptr<SpscQueue<Task>> channel, boost::asio::executor executor,
                  Config config = {})
        : channel_(std::move(channel)), executor_(std::move(executor)), config_(std::move(config)) {
    }

    void setSingleItemProcessor(ProcessFn processor) { singleProcessor_ = std::move(processor); }

    void setBatchProcessor(BatchProcessFn processor) { batchProcessor_ = std::move(processor); }

    boost::asio::awaitable<void> run(std::atomic<bool>& stop) {
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        auto idleDelay = config_.minBackoff;

        while (!stop.load()) {
            bool didWork = false;
            std::vector<Task> batch;
            batch.reserve(config_.batchSize);

            if (config_.singleItemMode) {
                Task task;
                while (inFlight_.load() < config_.maxConcurrent && channel_->try_pop(task)) {
                    didWork = true;
                    inFlight_.fetch_add(1);
                    boost::asio::post(executor_, [this, task = std::move(task)]() mutable {
                        if (singleProcessor_) {
                            singleProcessor_(task);
                        }
                        inFlight_.fetch_sub(1);
                        onItemProcessed();
                    });
                }
            } else {
                Task task;
                while (inFlight_.load() < config_.maxConcurrent &&
                       batch.size() < config_.batchSize && channel_->try_pop(task)) {
                    didWork = true;
                    inFlight_.fetch_add(1);
                    batch.push_back(std::move(task));
                }

                if (didWork && !batch.empty()) {
                    const std::size_t batchCount = batch.size();
                    boost::asio::post(executor_,
                                      [this, batch = std::move(batch), batchCount]() mutable {
                                          if (batchProcessor_) {
                                              batchProcessor_(std::move(batch));
                                          }
                                          inFlight_.fetch_sub(batchCount);
                                          onBatchProcessed();
                                      });
                }
            }

            if (didWork) {
                idleDelay = config_.minBackoff;
                continue;
            }

            timer.expires_after(idleDelay);
            co_await timer.async_wait(boost::asio::use_awaitable);
            if (idleDelay < config_.maxBackoff) {
                idleDelay *= 2;
            }
        }
    }

    std::size_t inFlight() const { return inFlight_.load(); }
    std::size_t queued() const { return channel_ ? channel_->size_approx() : 0; }

protected:
    virtual void onItemProcessed() {}
    virtual void onBatchProcessed() {}

    std::shared_ptr<SpscQueue<Task>> channel_;
    boost::asio::executor executor_;
    Config config_;
    std::atomic<std::size_t> inFlight_{0};
    ProcessFn singleProcessor_;
    BatchProcessFn batchProcessor_;
};

} // namespace yams::daemon

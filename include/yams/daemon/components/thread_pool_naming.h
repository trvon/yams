// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>

#include <yams/core/thread_name.h>

namespace yams::daemon {

/// Names the worker threads of a boost::asio::thread_pool, which creates them internally and
/// never runs caller code on them before the first task. Posts one task per worker; each task
/// names its thread and then holds it until every task has arrived, so the tasks land on
/// distinct threads. Every wait is bounded: if `threads` is larger than the pool, or the pool
/// is busy, the call returns after `budget` with some workers left unnamed instead of hanging.
/// Call right after constructing the pool, before posting other work.
inline void nameThreadPoolWorkers(boost::asio::thread_pool& pool, std::size_t threads,
                                  const std::string& prefix,
                                  std::chrono::milliseconds budget = std::chrono::seconds{2}) {
    struct Arrivals {
        std::mutex mutex;
        std::condition_variable cv;
        std::size_t arrived{0};
    };
    auto arrivals = std::make_shared<Arrivals>();
    const auto deadline = std::chrono::steady_clock::now() + budget;
    for (std::size_t i = 0; i < threads; ++i) {
        boost::asio::post(pool, [arrivals, threads, deadline, name = prefix + std::to_string(i)] {
            yams::setCurrentThreadName(name);
            std::unique_lock<std::mutex> lock(arrivals->mutex);
            ++arrivals->arrived;
            arrivals->cv.notify_all();
            arrivals->cv.wait_until(lock, deadline, [&] { return arrivals->arrived >= threads; });
        });
    }
    std::unique_lock<std::mutex> lock(arrivals->mutex);
    arrivals->cv.wait_until(lock, deadline, [&] { return arrivals->arrived >= threads; });
}

} // namespace yams::daemon

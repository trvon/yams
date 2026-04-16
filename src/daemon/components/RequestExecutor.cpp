// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2026 YAMS Project Contributors

#include "yams/daemon/components/RequestExecutor.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

#include <boost/asio/detail/concurrency_hint.hpp>

namespace yams::daemon {

RequestExecutor::RequestExecutor()
    : ioContext_(std::make_shared<boost::asio::io_context>(BOOST_ASIO_CONCURRENCY_HINT_SAFE)) {
    spdlog::debug("[RequestExecutor] Constructed");
}

RequestExecutor::~RequestExecutor() {
    if (started_) {
        try {
            spdlog::debug("[RequestExecutor] Destructor: stopping...");
        } catch (...) {
        }
        stop();
        for (auto& w : workers_) {
            if (w.joinable()) {
                try {
                    w.join();
                } catch (...) {
                }
            }
        }
        workers_.clear();
        started_ = false;
    }
}

std::size_t RequestExecutor::defaultThreadCount() {
    if (const char* env = std::getenv("YAMS_REQUEST_THREADS")) {
        try {
            auto v = std::stoul(env);
            if (v >= 1 && v <= 64)
                return v;
        } catch (...) {
        }
    }
    return std::max<std::size_t>(4, std::thread::hardware_concurrency() / 2);
}

void RequestExecutor::start(std::optional<std::size_t> numThreads) {
    if (started_)
        throw std::runtime_error("RequestExecutor already started");

    workGuard_.emplace(boost::asio::make_work_guard(*ioContext_));

    const std::size_t count = numThreads.value_or(defaultThreadCount());
    workers_.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        workers_.emplace_back([this, i]() {
            activeWorkers_.fetch_add(1, std::memory_order_release);
            for (;;) {
                try {
                    ioContext_->run();
                    break;
                } catch (const std::exception& e) {
                    try {
                        spdlog::error("[RequestExecutor] Worker {} exception: {}", i, e.what());
                    } catch (...) {
                    }
                } catch (...) {
                }
                if (ioContext_->stopped())
                    break;
            }
            activeWorkers_.fetch_sub(1, std::memory_order_release);
        });
    }
    started_ = true;
    spdlog::info("[RequestExecutor] Started with {} threads (dedicated IPC request pool)", count);
}

void RequestExecutor::stop() {
    if (workGuard_) {
        workGuard_.reset();
    }
    if (ioContext_) {
        ioContext_->stop();
    }
}

void RequestExecutor::join() {
    for (auto& w : workers_) {
        if (w.joinable())
            w.join();
    }
}

boost::asio::any_io_executor RequestExecutor::getExecutor() const noexcept {
    return ioContext_->get_executor();
}

std::shared_ptr<boost::asio::io_context> RequestExecutor::getIOContext() const noexcept {
    return ioContext_;
}

bool RequestExecutor::isRunning() const noexcept {
    return started_ && activeWorkers_.load(std::memory_order_relaxed) > 0;
}

std::size_t RequestExecutor::getWorkerCount() const noexcept {
    return workers_.size();
}

} // namespace yams::daemon

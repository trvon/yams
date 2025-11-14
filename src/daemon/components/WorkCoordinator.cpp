// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "yams/daemon/components/WorkCoordinator.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <stdexcept>

#include <boost/asio/detail/concurrency_hint.hpp>

namespace yams::daemon {

WorkCoordinator::WorkCoordinator()
    : ioContext_(std::make_shared<boost::asio::io_context>(BOOST_ASIO_CONCURRENCY_HINT_SAFE)),
      started_(false) {
    spdlog::debug("[WorkCoordinator] Constructed (io_context created, not started)");
}

WorkCoordinator::~WorkCoordinator() {
    // Ensure clean shutdown
    if (started_) {
        spdlog::debug("[WorkCoordinator] Destructor called with active threads, stopping...");
        stop();
        join();
    }
    spdlog::debug("[WorkCoordinator] Destroyed");
}

void WorkCoordinator::start(std::optional<std::size_t> numThreads) {
    if (started_) {
        throw std::runtime_error("WorkCoordinator already started");
    }

    spdlog::debug("[WorkCoordinator] Starting worker thread pool...");

    // Create work guard to keep io_context alive
    workGuard_.emplace(boost::asio::make_work_guard(*ioContext_));
    spdlog::debug("[WorkCoordinator] Work guard created");

    // Determine thread count
    const std::size_t workerCount =
        numThreads.value_or(std::max<std::size_t>(1, std::thread::hardware_concurrency()));

    // Spawn worker threads
    workers_.reserve(workerCount);
    try {
        for (std::size_t i = 0; i < workerCount; ++i) {
            workers_.emplace_back([this, i]() {
                spdlog::trace("[WorkCoordinator] Worker {} starting io_context.run()", i);
                ioContext_->run();
                spdlog::trace("[WorkCoordinator] Worker {} exited io_context.run()", i);
            });
        }
        started_ = true;
        spdlog::info("[WorkCoordinator] Started with {} worker threads", workerCount);
    } catch (const std::exception& e) {
        // Cleanup on thread creation failure
        spdlog::error("[WorkCoordinator] Failed to spawn worker thread: {}", e.what());
        spdlog::debug("[WorkCoordinator] Cleaning up {} existing workers", workers_.size());

        ioContext_->stop();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                    // Ignore join failures during cleanup
                }
            }
        }
        workers_.clear();
        workGuard_.reset();
        throw std::runtime_error(std::string("Failed to start WorkCoordinator: ") + e.what());
    }
}

void WorkCoordinator::stop() {
    if (!started_) {
        spdlog::debug("[WorkCoordinator] stop() called but not started (no-op)");
        return;
    }

    spdlog::debug("[WorkCoordinator] Stopping (resetting work guard and stopping io_context)...");
    workGuard_.reset();
    ioContext_->stop();
    spdlog::info("[WorkCoordinator] Work guard reset and io_context stopped");
}

void WorkCoordinator::join() {
    if (workers_.empty()) {
        spdlog::debug("[WorkCoordinator] join() called with no workers (no-op)");
        return;
    }

    spdlog::debug("[WorkCoordinator] Joining {} worker threads...", workers_.size());
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            try {
                worker.join();
            } catch (const std::exception& e) {
                spdlog::warn("[WorkCoordinator] Exception during worker join: {}", e.what());
            }
        }
    }
    workers_.clear();
    started_ = false;
    spdlog::info("[WorkCoordinator] All workers joined");
}

std::shared_ptr<boost::asio::io_context> WorkCoordinator::getIOContext() const noexcept {
    return ioContext_;
}

boost::asio::io_context::executor_type WorkCoordinator::getExecutor() const noexcept {
    return ioContext_->get_executor();
}

boost::asio::strand<boost::asio::io_context::executor_type> WorkCoordinator::makeStrand() const {
    return boost::asio::strand<boost::asio::io_context::executor_type>(ioContext_->get_executor());
}

bool WorkCoordinator::isRunning() const noexcept {
    return started_ && !workers_.empty();
}

std::size_t WorkCoordinator::getWorkerCount() const noexcept {
    return workers_.size();
}

} // namespace yams::daemon

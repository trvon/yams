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
    if (started_) {
        try {
            spdlog::debug("[WorkCoordinator] Destructor called with active threads, stopping...");
        } catch (...) {
        }
        stop();

        // Ensure all worker threads are joined before destruction.
        // The destructor is never executed on a worker thread (the ServiceManager owns the
        // WorkCoordinator), so a full join is safe here.
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                }
            }
        }
        workers_.clear();
        started_ = false;
    }
    try {
        spdlog::debug("[WorkCoordinator] Destroyed");
    } catch (...) {
    }
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

    workers_.reserve(workerCount);
    try {
        for (std::size_t i = 0; i < workerCount; ++i) {
            workers_.emplace_back([this, i]() {
                activeWorkers_.fetch_add(1, std::memory_order_release);
                try {
                    spdlog::trace("[WorkCoordinator] Worker {} starting io_context.run()", i);
                } catch (...) {
                }
                for (;;) {
                    try {
                        ioContext_->run();
                        break;
                    } catch (const std::exception& e) {
                        try {
                            spdlog::error("[WorkCoordinator] Worker {} exception: {}", i, e.what());
                        } catch (...) {
                        }
                    } catch (...) {
                        try {
                            spdlog::error("[WorkCoordinator] Worker {} unknown exception", i);
                        } catch (...) {
                        }
                    }

                    if (ioContext_->stopped()) {
                        break;
                    }

                    try {
                        spdlog::debug("[WorkCoordinator] Worker {} restarting after exception", i);
                    } catch (...) {
                    }
                }
                try {
                    spdlog::trace("[WorkCoordinator] Worker {} exited io_context.run()", i);
                } catch (...) {
                }
                activeWorkers_.fetch_sub(1, std::memory_order_release);
                joinCV_.notify_all();
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
        try {
            spdlog::debug("[WorkCoordinator] stop() called but not started (no-op)");
        } catch (...) {
        }
        return;
    }

    try {
        spdlog::debug("[WorkCoordinator] Stopping...");
    } catch (...) {
    }
    workGuard_.reset();
    ioContext_->stop();
    try {
        spdlog::info("[WorkCoordinator] Work guard reset and io_context stopped");
    } catch (...) {
    }
}

void WorkCoordinator::join() {
    if (workers_.empty()) {
        try {
            spdlog::debug("[WorkCoordinator] join() called with no workers (no-op)");
        } catch (...) {
        }
        return;
    }

    try {
        spdlog::debug("[WorkCoordinator] Joining {} worker threads...", workers_.size());
    } catch (...) {
    }
    for (auto& worker : workers_) {
        if (!worker.joinable()) {
            continue;
        }

        // If join() is called from one of the worker threads, do not attempt to
        // self-join, and leave thread cleanup to the WorkCoordinator destructor.
        if (worker.get_id() == std::this_thread::get_id()) {
            continue;
        }

        try {
            worker.join();
        } catch (const std::exception& e) {
            try {
                spdlog::warn("[WorkCoordinator] join exception: {}", e.what());
            } catch (...) {
            }
        } catch (...) {
            try {
                spdlog::warn("[WorkCoordinator] join unknown exception");
            } catch (...) {
            }
        }
    }
    workers_.clear();
    started_ = false;
    try {
        spdlog::info("[WorkCoordinator] All workers joined");
    } catch (...) {
    }
}

bool WorkCoordinator::joinWithTimeout(std::chrono::milliseconds timeout) {
    if (workers_.empty()) {
        try {
            spdlog::debug("[WorkCoordinator] joinWithTimeout() called with no workers (no-op)");
        } catch (...) {
        }
        return true;
    }

    try {
        spdlog::debug("[WorkCoordinator] joinWithTimeout({}ms) waiting for {} workers",
                      timeout.count(), activeWorkers_.load());
    } catch (...) {
    }

    std::unique_lock<std::mutex> lock(joinMutex_);
    bool completed = joinCV_.wait_for(
        lock, timeout, [this]() { return activeWorkers_.load(std::memory_order_acquire) == 0; });

    if (!completed) {
        try {
            spdlog::warn("[WorkCoordinator] Timeout expired with {} workers still active",
                         activeWorkers_.load());
        } catch (...) {
        }
        // Detach remaining threads - they'll exit when process terminates
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.detach();
            }
        }
    } else {
        // Join all workers that have exited
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                try {
                    worker.join();
                } catch (...) {
                }
            }
        }
    }

    workers_.clear();
    started_ = false;
    try {
        spdlog::info("[WorkCoordinator] joinWithTimeout complete (success={})", completed);
    } catch (...) {
    }
    return completed;
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

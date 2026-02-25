// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2025 YAMS Project Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

#include "yams/daemon/components/WorkCoordinator.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <stdexcept>

#include <boost/asio/detail/concurrency_hint.hpp>

namespace yams::daemon {

WorkCoordinator::WorkCoordinator()
    : ioContext_(std::make_shared<boost::asio::io_context>(BOOST_ASIO_CONCURRENCY_HINT_SAFE)),
      started_(false), highPriorityStrand_(ioContext_->get_executor()),
      normalPriorityStrand_(ioContext_->get_executor()),
      backgroundPriorityStrand_(ioContext_->get_executor()) {
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
    {
        std::lock_guard<std::mutex> stateLock(workerStateMutex_);
        workerThreadIds_.assign(workerCount, std::thread::id{});
        workerRunStart_.assign(workerCount, std::chrono::steady_clock::time_point{});
        workerRunEnd_.assign(workerCount, std::chrono::steady_clock::time_point{});
        workerExited_.assign(workerCount, false);
        stopRequestedSet_ = false;
    }

    try {
        for (std::size_t i = 0; i < workerCount; ++i) {
            workers_.emplace_back([this, i]() {
                {
                    std::lock_guard<std::mutex> stateLock(workerStateMutex_);
                    if (i < workerThreadIds_.size()) {
                        workerThreadIds_[i] = std::this_thread::get_id();
                    }
                    if (i < workerRunStart_.size()) {
                        workerRunStart_[i] = std::chrono::steady_clock::now();
                    }
                    if (i < workerExited_.size()) {
                        workerExited_[i] = false;
                    }
                }
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
                {
                    std::lock_guard<std::mutex> stateLock(workerStateMutex_);
                    if (i < workerRunEnd_.size()) {
                        workerRunEnd_[i] = std::chrono::steady_clock::now();
                    }
                    if (i < workerExited_.size()) {
                        workerExited_[i] = true;
                    }
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
    {
        std::lock_guard<std::mutex> stateLock(workerStateMutex_);
        stopRequestedAt_ = std::chrono::steady_clock::now();
        stopRequestedSet_ = true;
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

    auto joinStart = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(joinMutex_);
    const auto joinDeadline = joinStart + timeout;
    bool completed = false;
    while (true) {
        if (activeWorkers_.load(std::memory_order_acquire) == 0) {
            completed = true;
            break;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= joinDeadline) {
            break;
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(joinDeadline - now);
        const auto slice =
            std::min<std::chrono::milliseconds>(remaining, std::chrono::milliseconds(50));
        joinCV_.wait_for(lock, slice);
    }

    auto joinEnd = std::chrono::steady_clock::now();
    auto joinDurationMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(joinEnd - joinStart).count();

    struct WorkerSnapshot {
        std::size_t index = 0;
        std::size_t tidHash = 0;
        bool exited = false;
        long long runMs = -1;
        long long exitAfterStopMs = -1;
    };

    std::vector<WorkerSnapshot> workersStillRunning;
    WorkerSnapshot slowestExited{};
    bool haveSlowestExited = false;

    {
        std::lock_guard<std::mutex> stateLock(workerStateMutex_);
        for (std::size_t i = 0; i < workers_.size(); ++i) {
            WorkerSnapshot snap;
            snap.index = i;
            if (i < workerThreadIds_.size() && workerThreadIds_[i] != std::thread::id{}) {
                snap.tidHash = std::hash<std::thread::id>{}(workerThreadIds_[i]);
            }
            const auto startTs = i < workerRunStart_.size()
                                     ? workerRunStart_[i]
                                     : std::chrono::steady_clock::time_point{};
            const auto endTs = i < workerRunEnd_.size() ? workerRunEnd_[i]
                                                        : std::chrono::steady_clock::time_point{};
            snap.exited = (i < workerExited_.size()) ? workerExited_[i] : false;

            if (startTs != std::chrono::steady_clock::time_point{}) {
                const auto runUntil =
                    snap.exited && endTs != std::chrono::steady_clock::time_point{} ? endTs
                                                                                    : joinEnd;
                snap.runMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(runUntil - startTs)
                        .count();
            }

            if (stopRequestedSet_ && snap.exited &&
                endTs != std::chrono::steady_clock::time_point{}) {
                snap.exitAfterStopMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(endTs - stopRequestedAt_)
                        .count();
                if (!haveSlowestExited || snap.exitAfterStopMs > slowestExited.exitAfterStopMs) {
                    slowestExited = snap;
                    haveSlowestExited = true;
                }
            }

            if (!snap.exited) {
                workersStillRunning.push_back(snap);
            }
        }
    }

    if (!completed || joinDurationMs >= 500) {
        if (haveSlowestExited) {
            spdlog::warn("[WorkCoordinator] joinWithTimeout diagnostics: elapsed={}ms completed={} "
                         "slowest_worker={} tid_hash={} exit_after_stop={}ms run={}ms",
                         joinDurationMs, completed, slowestExited.index, slowestExited.tidHash,
                         slowestExited.exitAfterStopMs, slowestExited.runMs);
        } else {
            spdlog::warn("[WorkCoordinator] joinWithTimeout diagnostics: elapsed={}ms completed={} "
                         "no worker exit samples",
                         joinDurationMs, completed);
        }
    }

    if (!completed) {
        try {
            spdlog::error(
                "[WorkCoordinator] CRITICAL: Timeout expired with {} workers still active. "
                "Attempting to join remaining threads...",
                activeWorkers_.load());
            for (const auto& snap : workersStillRunning) {
                spdlog::error("[WorkCoordinator] timeout worker detail: worker={} tid_hash={} "
                              "exited={} run={}ms",
                              snap.index, snap.tidHash, snap.exited, snap.runMs);
            }
        } catch (...) {
        }
        lock.unlock();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                try {
                    spdlog::error(
                        "[WorkCoordinator] Detaching worker as last resort after join timeout");
                    worker.detach();
                } catch (...) {
                    spdlog::error("[WorkCoordinator] Exception while joining worker thread");
                    worker.detach();
                }
            }
        }
    } else {
        lock.unlock();
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

boost::asio::any_io_executor WorkCoordinator::getPriorityExecutor(Priority priority) const {
    switch (priority) {
        case Priority::High:
            return highPriorityStrand_;
        case Priority::Background:
            return backgroundPriorityStrand_;
        case Priority::Normal:
        default:
            return normalPriorityStrand_;
    }
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

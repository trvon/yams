// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/IOCoordinator.h>

#include <spdlog/spdlog.h>

#include <boost/asio/detail/concurrency_hint.hpp>

#include <stdexcept>

namespace yams::daemon {

IOCoordinator::IOCoordinator() : IOCoordinator(Config{}) {}

IOCoordinator::IOCoordinator(Config config)
    : config_(std::move(config)),
      io_context_(std::make_shared<boost::asio::io_context>(BOOST_ASIO_CONCURRENCY_HINT_SAFE)) {
    // Ensure at least 1 thread
    if (config_.num_threads == 0) {
        config_.num_threads = 2;
    }
    spdlog::debug("[IOCoordinator] Created with {} threads configured", config_.num_threads);
}

IOCoordinator::~IOCoordinator() {
    if (running_.load(std::memory_order_acquire)) {
        spdlog::debug("[IOCoordinator] Destructor called with active threads, stopping...");
        stop();
        join();
    }
    spdlog::debug("[IOCoordinator] Destroyed");
}

void IOCoordinator::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        throw std::runtime_error("IOCoordinator already started");
    }

    spdlog::debug("[IOCoordinator] Starting {} I/O threads...", config_.num_threads);

    // Create work guard to keep io_context alive
    work_guard_.emplace(boost::asio::make_work_guard(*io_context_));

    // Spawn I/O threads
    threads_.reserve(config_.num_threads);
    try {
        for (size_t i = 0; i < config_.num_threads; ++i) {
            threads_.emplace_back([this, i]() {
                spdlog::trace("[IOCoordinator] I/O thread {} starting", i);
                io_context_->run();
                spdlog::trace("[IOCoordinator] I/O thread {} exited", i);
            });
        }
        spdlog::info("[IOCoordinator] Started with {} I/O threads", config_.num_threads);
    } catch (const std::exception& e) {
        spdlog::error("[IOCoordinator] Failed to spawn I/O thread: {}", e.what());

        // Cleanup on failure
        io_context_->stop();
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                try {
                    thread.join();
                } catch (...) {
                }
            }
        }
        threads_.clear();
        work_guard_.reset();
        running_.store(false, std::memory_order_release);
        throw std::runtime_error(std::string("Failed to start IOCoordinator: ") + e.what());
    }
}

void IOCoordinator::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        spdlog::debug("[IOCoordinator] stop() called but not running");
        return;
    }

    spdlog::debug("[IOCoordinator] Stopping...");
    work_guard_.reset();
    io_context_->stop();
    spdlog::info("[IOCoordinator] Stopped");
}

void IOCoordinator::join() {
    if (threads_.empty()) {
        spdlog::debug("[IOCoordinator] join() called with no threads");
        return;
    }

    spdlog::debug("[IOCoordinator] Joining {} I/O threads...", threads_.size());
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            try {
                thread.join();
            } catch (const std::exception& e) {
                spdlog::warn("[IOCoordinator] Exception during thread join: {}", e.what());
            }
        }
    }
    threads_.clear();
    spdlog::info("[IOCoordinator] All I/O threads joined");
}

std::shared_ptr<boost::asio::io_context> IOCoordinator::getIOContext() const noexcept {
    return io_context_;
}

boost::asio::io_context::executor_type IOCoordinator::getExecutor() const noexcept {
    return io_context_->get_executor();
}

boost::asio::strand<boost::asio::io_context::executor_type> IOCoordinator::makeStrand() const {
    return boost::asio::make_strand(*io_context_);
}

bool IOCoordinator::isRunning() const noexcept {
    return running_.load(std::memory_order_acquire);
}

size_t IOCoordinator::getThreadCount() const noexcept {
    return threads_.size();
}

const IOCoordinator::Config& IOCoordinator::config() const noexcept {
    return config_;
}

} // namespace yams::daemon

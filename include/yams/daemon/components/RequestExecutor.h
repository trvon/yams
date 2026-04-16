// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2024-2026 YAMS Project Contributors

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace yams::daemon {

class RequestExecutor {
public:
    RequestExecutor();
    ~RequestExecutor();

    RequestExecutor(const RequestExecutor&) = delete;
    RequestExecutor& operator=(const RequestExecutor&) = delete;
    RequestExecutor(RequestExecutor&&) = delete;
    RequestExecutor& operator=(RequestExecutor&&) = delete;

    void start(std::optional<std::size_t> numThreads = std::nullopt);
    void stop();
    void join();

    [[nodiscard]] boost::asio::any_io_executor getExecutor() const noexcept;
    [[nodiscard]] std::shared_ptr<boost::asio::io_context> getIOContext() const noexcept;
    [[nodiscard]] bool isRunning() const noexcept;
    [[nodiscard]] std::size_t getWorkerCount() const noexcept;

    static std::size_t defaultThreadCount();

private:
    std::shared_ptr<boost::asio::io_context> ioContext_;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        workGuard_;
    std::vector<std::thread> workers_;
    bool started_ = false;
    std::atomic<std::size_t> activeWorkers_{0};
};

} // namespace yams::daemon

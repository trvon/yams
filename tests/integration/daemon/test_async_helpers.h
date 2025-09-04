#pragma once

#include <chrono>
#include <yams/cli/async_bridge.h>

namespace yams::test_async {

using namespace std::chrono_literals;

template <typename T>
inline Result<T> res(Task<Result<T>> task, std::chrono::milliseconds timeout = 5s) {
    return yams::cli::run_sync(std::move(task), timeout);
}

template <typename T> inline bool ok(Task<Result<T>> task, std::chrono::milliseconds timeout = 5s) {
    return static_cast<bool>(yams::cli::run_sync(std::move(task), timeout));
}

template <typename T> inline T val(Task<Result<T>> task, std::chrono::milliseconds timeout = 5s) {
    auto r = yams::cli::run_sync(std::move(task), timeout);
    if (!r) {
        throw std::runtime_error("Async task failed");
    }
    return std::move(r.value());
}

} // namespace yams::test_async

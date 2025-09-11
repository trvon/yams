#pragma once

#include <chrono>
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/core/types.h>

namespace yams::test_async {

using namespace std::chrono_literals;

namespace detail {
template <typename T>
inline yams::Result<T> run_with_timeout(boost::asio::awaitable<yams::Result<T>> aw,
                                        std::chrono::milliseconds timeout) {
    std::promise<yams::Result<T>> prom;
    auto fut = prom.get_future();
    boost::asio::co_spawn(
        boost::asio::system_executor{},
        [a = std::move(aw), &prom]() mutable -> boost::asio::awaitable<void> {
            auto r = co_await std::move(a);
            prom.set_value(std::move(r));
            co_return;
        },
        boost::asio::detached);
    if (fut.wait_for(timeout) != std::future_status::ready) {
        return yams::Error{yams::ErrorCode::Timeout, "test await timeout"};
    }
    return fut.get();
}
} // namespace detail

// Test convenience wrappers
template <typename T>
inline yams::Result<T> res(boost::asio::awaitable<yams::Result<T>> aw,
                           std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return detail::run_with_timeout<T>(std::move(aw), timeout);
}

template <typename T>
inline bool ok(boost::asio::awaitable<yams::Result<T>> aw,
               std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return static_cast<bool>(detail::run_with_timeout<T>(std::move(aw), timeout));
}

template <typename T>
inline T val(boost::asio::awaitable<yams::Result<T>> aw,
             std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    auto r = detail::run_with_timeout<T>(std::move(aw), timeout);
    if (!r)
        throw std::runtime_error("Async task failed");
    return std::move(r.value());
}

} // namespace yams::test_async

// Backward-compat: provide yams::cli::run_sync used by tests (global yams::cli)
namespace yams::cli {
template <typename T>
inline yams::Result<T> run_sync(boost::asio::awaitable<yams::Result<T>> aw,
                                std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    return yams::test_async::detail::run_with_timeout<T>(std::move(aw), timeout);
}
} // namespace yams::cli

#pragma once

#include <chrono>
#include <future>
#include <type_traits>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/daemon/client/global_io_context.h>

#include <yams/core/types.h>

namespace yams::cli {

// Run a Boost.Asio awaitable<Result<T>> synchronously with a timeout.
// Returns Result<T> or a Timeout/InternalError on failure.
template <typename T, typename Rep, typename Period>
inline yams::Result<T> run_sync(boost::asio::awaitable<yams::Result<T>> aw,
                                const std::chrono::duration<Rep, Period>& timeout) {
    try {
        std::promise<yams::Result<T>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [aw = std::move(aw), p = std::move(prom)]() mutable -> boost::asio::awaitable<void> {
                try {
                    auto r = co_await std::move(aw);
                    p.set_value(std::move(r));
                } catch (const std::exception& e) {
                    p.set_value(yams::Error{yams::ErrorCode::InternalError, e.what()});
                } catch (...) {
                    p.set_value(yams::Error{yams::ErrorCode::InternalError, "unknown error"});
                }
                co_return;
            },
            boost::asio::detached);
        if (fut.wait_for(timeout) != std::future_status::ready) {
            return yams::Error{yams::ErrorCode::Timeout, "timeout"};
        }
        return fut.get();
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    } catch (...) {
        return yams::Error{yams::ErrorCode::InternalError, "unknown error"};
    }
}

} // namespace yams::cli

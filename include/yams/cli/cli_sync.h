#pragma once

#include <chrono>
#include <future>
#include <type_traits>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/daemon/client/global_io_context.h>

#include <yams/core/types.h>

namespace yams::cli {

// Run a Boost.Asio awaitable<Result<T>> synchronously with a timeout.
// Uses cancellation_signal for proper cleanup on timeout per Boost.Asio best practices.
template <typename T, typename Rep, typename Period>
inline yams::Result<T> run_sync(boost::asio::awaitable<yams::Result<T>> aw,
                                const std::chrono::duration<Rep, Period>& timeout) {
    try {
        boost::asio::cancellation_signal cancel_signal;

        auto result_future = boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [aw = std::move(aw)]() mutable -> boost::asio::awaitable<yams::Result<T>> {
                try {
                    co_return co_await std::move(aw);
                } catch (const boost::system::system_error& e) {
                    if (e.code() == boost::asio::error::operation_aborted) {
                        co_return yams::Error{yams::ErrorCode::OperationCancelled, "cancelled"};
                    }
                    co_return yams::Error{yams::ErrorCode::InternalError, e.what()};
                } catch (const std::exception& e) {
                    co_return yams::Error{yams::ErrorCode::InternalError, e.what()};
                } catch (...) {
                    co_return yams::Error{yams::ErrorCode::InternalError, "unknown error"};
                }
            },
            boost::asio::bind_cancellation_slot(cancel_signal.slot(), boost::asio::use_future));

        if (result_future.wait_for(timeout) == std::future_status::ready) {
            try {
                return result_future.get();
            } catch (const std::exception& e) {
                return yams::Error{yams::ErrorCode::InternalError, e.what()};
            }
        }

        // Timeout: emit cancellation and wait for coroutine to complete
        cancel_signal.emit(boost::asio::cancellation_type::terminal);

        // Wait for coroutine to acknowledge cancellation and complete
        constexpr auto cancel_grace = std::chrono::milliseconds(1000);
        if (result_future.wait_for(cancel_grace) == std::future_status::ready) {
            try {
                result_future.get(); // Consume to clean up
            } catch (...) {
            }
        }

        return yams::Error{yams::ErrorCode::Timeout, "timeout"};
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    } catch (...) {
        return yams::Error{yams::ErrorCode::InternalError, "unknown error"};
    }
}

} // namespace yams::cli

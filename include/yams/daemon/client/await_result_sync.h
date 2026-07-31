#pragma once

#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <yams/core/types.h>

namespace yams::daemon::detail {

// Runs an awaitable factory on the supplied executor and synchronously returns its result.
// A zero or negative timeout preserves the established unbounded-wait behavior. The factory
// must own every object referenced by the awaitable because the coroutine may finish after a
// timed-out caller has returned.
template <typename T, typename AwaitableFactory>
Result<T> awaitResultSync(boost::asio::any_io_executor executor, AwaitableFactory&& makeAwaitable,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                          std::string_view operation = "Awaitable") {
    using Factory = std::decay_t<AwaitableFactory>;

    auto promise = std::make_shared<std::promise<Result<T>>>();
    auto future = promise->get_future();
    auto operationName = std::string(operation);

    try {
        boost::asio::co_spawn(
            std::move(executor),
            [promise, makeAwaitable = Factory(std::forward<AwaitableFactory>(makeAwaitable)),
             operationName]() mutable -> boost::asio::awaitable<void> {
                try {
                    promise->set_value(co_await makeAwaitable());
                } catch (const std::exception& exception) {
                    promise->set_value(
                        Error{ErrorCode::InternalError,
                              operationName + " failed with exception: " + exception.what()});
                } catch (...) {
                    promise->set_value(Error{ErrorCode::InternalError,
                                             operationName + " failed with unknown exception"});
                }
                co_return;
            },
            boost::asio::detached);
    } catch (const std::exception& exception) {
        return Error{ErrorCode::InternalError,
                     operationName + " failed to start: " + exception.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError, operationName + " failed to start"};
    }

    if (timeout.count() > 0) {
        if (future.wait_for(timeout) != std::future_status::ready) {
            return Error{ErrorCode::Timeout, operationName + " timed out"};
        }
    } else {
        future.wait();
    }

    try {
        return future.get();
    } catch (const std::exception& exception) {
        return Error{ErrorCode::InternalError,
                     operationName + " failed with exception: " + exception.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError, operationName + " failed with unknown exception"};
    }
}

template <typename T>
Result<T> awaitResultSync(boost::asio::any_io_executor executor,
                          boost::asio::awaitable<Result<T>> awaitable,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                          std::string_view operation = "Awaitable") {
    return awaitResultSync<T>(
        std::move(executor),
        [awaitable = std::move(awaitable)]() mutable { return std::move(awaitable); }, timeout,
        operation);
}

} // namespace yams::daemon::detail
